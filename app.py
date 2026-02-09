import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from pathlib import Path
import hashlib
import shutil

# Whoosh (ou Whoosh-Reloaded)
from whoosh import index
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import MultifieldParser, AndGroup, OrGroup
from whoosh.analysis import RegexTokenizer, LowercaseFilter, StopFilter, StemFilter, CharsetFilter
from whoosh.support.charset import default_charset

# -------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -------------------------------------------------
st.set_page_config(page_title="Coletor + Busca (Whoosh) | ALMG", layout="wide")
st.title("📄 Coletor de Textos de Normas da ALMG + 🔎 Busca Booleana (Whoosh)")

# -------------------------------------------------
# FUNÇÕES
# -------------------------------------------------
@st.cache_data(show_spinner=False, ttl=24 * 3600)
def extrair_texto_html(url: str) -> str:
    """Extrai texto da página da ALMG (cacheado por URL)."""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Tentativa 1: span tradicional
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Tentativa 2: <main>
        main = soup.find("main")
        if main:
            for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
                tag.decompose()

            for div in main.find_all("div"):
                if "compartilhar" in div.get_text(strip=True).lower():
                    div.decompose()

            texto = main.get_text(separator="\n", strip=True)

            for marcador in ["DELIBERA", "RESOLVE", "Art. 1º", "Art. 1o", "Art. 1"]:
                if marcador in texto:
                    return marcador + "\n" + texto.split(marcador, 1)[-1].strip()

            if len(texto) > 100:
                return texto.strip()

        return "❌ Texto não encontrado"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"


def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {"Original": base + "/", "Consolidado": base + "/?cons=1"}


def parse_linhas_normas(texto: str) -> pd.DataFrame:
    """
    Aceita linhas tipo:
      LEI, 123, 2020
      DEC;456;2019
      DCS\t10\t2024
    """
    linhas = [l.strip() for l in (texto or "").splitlines() if l.strip()]
    rows = []
    for l in linhas:
        for sep in [",", ";", "\t"]:
            if sep in l:
                parts = [p.strip() for p in l.split(sep)]
                break
        else:
            # tenta split por espaço
            parts = [p.strip() for p in l.split()]

        if len(parts) >= 3:
            rows.append({"tipo_sigla": parts[0], "numero": parts[1], "ano": parts[2]})

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)
    df["numero"] = df["numero"].astype(str)
    df["tipo_sigla"] = df["tipo_sigla"].astype(str).str.strip()
    return df


def make_pt_analyzer():
    """
    Analyzer voltado a PT-BR:
    - lowercase
    - folding de acentos (ação -> acao)
    - stopwords PT quando disponível
    - stemming PT quando disponível
    """
    ana = RegexTokenizer() | LowercaseFilter() | CharsetFilter(default_charset)

    # Stopwords PT (se disponível)
    try:
        ana = ana | StopFilter(lang="pt")
    except Exception:
        ana = ana | StopFilter()

    # Stemming PT (se disponível)
    try:
        from whoosh.lang import has_stemmer
        if has_stemmer("pt"):
            ana = ana | StemFilter(lang="pt")
    except Exception:
        pass

    return ana


SCHEMA = Schema(
    doc_id=ID(stored=True, unique=True),
    tipo_sigla=ID(stored=True),
    numero=ID(stored=True),
    ano=ID(stored=True),
    versao=ID(stored=True),
    url=ID(stored=True),
    texto=TEXT(stored=True, analyzer=make_pt_analyzer()),
)


def fingerprint_df(df: pd.DataFrame) -> str:
    m = hashlib.md5()
    # hash leve: URLs + tamanho do texto
    for u, t in zip(df["url"].astype(str), df["texto"].astype(str)):
        m.update(u.encode("utf-8"))
        m.update(str(len(t)).encode("utf-8"))
    return m.hexdigest()


def build_whoosh_index(df_textos: pd.DataFrame) -> index.Index:
    """
    Cria (ou recria) índice em disco numa pasta local.
    Armazenamos no session_state o fingerprint para não reindexar à toa.
    """
    fp = fingerprint_df(df_textos)

    if st.session_state.get("whoosh_fp") == fp and st.session_state.get("whoosh_ready"):
        return st.session_state["whoosh_ix"]

    idx_dir = Path(".whoosh_index")
    if idx_dir.exists():
        shutil.rmtree(idx_dir)
    idx_dir.mkdir(parents=True, exist_ok=True)

    ix = index.create_in(idx_dir, SCHEMA)
    writer = ix.writer(limitmb=256)

    for _, r in df_textos.iterrows():
        doc_id = f'{r["tipo_sigla"]}-{r["numero"]}-{r["ano"]}-{r["versao"]}'
        writer.add_document(
            doc_id=doc_id,
            tipo_sigla=str(r["tipo_sigla"]),
            numero=str(r["numero"]),
            ano=str(r["ano"]),
            versao=str(r["versao"]),
            url=str(r["url"]),
            texto=str(r["texto"]),
        )

    writer.commit()
    st.session_state["whoosh_ix"] = ix
    st.session_state["whoosh_fp"] = fp
    st.session_state["whoosh_ready"] = True
    return ix


# -------------------------------------------------
# UI
# -------------------------------------------------
tab_coleta, tab_busca = st.tabs(["📥 Coleta", "🔎 Busca (Whoosh)"])

with tab_coleta:
    st.subheader("1) Monte sua lista de normas (sem upload)")

    colA, colB = st.columns([2, 1], gap="large")

    with colA:
        st.caption("Opção A: edite/cole como planilha")
        if "df_normas" not in st.session_state:
            st.session_state["df_normas"] = pd.DataFrame(
                [{"tipo_sigla": "LEI", "numero": "1", "ano": "1989"}]
            )

        df_edit = st.data_editor(
            st.session_state["df_normas"],
            num_rows="dynamic",
            use_container_width=True,
            key="editor_normas",
        )
        st.session_state["df_normas"] = df_edit

    with colB:
        st.caption("Opção B: cole linhas (tipo_sigla,numero,ano)")
        exemplos = "LEI, 100, 2020\nDEC; 1234; 2019\nDCS\t10\t2024"
        texto_colado = st.text_area("Cole aqui", value="", height=140, placeholder=exemplos)
        if st.button("Importar linhas coladas"):
            df_import = parse_linhas_normas(texto_colado)
            if df_import.empty:
                st.warning("Não consegui ler nenhuma linha válida.")
            else:
                st.session_state["df_normas"] = (
                    pd.concat([st.session_state["df_normas"], df_import], ignore_index=True)
                    .dropna()
                    .drop_duplicates()
                )
                st.success(f"Importadas {len(df_import)} linhas.")

    df = st.session_state["df_normas"].copy()
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)
    df["numero"] = df["numero"].astype(str)
    df["tipo_sigla"] = df["tipo_sigla"].astype(str)

    st.divider()
    st.subheader("2) Selecione e colete")

    anos_disponiveis = sorted(df["ano"].unique(), reverse=True)
    anos_sel = st.multiselect("📅 Anos", anos_disponiveis, default=anos_disponiveis[:1])

    versoes_sel = st.multiselect("Versões", ["Original", "Consolidado"], default=["Original", "Consolidado"])

    df_filtrado = df[df["ano"].isin(anos_sel)] if anos_sel else df.iloc[0:0]
    st.markdown(f"🔎 Normas selecionadas: **{len(df_filtrado)}**")

    limite = st.number_input("Limite de segurança", min_value=1, max_value=2000, value=50, step=10)
    if len(df_filtrado) > limite:
        st.warning(f"⚠️ Selecione até {limite} normas para evitar travamentos.")
        st.stop()

    if st.button(f"🚀 Coletar textos ({len(df_filtrado)} normas × {len(versoes_sel)} versão(ões))", disabled=len(df_filtrado) == 0):
        st.info("🔄 Coletando…")
        resultados = []
        barra = st.progress(0)
        total = max(1, len(df_filtrado))

        for i, (_, row) in enumerate(df_filtrado.iterrows()):
            tipo, numero, ano = row["tipo_sigla"], row["numero"], row["ano"]
            links = gerar_links(tipo, numero, ano)

            for versao in versoes_sel:
                url = links[versao]
                texto = extrair_texto_html(url)
                resultados.append(
                    {
                        "tipo_sigla": tipo,
                        "numero": numero,
                        "ano": ano,
                        "versao": versao,
                        "url": url,
                        "texto": texto,
                    }
                )

            barra.progress((i + 1) / total)

        df_result = pd.DataFrame(resultados)
        st.session_state["df_textos"] = df_result
        st.success("✅ Coleta finalizada!")
        st.dataframe(df_result, use_container_width=True)

        # Download CSV
        buffer = BytesIO()
        df_result.to_csv(buffer, index=False, encoding="utf-8-sig")
        st.download_button(
            label="⬇️ Baixar CSV com os textos",
            data=buffer.getvalue(),
            file_name="textos_normas_almg.csv",
            mime="text/csv",
        )

with tab_busca:
    st.subheader("Busca booleana (Whoosh)")

    if "df_textos" not in st.session_state or st.session_state["df_textos"].empty:
        st.info("Colete textos na aba **📥 Coleta** para habilitar a busca.")
        st.stop()

    df_textos = st.session_state["df_textos"]

    with st.expander("Sintaxe rápida (exemplos)"):
        st.markdown(
            """
- `(licitação OR contrato) AND (Art. 1 OR "Art. 1º")`
- `meio AND ambiente AND NOT revoga`
- `tipo_sigla:LEI AND ano:2020 AND "Art. 1º"`
- Use aspas para frases: `"proteção ambiental"`
- Use parênteses para precedência: `(A OR B) AND C`
"""
        )

    # Indexa (uma vez por conjunto de textos)
    ix = build_whoosh_index(df_textos)

    col1, col2 = st.columns([2, 1], gap="large")
    with col1:
        q = st.text_input("Consulta", value="", placeholder='Ex: (educação OR escola) AND NOT revoga')
    with col2:
        default_op = st.selectbox("Operador padrão", ["AND", "OR"], index=0)
        max_hits = st.number_input("Máx. resultados", min_value=10, max_value=2000, value=200, step=50)

    if not q.strip():
        st.stop()

    group = AndGroup if default_op == "AND" else OrGroup

    # Busca em múltiplos campos (texto + metadados)
    parser = MultifieldParser(
        ["texto", "tipo_sigla", "numero", "ano", "versao"],
        schema=ix.schema,
        group=group,
    )

    try:
        query = parser.parse(q)
    except Exception as e:
        st.error(f"Erro ao interpretar a consulta: {e}")
        st.stop()

    with ix.searcher() as searcher:
        results = searcher.search(query, limit=int(max_hits))
        st.markdown(f"**{len(results)}** resultado(s)")

        rows = []
        for hit in results:
            # highlight do campo texto (HTML)
            snippet = hit.highlights("texto") or hit["texto"][:350]
            rows.append(
                {
                    "tipo_sigla": hit["tipo_sigla"],
                    "numero": hit["numero"],
                    "ano": hit["ano"],
                    "versao": hit["versao"],
                    "url": hit["url"],
                    "trecho": snippet,
                }
            )

        df_hits = pd.DataFrame(rows)

    # Exibe com HTML no trecho (highlight)
    if not df_hits.empty:
        st.dataframe(df_hits.drop(columns=["trecho"]), use_container_width=True)

        st.markdown("### Trechos (com destaque)")
        for _, r in df_hits.iterrows():
            st.markdown(
                f"""
**{r['tipo_sigla']} {r['numero']}/{r['ano']} ({r['versao']})**  
{r['url']}  
{r['trecho']}
<hr/>
""",
                unsafe_allow_html=True,
            )
    else:
        st.warning("Nenhum resultado.")
