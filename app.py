import os
import re
import time
from io import BytesIO
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

from whoosh import index
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NUMERIC
from whoosh.qparser import MultifieldParser, OrGroup
from whoosh.query import And, Term, Every

# =================================================
# CONFIG
# =================================================
st.set_page_config(page_title="Coletor + Busca (Whoosh) - ALMG", layout="wide")

APP_TITLE = "📚 Coletor e Motor de Busca (Booleano) — Normas ALMG (Streamlit Cloud)"
DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")

DEFAULT_TIMEOUT = 20
MAX_COLETA_UI = 50  # limite de segurança na UI (evita travar Streamlit Cloud)

# =================================================
# UI HEADER
# =================================================
st.title(APP_TITLE)
st.caption(
    "Upload → Coleta HTML → Indexação local (Whoosh) → Busca booleana + filtros por campos. "
    "Ideal para rodar 100% no Streamlit Cloud."
)

with st.expander("⚠️ Sobre persistência no Streamlit Cloud", expanded=False):
    st.markdown(
        """
- O Streamlit Community Cloud pode reiniciar o app e o filesystem pode ser limpo.
- Para uso grande (120k docs), o ideal é **pré-gerar o índice** e hospedá-lo como arquivo (zip) no GitHub Releases/Storage,
  e o app **baixar e abrir**.  
- Este app já funciona para protótipo/uso por sessão e ingestões menores pela UI.
"""
    )

# =================================================
# HELPERS
# =================================================
def mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def limpar_texto(texto: str) -> str:
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()

# =================================================
# HTML EXTRACTION (baseado no seu)
# =================================================
def extrair_texto_html(url: str) -> Tuple[str, str]:
    """
    Retorna (texto, html). Se falhar, texto começa com '❌' e html vem ''.
    """
    try:
        resp = requests.get(
            url,
            timeout=DEFAULT_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # Tentativa 1: span tradicional usado por LEI/DEC
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = limpar_texto(span.get_text(separator="\n", strip=True))
            if len(texto) > 50:
                return texto, html

        # Tentativa 2: extrair do <main> (usado por DCS, DCE, etc.)
        main = soup.find("main")
        if main:
            for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
                tag.decompose()

            for div in main.find_all("div"):
                if "compartilhar" in div.get_text(strip=True).lower():
                    div.decompose()

            texto = limpar_texto(main.get_text(separator="\n", strip=True))

            for marcador in ["DELIBERA", "RESOLVE", "Art. 1º", "Art. 1o", "Art. 1"]:
                if marcador in texto:
                    return limpar_texto(marcador + "\n" + texto.split(marcador, 1)[-1]), html

            if len(texto) > 100:
                return texto, html

        return "❌ Texto não encontrado", ""
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}", ""

def gerar_links(tipo: str, numero: str | int, ano: str | int) -> Dict[str, str]:
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {"Original": base + "/", "Consolidado": base + "/?cons=1"}

def make_doc_id(tipo: str, numero: int, ano: int, versao: str) -> str:
    versao_slug = "orig" if versao.lower().startswith("orig") else "cons"
    return f"{tipo.upper()}_{numero}_{ano}_{versao_slug}"

# =================================================
# WHOOSH INDEX
# =================================================
def get_schema() -> Schema:
    # KEYWORD = exato (filtro). TEXT = full-text.
    return Schema(
        doc_id=ID(stored=True, unique=True),
        tipo_sigla=KEYWORD(stored=True, commas=False, lowercase=False),
        numero=NUMERIC(stored=True, numtype=int),
        ano=NUMERIC(stored=True, numtype=int),
        versao=KEYWORD(stored=True, commas=False, lowercase=False),
        url=ID(stored=True),
        texto=TEXT(stored=False),  # full-text
        coletado_em=ID(stored=True),
    )

@st.cache_resource(show_spinner=False)
def open_or_create_index(index_dir: str):
    mkdirp(index_dir)
    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    return index.create_in(index_dir, schema=get_schema())

def add_documents(ix, docs: List[Dict]) -> Tuple[int, int]:
    """
    docs: lista de dicts com chaves do schema.
    Retorna (ok, fail).
    """
    ok = 0
    fail = 0
    writer = ix.writer(limitmb=256, procs=1, multisegment=True)
    try:
        for d in docs:
            try:
                writer.update_document(**d)  # upsert por doc_id unique
                ok += 1
            except Exception:
                fail += 1
        writer.commit()
    except Exception:
        writer.cancel()
        raise
    return ok, fail

def build_query(ix, expr: str, filtros: Dict) :
    # Parser booleana em campos de texto (pode expandir para titulo etc.)
    parser = MultifieldParser(["texto"], schema=ix.schema, group=OrGroup)
    q_text = (expr or "").strip()

    base_q = parser.parse(q_text) if q_text else Every()

    filter_terms = []
    if filtros.get("tipo_sigla"):
        filter_terms.append(Term("tipo_sigla", str(filtros["tipo_sigla"]).upper()))
    if filtros.get("versao"):
        filter_terms.append(Term("versao", str(filtros["versao"])))
    if filtros.get("ano") not in (None, "", "Todos"):
        filter_terms.append(Term("ano", int(filtros["ano"])))
    if filtros.get("numero") not in (None, "", "Todos"):
        filter_terms.append(Term("numero", int(filtros["numero"])))

    if filter_terms:
        return And([base_q] + filter_terms)
    return base_q

def search(ix, expr: str, filtros: Dict, limit: int = 20):
    q = build_query(ix, expr, filtros)
    with ix.searcher() as s:
        results = s.search(q, limit=limit)
        out = []
        for r in results:
            out.append({
                "doc_id": r.get("doc_id"),
                "tipo_sigla": r.get("tipo_sigla"),
                "numero": r.get("numero"),
                "ano": r.get("ano"),
                "versao": r.get("versao"),
                "url": r.get("url"),
                "coletado_em": r.get("coletado_em"),
                "score": float(r.score),
            })
        return out, results.scored_length()

# =================================================
# SIDEBAR
# =================================================
with st.sidebar:
    st.header("🧠 Sintaxe booleana")
    st.markdown(
        """
Exemplos:
- `("transparência" OR publicidade) AND contrato`
- `(licitação OR dispensa) AND NOT revogado`
- `"Art. 1" AND (prazo OR vigência)`

Campos (filtros):
- use os inputs de tipo/ano/número/versão.
"""
    )
    st.header("⚙️ Índice local")
    st.caption(f"Pasta do índice: `{INDEX_DIR}`")

# =================================================
# INIT INDEX
# =================================================
mkdirp(DATA_DIR)
ix = open_or_create_index(INDEX_DIR)

# =================================================
# SECTION 1: UPLOAD + COLETA + INDEXAÇÃO
# =================================================
st.subheader("1) 📥 Upload → Coletar HTML → Indexar (Whoosh)")

arquivo = st.file_uploader("Envie CSV ou Excel com colunas: tipo_sigla, numero, ano", type=["csv", "xlsx"])

if arquivo:
    try:
        df = pd.read_csv(arquivo) if arquivo.name.endswith(".csv") else pd.read_excel(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        st.stop()

    colunas_necessarias = {"tipo_sigla", "numero", "ano"}
    if not colunas_necessarias.issubset(df.columns):
        st.error("⚠️ O arquivo deve conter as colunas: tipo_sigla, numero, ano")
        st.stop()

    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates().copy()
    df["tipo_sigla"] = df["tipo_sigla"].astype(str).str.strip()
    df["numero"] = pd.to_numeric(df["numero"], errors="coerce")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
    df = df.dropna(subset=["numero", "ano"])
    df["numero"] = df["numero"].astype(int)
    df["ano"] = df["ano"].astype(int)

    anos_disponiveis = sorted(df["ano"].unique(), reverse=True)
    anos_selecionados = st.multiselect("Selecione ano(s) para coletar", anos_disponiveis)

    if anos_selecionados:
        df_filtrado = df[df["ano"].isin(anos_selecionados)].copy()
        st.info(f"Normas selecionadas: {len(df_filtrado)}")

        if len(df_filtrado) > MAX_COLETA_UI:
            st.warning(
                f"⚠️ Limite no Streamlit Cloud: selecione até {MAX_COLETA_UI} normas por vez."
                " (Para carga grande, pré-gere o índice fora do Cloud e publique o índice pronto.)"
            )
            st.stop()

        colA, colB = st.columns(2)
        coletar_original = colA.checkbox("Coletar versão Original", value=True)
        coletar_consolidado = colB.checkbox("Coletar versão Consolidado", value=True)

        if st.button("🚀 Coletar e indexar"):
            st.info("🔄 Coletando e indexando…")
            resultados = []
            docs_index = []

            barra = st.progress(0)
            total = len(df_filtrado)

            for j, (_, row) in enumerate(df_filtrado.iterrows(), start=1):
                tipo = row["tipo_sigla"]
                numero = int(row["numero"])
                ano = int(row["ano"])
                links = gerar_links(tipo, numero, ano)

                versoes = []
                if coletar_original:
                    versoes.append(("Original", links["Original"]))
                if coletar_consolidado:
                    versoes.append(("Consolidado", links["Consolidado"]))

                for versao, url in versoes:
                    texto, _html = extrair_texto_html(url)

                    resultados.append({
                        "tipo_sigla": tipo,
                        "numero": numero,
                        "ano": ano,
                        "versao": versao,
                        "url": url,
                        "texto": texto,
                    })

                    # indexa somente se texto válido
                    if texto and not texto.startswith("❌") and len(texto) > 50:
                        docs_index.append({
                            "doc_id": make_doc_id(tipo, numero, ano, versao),
                            "tipo_sigla": tipo.upper(),
                            "numero": numero,
                            "ano": ano,
                            "versao": versao,
                            "url": url,
                            "texto": texto,
                            "coletado_em": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        })

                barra.progress(j / total)

            df_resultado = pd.DataFrame(resultados)
            st.success("✅ Coleta finalizada!")
            st.dataframe(df_resultado.head(50), use_container_width=True)

            with st.spinner("Indexando (Whoosh)…"):
                ok, fail = add_documents(ix, docs_index)

            st.success(f"📌 Indexação concluída: {ok} docs OK | {fail} falhas")

            # Download do CSV
            buffer = BytesIO()
            df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")
            st.download_button(
                "⬇️ Baixar CSV com os textos coletados",
                data=buffer.getvalue(),
                file_name="textos_normas_almg.csv",
                mime="text/csv",
            )

st.divider()

# =================================================
# SECTION 2: BUSCA
# =================================================
st.subheader("2) 🔎 Buscar no índice (Booleano + campos)")

col1, col2 = st.columns([2, 1])
expr = col1.text_input(
    "Busca booleana (AND/OR/NOT, parênteses, aspas)",
    value='("transparência" OR publicidade) AND contrato',
)
limit = col2.number_input("Qtde de resultados", min_value=5, max_value=100, value=20, step=5)

f1, f2, f3, f4 = st.columns(4)
f_tipo = f1.text_input("tipo_sigla (ex.: LEI, DEC)", value="")
f_ano = f2.text_input("ano (opcional)", value="")
f_num = f3.text_input("numero (opcional)", value="")
f_versao = f4.selectbox("versao", ["", "Original", "Consolidado"], index=0)

if st.button("Buscar"):
    filtros = {
        "tipo_sigla": f_tipo.strip(),
        "ano": f_ano.strip(),
        "numero": f_num.strip(),
        "versao": f_versao.strip(),
    }

    try:
        hits, total = search(ix, expr, filtros, limit=int(limit))
    except Exception as e:
        st.error(f"Erro na busca (sintaxe/consulta): {e}")
        st.stop()

    st.success(f"Resultados retornados: {len(hits)}")

    for h in hits:
        meta = f"{h.get('tipo_sigla')} {h.get('numero')}/{h.get('ano')} — {h.get('versao')}"
        st.markdown(f"### {meta}")
        st.caption(f"doc_id: {h.get('doc_id')} | score: {h.get('score'):.3f} | coletado_em: {h.get('coletado_em')}")
        if h.get("url"):
            st.markdown(h["url"])
        st.divider()
