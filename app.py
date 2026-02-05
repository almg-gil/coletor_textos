import os
import re
import time
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from opensearchpy import OpenSearch, helpers

# =================================================
# CONFIG
# =================================================
st.set_page_config(page_title="Coletor + Busca (OpenSearch) - ALMG", layout="wide")

APP_TITLE = "📚 Coletor e Motor de Busca (Booleano) - Normas ALMG"
INDEX = os.getenv("OPENSEARCH_INDEX", "normas_almg")

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "")  # opcional
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "")  # opcional
OPENSEARCH_USE_SSL = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"

DEFAULT_TIMEOUT = 20
MAX_COLETA_UI = 50  # limite de segurança na UI

# =================================================
# UI HEADER
# =================================================
st.title(APP_TITLE)
st.caption(
    "Upload -> Coleta HTML -> Indexação no OpenSearch -> Busca booleana com filtros por campos."
)

with st.expander("⚙️ Configuração (host/índice)", expanded=False):
    st.write("Você pode mudar via variáveis de ambiente também.")
    st.text_input("OpenSearch host", value=OPENSEARCH_HOST, key="cfg_host")
    st.number_input("OpenSearch port", value=OPENSEARCH_PORT, step=1, key="cfg_port")
    st.text_input("Índice", value=INDEX, key="cfg_index")
    st.toggle("Usar SSL", value=OPENSEARCH_USE_SSL, key="cfg_ssl")


# =================================================
# OPENSEARCH
# =================================================
@st.cache_resource(show_spinner=False)
def get_client(host: str, port: int, use_ssl: bool, user: str = "", pwd: str = "") -> OpenSearch:
    auth = (user, pwd) if user and pwd else None
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_compress=True,
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
        http_auth=auth,
        timeout=60,
        max_retries=3,
        retry_on_timeout=True,
    )


def ensure_index(client: OpenSearch, index_name: str) -> None:
    if client.indices.exists(index=index_name):
        return

    body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "tipo_sigla": {"type": "keyword"},
                "numero": {"type": "integer"},
                "ano": {"type": "integer"},
                "versao": {"type": "keyword"},   # Original | Consolidado
                "url": {"type": "keyword"},
                "texto": {"type": "text"},
                "html": {"type": "text", "index": False},  # guardado, não indexado
                "coletado_em": {"type": "date"},
            }
        },
    }

    client.indices.create(index=index_name, body=body)


def bulk_index(client: OpenSearch, index_name: str, actions: list[dict]) -> tuple[int, int]:
    """
    Retorna (sucessos, falhas).
    """
    if not actions:
        return 0, 0

    success, errors = helpers.bulk(
        client,
        actions,
        index=index_name,
        request_timeout=180,
        raise_on_error=False,
        raise_on_exception=False,
        chunk_size=500,
    )
    fail = len(errors) if isinstance(errors, list) else (0 if not errors else 1)
    return int(success), int(fail)


# =================================================
# HTML EXTRACTION
# =================================================
def limpar_texto(texto: str) -> str:
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()


def extrair_texto_html(url: str) -> tuple[str, str]:
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

        # Tentativa 1: span tradicional
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            texto = limpar_texto(texto)
            if len(texto) > 50:
                return texto, html

        # Tentativa 2: <main>
        main = soup.find("main")
        if main:
            for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
                tag.decompose()

            for div in main.find_all("div"):
                if "compartilhar" in div.get_text(strip=True).lower():
                    div.decompose()

            texto = main.get_text(separator="\n", strip=True)
            texto = limpar_texto(texto)

            # Captura a partir de marcadores
            for marcador in ["DELIBERA", "RESOLVE", "Art. 1º", "Art. 1o", "Art. 1"]:
                if marcador in texto:
                    return limpar_texto(marcador + "\n" + texto.split(marcador, 1)[-1]), html

            if len(texto) > 100:
                return texto, html

        return "❌ Texto não encontrado", ""
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}", ""


def gerar_links(tipo: str, numero: str | int, ano: str | int) -> dict[str, str]:
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {"Original": base + "/", "Consolidado": base + "/?cons=1"}


def make_doc_id(tipo: str, numero: int, ano: int, versao: str) -> str:
    versao_slug = "orig" if versao.lower().startswith("orig") else "cons"
    return f"{tipo.upper()}_{numero}_{ano}_{versao_slug}"


def doc_to_action(index_name: str, tipo: str, numero: int, ano: int, versao: str, url: str, texto: str, html: str) -> dict:
    doc_id = make_doc_id(tipo, numero, ano, versao)
    return {
        "_index": index_name,
        "_id": doc_id,
        "_source": {
            "doc_id": doc_id,
            "tipo_sigla": tipo.upper(),
            "numero": int(numero),
            "ano": int(ano),
            "versao": versao,
            "url": url,
            "texto": texto,
            "html": html,
            "coletado_em": time.strftime("%Y-%m-%dT%H:%M:%S"),
        },
    }


# =================================================
# SEARCH
# =================================================
def montar_query(expr_booleana: str, filtros: dict, size: int = 20, from_: int = 0) -> dict:
    filter_clauses = []

    # filtros por campo
    if filtros.get("tipo_sigla"):
        filter_clauses.append({"term": {"tipo_sigla": str(filtros["tipo_sigla"]).upper()}})
    if filtros.get("versao"):
        filter_clauses.append({"term": {"versao": str(filtros["versao"])}})

    if filtros.get("ano") not in (None, "", "Todos"):
        filter_clauses.append({"term": {"ano": int(filtros["ano"])}})

    if filtros.get("numero") not in (None, "", "Todos"):
        filter_clauses.append({"term": {"numero": int(filtros["numero"])}})

    # query booleana
    must_clause = []
    expr_booleana = (expr_booleana or "").strip()
    if expr_booleana:
        must_clause.append(
            {
                "query_string": {
                    "query": expr_booleana,
                    "fields": ["texto"],
                    "default_operator": "AND",
                    "analyze_wildcard": True,
                }
            }
        )
    else:
        must_clause.append({"match_all": {}})

    return {
        "query": {"bool": {"filter": filter_clauses, "must": must_clause}},
        "highlight": {"fields": {"texto": {}}, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
        "size": int(size),
        "from": int(from_),
    }


# =================================================
# SIDEBAR - HELP
# =================================================
with st.sidebar:
    st.header("🧠 Sintaxe booleana")
    st.markdown(
        """
Exemplos (campo livre):
- `("transparência" OR publicidade) AND contrato`
- `(licitação OR dispensa) AND NOT revogado`
- `"Art. 1" AND (prazo OR vigência)`

Filtros:
- use os campos do formulário (tipo/ano/número/versão).
"""
    )
    st.header("🐳 OpenSearch via Docker (rápido)")
    st.code(
        """docker run -d --name opensearch \\
  -p 9200:9200 -p 9600:9600 \\
  -e "discovery.type=single-node" \\
  -e "plugins.security.disabled=true" \\
  opensearchproject/opensearch:2""",
        language="bash",
    )


# =================================================
# SECTION 1: UPLOAD + COLETA + INDEXAÇÃO
# =================================================
st.subheader("1) 📥 Upload → Coletar HTML → Indexar no OpenSearch")

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
                f"⚠️ Limite temporário na interface: selecione até {MAX_COLETA_UI} normas por vez."
                " (Para carga grande, use um script de ingestão em lote.)"
            )
            st.stop()

        colA, colB, colC = st.columns(3)
        coletar_original = colA.checkbox("Coletar versão Original", value=True)
        coletar_consolidado = colB.checkbox("Coletar versão Consolidado", value=True)
        salvar_html = colC.checkbox("Guardar HTML (não indexa)", value=False)

        if st.button("🚀 Coletar e indexar"):
            host = st.session_state["cfg_host"]
            port = int(st.session_state["cfg_port"])
            index_name = st.session_state["cfg_index"]
            use_ssl = bool(st.session_state["cfg_ssl"])

            client = get_client(host, port, use_ssl, OPENSEARCH_USER, OPENSEARCH_PASS)
            try:
                ensure_index(client, index_name)
            except Exception as e:
                st.error(f"Não consegui criar/verificar o índice '{index_name}': {e}")
                st.stop()

            st.info("🔄 Coletando e indexando…")
            resultados = []
            actions = []

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
                    texto, html = extrair_texto_html(url)

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

                    # Só indexa se não for erro e tiver conteúdo mínimo
                    if texto and not texto.startswith("❌") and len(texto) > 50:
                        actions.append(
                            doc_to_action(
                                index_name=index_name,
                                tipo=tipo,
                                numero=numero,
                                ano=ano,
                                versao=versao,
                                url=url,
                                texto=texto,
                                html=(html if salvar_html else ""),
                            )
                        )

                barra.progress(j / total)

            df_resultado = pd.DataFrame(resultados)
            st.success("✅ Coleta finalizada!")
            st.dataframe(df_resultado.head(50), use_container_width=True)

            # Bulk index
            with st.spinner("Indexando no OpenSearch (bulk)…"):
                ok, fail = bulk_index(client, index_name, actions)

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
st.subheader("2) 🔎 Buscar no índice (OpenSearch)")

host = st.session_state["cfg_host"]
port = int(st.session_state["cfg_port"])
index_name = st.session_state["cfg_index"]
use_ssl = bool(st.session_state["cfg_ssl"])

col1, col2 = st.columns([2, 1])
expr = col1.text_input(
    "Busca booleana (AND/OR/NOT, parênteses, aspas)",
    value='("transparência" OR publicidade) AND contrato',
)

with col2:
    size = st.number_input("Qtde de resultados", min_value=5, max_value=100, value=20, step=5)

f1, f2, f3, f4 = st.columns(4)
f_tipo = f1.text_input("tipo_sigla (ex.: LEI, DEC)", value="")
f_ano = f2.text_input("ano (opcional)", value="")
f_num = f3.text_input("numero (opcional)", value="")
f_versao = f4.selectbox("versao", ["", "Original", "Consolidado"], index=0)

# paginação simples
pcol1, pcol2, _ = st.columns([1, 1, 2])
if "page" not in st.session_state:
    st.session_state["page"] = 1

if pcol1.button("⬅️ Página anterior"):
    st.session_state["page"] = max(1, st.session_state["page"] - 1)

if pcol2.button("➡️ Próxima página"):
    st.session_state["page"] += 1

from_ = (st.session_state["page"] - 1) * int(size)
st.caption(f"Página atual: {st.session_state['page']} (offset={from_})")

if st.button("Buscar"):
    client = get_client(host, port, use_ssl, OPENSEARCH_USER, OPENSEARCH_PASS)

    try:
        if not client.indices.exists(index=index_name):
            st.error(f"Índice '{index_name}' não existe. Faça a coleta/indexação ou crie o índice.")
            st.stop()
    except Exception as e:
        st.error(f"Erro ao acessar OpenSearch: {e}")
        st.stop()

    filtros = {"tipo_sigla": f_tipo.strip(), "ano": f_ano.strip(), "numero": f_num.strip(), "versao": f_versao.strip()}
    body = montar_query(expr, filtros, size=size, from_=from_)

    try:
        resp = client.search(index=index_name, body=body)
    except Exception as e:
        st.error(f"Erro na busca: {e}")
        st.stop()

    hits = resp.get("hits", {}).get("hits", [])
    total = resp.get("hits", {}).get("total", {})
    total_val = total.get("value", total) if isinstance(total, dict) else total

    st.success(f"Resultados retornados: {len(hits)} | Total estimado: {total_val}")

    for h in hits:
        src = h.get("_source", {})
        score = h.get("_score", 0)

        meta = f"{src.get('tipo_sigla')} {src.get('numero')}/{src.get('ano')} — {src.get('versao')}"
        st.markdown(f"### {meta}")
        st.caption(f"doc_id: {src.get('doc_id')} | score: {score:.3f}")
        if src.get("url"):
            st.markdown(src["url"])

        trecho = ""
        hl = h.get("highlight", {}).get("texto")
        if hl:
            trecho = hl[0]

        if trecho:
            st.markdown(trecho, unsafe_allow_html=True)
        else:
            # fallback
            texto = src.get("texto", "")
            st.write(texto[:600] + ("…" if len(texto) > 600 else ""))

        st.divider()
