import streamlit as st
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import shutil

from whoosh import index
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import MultifieldParser, AndGroup, OrGroup
from whoosh.analysis import RegexTokenizer, LowercaseFilter, StopFilter, StemFilter, CharsetFilter
from whoosh.support.charset import default_charset

# -------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -------------------------------------------------
st.set_page_config(
    page_title="Busca Booleana – Normas ALMG",
    layout="wide"
)

st.title("📚 Banco de Normas ALMG + 🔎 Busca Booleana")

# -------------------------------------------------
# CONSTANTES
# -------------------------------------------------
TIPOS_NORMA = [
    "LEI", "DEC", "DNE", "DSN", "PRT",
    "RAL", "DLB", "PTC", "DCS", "LCP"
]

INDICE_DIR = Path("indice_whoosh")

# -------------------------------------------------
# FUNÇÕES DE COLETA
# -------------------------------------------------
def gerar_url(tipo, numero, ano, consolidado=False):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return base + ("/?cons=1" if consolidado else "/")


@st.cache_data(show_spinner=False)
def extrair_texto_html(url):
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        main = soup.find("main")
        if not main:
            return None

        for tag in main.find_all(["nav", "header", "footer", "script", "style", "aside"]):
            tag.decompose()

        texto = main.get_text(separator="\n", strip=True)

        if len(texto) > 100:
            return texto

        return None

    except Exception:
        return None


# -------------------------------------------------
# WHOOSH – ANALYZER E SCHEMA
# -------------------------------------------------
def analyzer_pt():
    a = RegexTokenizer() | LowercaseFilter() | CharsetFilter(default_charset)
    try:
        a |= StopFilter(lang="pt")
        a |= StemFilter(lang="pt")
    except Exception:
        pass
    return a


SCHEMA = Schema(
    doc_id=ID(stored=True, unique=True),
    tipo=ID(stored=True),
    numero=ID(stored=True),
    ano=ID(stored=True),
    versao=ID(stored=True),
    url=ID(stored=True),
    texto=TEXT(stored=True, analyzer=analyzer_pt())
)


def criar_indice():
    if INDICE_DIR.exists():
        shutil.rmtree(INDICE_DIR)
    INDICE_DIR.mkdir()
    return index.create_in(INDICE_DIR, SCHEMA)


def abrir_indice():
    if not INDICE_DIR.exists():
        return None
    return index.open_dir(INDICE_DIR)


# -------------------------------------------------
# SIDEBAR – CONFIGURAÇÃO DO BANCO
# -------------------------------------------------
st.sidebar.header("⚙️ Configuração do Banco")

if "tipos_sel" not in st.session_state:
    st.session_state["tipos_sel"] = TIPOS_NORMA.copy()

if st.sidebar.button("✅ Selecionar todos"):
    st.session_state["tipos_sel"] = TIPOS_NORMA.copy()

if st.sidebar.button("❌ Limpar seleção"):
    st.session_state["tipos_sel"] = []

tipos_selecionados = st.sidebar.multiselect(
    "Tipos de norma",
    TIPOS_NORMA,
    default=st.session_state["tipos_sel"]
)

st.session_state["tipos_sel"] = tipos_selecionados

ano_ini = st.sidebar.number_input("Ano inicial", value=1989)
ano_fim = st.sidebar.number_input("Ano final", value=2025)

max_numero = st.sidebar.number_input(
    "Número máximo por ano (limite técnico)",
    value=300,
    step=50
)

versao_cons = st.sidebar.checkbox("Incluir versão consolidada", value=True)

# -------------------------------------------------
# CONSTRUÇÃO DO BANCO
# -------------------------------------------------
st.subheader("📥 Construção / Atualização do Banco")

if st.button("🚀 Construir banco de dados"):
    if not tipos_selecionados:
        st.warning("Selecione ao menos um tipo de norma.")
        st.stop()

    ix = criar_indice()
    writer = ix.writer(limitmb=512)

    barra = st.progress(0)
    total = len(tipos_selecionados) * (ano_fim - ano_ini + 1) * max_numero
    contador = 0
    inseridos = 0

    for tipo in tipos_selecionados:
        for ano in range(ano_ini, ano_fim + 1):
            for numero in range(1, max_numero + 1):
                contador += 1

                url = gerar_url(tipo, numero, ano, False)
                texto = extrair_texto_html(url)

                if texto:
                    doc_id = f"{tipo}-{numero}-{ano}-O"
                    writer.add_document(
                        doc_id=doc_id,
                        tipo=tipo,
                        numero=str(numero),
                        ano=str(ano),
                        versao="Original",
                        url=url,
                        texto=texto
                    )
                    inseridos += 1

                    if versao_cons:
                        url_c = gerar_url(tipo, numero, ano, True)
                        texto_c = extrair_texto_html(url_c)
                        if texto_c:
                            writer.add_document(
                                doc_id=f"{tipo}-{numero}-{ano}-C",
                                tipo=tipo,
                                numero=str(numero),
                                ano=str(ano),
                                versao="Consolidado",
                                url=url_c,
                                texto=texto_c
                            )
                            inseridos += 1

                barra.progress(contador / total)

    writer.commit()
    st.success(f"✅ Banco construído! Documentos indexados: {inseridos}")

# -------------------------------------------------
# BUSCA
# -------------------------------------------------
st.divider()
st.subheader("🔎 Busca Booleana nos Textos")

ix = abrir_indice()
if not ix:
    st.info("O banco ainda não foi criado.")
    st.stop()

consulta = st.text_input(
    "Consulta",
    placeholder='Ex: (educação OR escola) AND NOT revoga'
)

col1, col2 = st.columns(2)
with col1:
    operador = st.selectbox("Operador padrão", ["AND", "OR"])
with col2:
    limite = st.number_input("Máx. resultados", 10, 2000, 200)

if consulta:
    parser = MultifieldParser(
        ["texto", "tipo", "ano", "numero"],
        schema=ix.schema,
        group=AndGroup if operador == "AND" else OrGroup
    )

    try:
        q = parser.parse(consulta)
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        st.stop()

    with ix.searcher() as searcher:
        resultados = searcher.search(q, limit=limite)

        st.markdown(f"**{len(resultados)} resultado(s)**")

        for r in resultados:
            trecho = r.highlights("texto") or r["texto"][:400]
            st.markdown(
                f"""
**{r['tipo']} {r['numero']}/{r['ano']} – {r['versao']}**  
{r['url']}

{trecho}
<hr/>
""",
                unsafe_allow_html=True
            )
