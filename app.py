import streamlit as st
import requests
from bs4 import BeautifulSoup

# -------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -------------------------------------------------
st.set_page_config(
    page_title="Teste de Extração ALMG",
    layout="wide"
)

st.title("🔍 Teste de Extração de Texto - ALMG")
st.markdown("Este app testa a extração de texto da norma `DCE/1/2020` diretamente do site da ALMG.")

# -------------------------------------------------
# FUNÇÃO DE EXTRAÇÃO
# -------------------------------------------------
def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Tentativa 1: texto de span (LEIs e DEC)
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Tentativa 2: pegar tudo do <main>
        main = soup.find("main")
        if main:
            for tag in main.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            texto_bruto = main.get_text(separator="\n", strip=True)

            # Busca ponto inicial do texto útil
            for marcador in ["DELIBERA", "Art. 1º", "RESOLVE"]:
                if marcador in texto_bruto:
                    texto_util = texto_bruto.split(marcador, 1)[-1]
                    return marcador + "\n" + texto_util.strip()

            # Se nenhum marcador, retorna tudo
            if len(texto_bruto) > 100:
                return texto_bruto.strip()

        return "❌ Texto não encontrado no HTML"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"

# -------------------------------------------------
# INTERFACE DE TESTE
# -------------------------------------------------
URL_TESTE = "https://www.almg.gov.br/legislacao-mineira/texto/DCE/1/2020/"

if st.button("🔍 Testar extração da norma DCE/1/2020"):
    st.info(f"Acessando: {URL_TESTE}")
    texto = extrair_texto_html(URL_TESTE)
    st.text_area("📄 Texto extraído da norma:", texto, height=500)
