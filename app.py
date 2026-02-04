import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

st.set_page_config(page_title="Coletor de Textos da ALMG", layout="wide")
st.title("📄 Coletor de Normas da ALMG (Texto Original e Consolidado)")

st.markdown("Envie um arquivo com as colunas: `tipo_sigla`, `numero`, `ano`.")

# Função para gerar links
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

# Função para extrair texto da norma
def extrair_texto_html(url):
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        div = soup.find("div", class_="texto-normal") or soup.find("div", id="corpo")
        if div:
            return div.get_text(separator="\n", strip=True)
        else:
            return "❌ Texto não encontrado na estrutura HTML"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"

# Upload do arquivo
arquivo = st.file_uploader("📥 Envie um CSV ou Excel", type=["csv", "xlsx"])

if arquivo:
    try:
        if arquivo.name.endswith(".csv"):
            df = pd.read_csv(arquivo, dtype=str)
        else:
            df = pd.read_excel(arquivo, dtype=str)
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        st.stop()

    colunas_necessarias = {"tipo_sigla", "numero", "ano"}
    if not colunas_necessarias.issubset(df.columns):
        st.error("Arquivo deve conter as colunas: tipo_sigla, numero, ano")
        st.stop()

    # Remover duplicados e nulos
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()

    if st.button("🚀 Iniciar Coleta de Textos"):
        st.info(f"Buscando textos para {len(df)} normas... Aguarde.")

        resultados = []
        progress = st.progress(0)
        for i, row in df.iterrows():
            tipo, numero, ano = row["tipo_sigla"], row["numero"], row["ano"]
            links = gerar_links(tipo, numero, ano)

            for versao, url in links.items():
                texto = extrair_texto_html(url)
                resultados.append({
                    "tipo_sigla": tipo,
                    "numero": numero,
                    "ano": ano,
                    "versao": versao,
                    "url": url,
                    "texto": texto
                })
            progress.progress((i + 1) / len(df))

        df_final = pd.DataFrame(resultados)
        st.success("✅ Coleta finalizada!")
        st.dataframe(df_final.head(50))

        # Botão de download
        buffer = BytesIO()
        df_final.to_csv(buffer, index=False, encoding="utf-8-sig")
        st.download_button("⬇️ Baixar CSV com os textos", data=buffer.getvalue(),
                           file_name="textos_normas_almg.csv", mime="text/csv")
