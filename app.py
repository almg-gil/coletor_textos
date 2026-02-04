import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

# -------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -------------------------------------------------
st.set_page_config(
    page_title="Coletor de Textos da ALMG",
    layout="wide"
)

st.title("📄 Coletor de Textos de Normas da ALMG")
st.markdown(
    """
    **Como usar:**
    1. Envie um arquivo CSV ou Excel com as colunas `tipo_sigla`, `numero`, `ano`;
    2. Selecione um ou mais **anos**;
    3. Clique no botão para gerar o arquivo com os textos.
    """
)

# -------------------------------------------------
# FUNÇÕES AUXILIARES
# -------------------------------------------------
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

def extrair_texto_html(url):
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Estruturas mais comuns do site da ALMG
        div = soup.find("div", class_="texto-normal") or soup.find("div", id="corpo")

        if div:
            return div.get_text(separator="\n", strip=True)
        else:
            return "❌ Texto não encontrado na estrutura HTML"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"

# -------------------------------------------------
# UPLOAD DO ARQUIVO
# -------------------------------------------------
arquivo = st.file_uploader(
    "📥 Envie um arquivo CSV ou Excel",
    type=["csv", "xlsx"]
)

if arquivo:
    # -------------------------------------------------
    # LEITURA DO ARQUIVO
    # -------------------------------------------------
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
        st.error("⚠️ O arquivo deve conter exatamente as colunas: tipo_sigla, numero, ano")
        st.stop()

    # Limpeza básica
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)

    # -------------------------------------------------
    # SELEÇÃO DE ANO
    # -------------------------------------------------
    anos_disponiveis = sorted(df["ano"].unique(), reverse=True)

    anos_selecionados = st.multiselect(
        "📅 Selecione o(s) ano(s) para coletar os textos",
        anos_disponiveis
    )

    if anos_selecionados:
        df_filtrado = df[df["ano"].isin(anos_selecionados)]
        st.markdown(f"🔎 Normas encontradas: **{len(df_filtrado)}**")

        # -------------------------------------------------
        # BOTÃO DE EXECUÇÃO
        # -------------------------------------------------
        if len(df_filtrado) == 0:
            st.warning("⚠️ Nenhuma norma encontrada para o(s) ano(s) selecionado(s).")
        else:
            if st.button(f"🚀 Gerar textos para {len(df_filtrado)} normas"):
                st.info("Coletando textos… isso pode levar alguns minutos.")

                resultados = []
                barra = st.progress(0.0)
                total = len(df_filtrado)
                contador = 0

                for _, row in df_filtrado.iterrows():
                    tipo = row["tipo_sigla"]
                    numero = row["numero"]
                    ano = row["ano"]

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

                    contador += 1
                    barra.progress(min(contador / total, 1.0))

                df_resultado = pd.DataFrame(resultados)

                st.success("✅ Coleta finalizada com sucesso!")
                st.dataframe(df_resultado.head(50))

                # -------------------------------------------------
                # DOWNLOAD DO CSV
                # -------------------------------------------------
                buffer = BytesIO()
                df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")

                st.download_button(
                    "⬇️ Baixar CSV com os textos",
                    data=buffer.getvalue(),
                    file_name="textos_normas_almg.csv",
                    mime="text/csv"
                )
