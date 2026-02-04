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
    **Fluxo do app:**
    1. Envie um CSV ou Excel com `tipo_sigla`, `numero`, `ano`
    2. Selecione o(s) **ano(s)**
    3. Clique para gerar o arquivo com os textos
    """
)

# -------------------------------------------------
# FUNÇÕES
# -------------------------------------------------
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 🔴 ESTRUTURA CORRETA DO TEXTO DA ALMG
        span = soup.find(
            "span",
            class_="js_interpretarLinks textNorma js_interpretarLinksDONE"
        )

        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto
            else:
                return "❌ Texto encontrado, mas vazio"
        else:
            return "❌ Texto não encontrado no span textNorma"

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
        st.error("⚠️ O arquivo deve conter exatamente: tipo_sigla, numero, ano")
        st.stop()

    # Limpeza
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)

    # -------------------------------------------------
    # SELEÇÃO DE ANO
    # -------------------------------------------------
    anos_disponiveis = sorted(df["ano"].unique(), reverse=True)
    anos_selecionados = st.multiselect(
        "📅 Selecione o(s) ano(s)",
        anos_disponiveis
    )

    if anos_selecionados:
        df_filtrado = df[df["ano"].isin(anos_selecionados)]
        st.markdown(f"🔎 Normas encontradas: **{len(df_filtrado)}**")

        if len(df_filtrado) == 0:
            st.warning("⚠️ Nenhuma norma para os anos selecionados.")
        else:
            if st.button(f"🚀 Gerar textos para {len(df_filtrado)} normas"):
                st.info("Coletando textos… aguarde.")

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
                # DOWNLOAD
                # -------------------------------------------------
                buffer = BytesIO()
                df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")

                st.download_button(
                    "⬇️ Baixar CSV com os textos",
                    data=buffer.getvalue(),
                    file_name="textos_normas_almg.csv",
                    mime="text/csv"
                )
