import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

# -------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -------------------------------------------------
st.set_page_config(page_title="Coletor de Textos ALMG", layout="wide")
st.title("📄 Coletor de Textos de Normas da ALMG")
st.markdown("1. Envie um arquivo com `tipo_sigla`, `numero`, `ano`  \n2. Selecione os anos  \n3. Gere o CSV com os textos")

# -------------------------------------------------
# FUNÇÃO DE EXTRAÇÃO DE TEXTO HTML
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

        # Tentativa 1: span tradicional usado por LEI/DEC
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Tentativa 2: extrair do <main> (usado por DCS, DCE, etc.)
        main = soup.find("main")
        if main:
            # Remove elementos irrelevantes
            for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
                tag.decompose()

            # Remove blocos com "compartilhar"
            for div in main.find_all("div"):
                if "compartilhar" in div.get_text(strip=True).lower():
                    div.decompose()

            texto = main.get_text(separator="\n", strip=True)

            # Captura a partir de palavras-chave
            for marcador in ["DELIBERA", "RESOLVE", "Art. 1º", "Art. 1o", "Art. 1"]:
                if marcador in texto:
                    return marcador + "\n" + texto.split(marcador, 1)[-1].strip()

            # Se não encontrou marcador, retorna tudo (se for significativo)
            if len(texto) > 100:
                return texto.strip()

        return "❌ Texto não encontrado"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"

# -------------------------------------------------
# GERAÇÃO DAS URLs
# -------------------------------------------------
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

# -------------------------------------------------
# UPLOAD E TRATAMENTO DO ARQUIVO
# -------------------------------------------------
arquivo = st.file_uploader("📤 Envie um arquivo CSV ou Excel", type=["csv", "xlsx"])

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

    # Limpeza básica
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)

    # Filtro por ano
    anos_disponiveis = sorted(df["ano"].unique(), reverse=True)
    anos_selecionados = st.multiselect("📅 Selecione o(s) ano(s)", anos_disponiveis)

    if anos_selecionados:
        df_filtrado = df[df["ano"].isin(anos_selecionados)]
        st.markdown(f"🔎 Normas encontradas: **{len(df_filtrado)}**")

        # Limite de segurança
        if len(df_filtrado) > 50:
            st.warning("⚠️ Limite temporário: selecione até 50 normas por vez para evitar travamentos.")
            st.stop()

        if st.button(f"🚀 Coletar textos para {len(df_filtrado)} normas"):
            st.info("🔄 Coletando… aguarde alguns minutos.")
            resultados = []
            barra = st.progress(0)
            total = len(df_filtrado)

            for i, row in df_filtrado.iterrows():
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

                barra.progress((i + 1) / total)

            df_resultado = pd.DataFrame(resultados)
            st.success("✅ Coleta finalizada!")
            st.dataframe(df_resultado.head(50))

            # Download do CSV
            buffer = BytesIO()
            df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")
            st.download_button("⬇️ Baixar CSV com os textos", data=buffer.getvalue(),
                               file_name="textos_normas_almg.csv", mime="text/csv")
