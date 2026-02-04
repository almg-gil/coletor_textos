import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

st.set_page_config(page_title="Coletor de Textos ALMG", layout="wide")
st.title("📄 Coletor de Textos de Normas da ALMG")
st.markdown("1. Envie um arquivo com `tipo_sigla`, `numero`, `ano`\n2. Selecione os anos\n3. Gere o arquivo com os textos")

# -----------------------
# Função principal de scraping
# -----------------------
def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 1. span tradicional
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # 2. conteúdo dentro do <main>
        main = soup.find("main")
        if main:
            for tag in main.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            texto_bruto = main.get_text(separator="\n", strip=True)

            for marcador in ["DELIBERA", "Art. 1º", "Art. 1o", "RESOLVE"]:
                if marcador in texto_bruto:
                    return marcador + "\n" + texto_bruto.split(marcador, 1)[-1].strip()

            if len(texto_bruto) > 100:
                return texto_bruto.strip()

        return "❌ Texto não encontrado no HTML"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"

# -----------------------
# Geração dos links
# -----------------------
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

# -----------------------
# Upload do arquivo
# -----------------------
arquivo = st.file_uploader("📤 Envie um CSV ou Excel com as normas", type=["csv", "xlsx"])

if arquivo:
    try:
        df = pd.read_csv(arquivo) if arquivo.name.endswith(".csv") else pd.read_excel(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        st.stop()

    obrigatorias = {"tipo_sigla", "numero", "ano"}
    if not obrigatorias.issubset(df.columns):
        st.error("⚠️ O arquivo deve conter as colunas: tipo_sigla, numero, ano")
        st.stop()

    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)

    anos = sorted(df["ano"].unique(), reverse=True)
    anos_selecionados = st.multiselect("📅 Selecione o(s) ano(s):", anos)

    if anos_selecionados:
        df_filtrado = df[df["ano"].isin(anos_selecionados)]
        st.markdown(f"🔎 Normas encontradas: **{len(df_filtrado)}**")

        if len(df_filtrado) > 50:
            st.warning("⚠️ Limite temporário: selecione até 50 normas por vez.")
            st.stop()

        if st.button(f"🚀 Coletar textos para {len(df_filtrado)} normas"):
            st.info("Coletando... aguarde alguns minutos.")
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

            buffer = BytesIO()
            df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")

            st.download_button("⬇️ Baixar CSV com os textos", data=buffer.getvalue(),
                               file_name="textos_normas_almg.csv", mime="text/csv")
