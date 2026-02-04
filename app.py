import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

# =================================================
# CONFIGURAÇÃO DA PÁGINA
# =================================================
st.set_page_config(page_title="Coletor de Textos ALMG", layout="wide")
st.title("📄 Coletor de Textos de Normas da ALMG")

st.markdown("""
Envie um arquivo `.csv` ou `.xlsx` contendo as colunas:

- `tipo_sigla`
- `numero`
- `ano`

Selecione o(s) ano(s) desejado(s) e clique em **Coletar textos**.
""")

# =================================================
# FUNÇÃO DE EXTRAÇÃO DE TEXTO
# =================================================
def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # -------------------------------------------------
        # 1️⃣ CASO PADRÃO (LEI, DEC etc.)
        # -------------------------------------------------
        span_norma = soup.find(
            "span",
            class_="js_interpretarLinks textNorma js_interpretarLinksDONE"
        )
        if span_norma:
            texto = span_norma.get_text(separator="\n", strip=True)
            if texto:
                return texto

        # -------------------------------------------------
        # 2️⃣ CASO DCS, DCE E OUTROS (texto no <main>)
        # -------------------------------------------------
        main = soup.find("main")
        if not main:
            return ""

        # Remove APENAS elementos de interface
        for tag in main.find_all([
            "nav", "header", "footer", "script", "style",
            "iframe", "form", "button", "svg"
        ]):
            tag.decompose()

        # Remove blocos explicitamente de compartilhamento
        for elem in main.find_all(["div", "span", "a"]):
            txt = elem.get_text(strip=True).lower()
            if any(p in txt for p in [
                "compartilhar",
                "whatsapp",
                "telegram",
                "facebook",
                "twitter",
                "imprimir",
                "solicitar norma em áudio"
            ]):
                elem.decompose()

        # Texto final
        texto_bruto = main.get_text(separator="\n", strip=True)

        # Normalização leve (sem agressividade)
        linhas = [l.strip() for l in texto_bruto.splitlines() if l.strip()]
        texto_final = "\n".join(linhas)

        # Elimina apenas mensagens artificiais do scraper
        mensagens_erro = [
            "texto não encontrado",
            "não foi possível localizar",
            "erro ao acessar"
        ]
        if any(msg in texto_final.lower() for msg in mensagens_erro):
            return ""

        return texto_final

    except Exception:
        return ""

# =================================================
# FUNÇÃO PARA GERAR LINKS
# =================================================
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

# =================================================
# UPLOAD DO ARQUIVO
# =================================================
arquivo = st.file_uploader(
    "📤 Envie o arquivo com as normas",
    type=["csv", "xlsx"]
)

if arquivo:
    try:
        df = pd.read_csv(arquivo) if arquivo.name.endswith(".csv") else pd.read_excel(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        st.stop()

    colunas = {"tipo_sigla", "numero", "ano"}
    if not colunas.issubset(df.columns):
        st.error("⚠️ O arquivo deve conter as colunas: tipo_sigla, numero, ano")
        st.stop()

    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)

    anos = sorted(df["ano"].unique(), reverse=True)
    anos_sel = st.multiselect("📅 Selecione o(s) ano(s)", anos)

    if anos_sel:
        df_filtrado = df[df["ano"].isin(anos_sel)]
        st.markdown(f"🔎 Normas selecionadas: **{len(df_filtrado)}**")

        if len(df_filtrado) > 50:
            st.warning("⚠️ Limite atual: até 50 normas por execução.")
            st.stop()

        if st.button(f"🚀 Coletar textos de {len(df_filtrado)} normas"):
            st.info("⏳ Coletando textos…")
            resultados = []
            barra = st.progress(0)
            total = len(df_filtrado)

            for i, row in df_filtrado.iterrows():
                links = gerar_links(row["tipo_sigla"], row["numero"], row["ano"])

                for versao, url in links.items():
                    resultados.append({
                        "tipo_sigla": row["tipo_sigla"],
                        "numero": row["numero"],
                        "ano": row["ano"],
                        "versao": versao,
                        "url": url,
                        "texto": extrair_texto_html(url)
                    })

                barra.progress((i + 1) / total)

            df_resultado = pd.DataFrame(resultados)
            st.success("✅ Coleta finalizada")
            st.dataframe(df_resultado)

            buffer = BytesIO()
            df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")

            st.download_button(
                "⬇️ Baixar CSV com os textos",
                data=buffer.getvalue(),
                file_name="textos_normas_almg.csv",
                mime="text/csv"
            )
