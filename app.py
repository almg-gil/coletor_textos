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
Este app:
1. Busca automaticamente as normas **via Dados Abertos da ALMG**
2. Permite selecionar o **ano**
3. Coleta os textos **Original e Consolidado**
4. Gera um CSV final
""")

# =================================================
# FUNÇÃO: BUSCAR NORMAS VIA API OFICIAL
# =================================================
def buscar_normas_ano_api(ano):
    url = "https://dadosabertos.almg.gov.br/ws/legislacaoNorma"
    params = {
        "formato": "json",
        "ano": ano
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    dados = r.json()

    lista = dados.get("list", [])
    if not lista:
        return pd.DataFrame()

    df = pd.DataFrame(lista)

    # Normalização mínima
    df = df.rename(columns={
        "tipo": "tipo_sigla",
        "numero": "numero",
        "ano": "ano"
    })

    return df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()

# =================================================
# FUNÇÃO: GERAR LINKS
# =================================================
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

# =================================================
# FUNÇÃO: EXTRAÇÃO DE TEXTO HTML
# (VERSÃO QUE VOCÊ VALIDOU)
# =================================================
def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Tentativa 1: LEI / DEC
        span = soup.find(
            "span",
            class_="js_interpretarLinks textNorma js_interpretarLinksDONE"
        )
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Tentativa 2: DCS / DCE etc.
        main = soup.find("main")
        if main:
            for tag in main.find_all(
                ["nav", "header", "footer", "script", "style", "button", "aside"]
            ):
                tag.decompose()

            for div in main.find_all("div"):
                if "compartilhar" in div.get_text(strip=True).lower():
                    div.decompose()

            texto = main.get_text(separator="\n", strip=True)

            for marcador in ["DELIBERA", "RESOLVE", "Art. 1º", "Art. 1o", "Art. 1"]:
                if marcador in texto:
                    return marcador + "\n" + texto.split(marcador, 1)[-1].strip()

            if len(texto) > 100:
                return texto.strip()

        return ""
    except Exception:
        return ""

# =================================================
# INTERFACE
# =================================================
ano_escolhido = st.selectbox(
    "📅 Selecione o ano",
    list(range(2026, 1946, -1))
)

if st.button("🔎 Buscar normas do ano"):
    with st.spinner("Buscando normas via Dados Abertos da ALMG…"):
        try:
            df_base = buscar_normas_ano_api(ano_escolhido)
        except Exception as e:
            st.error(f"Erro ao acessar a API: {e}")
            st.stop()

    if df_base.empty:
        st.warning("Nenhuma norma encontrada para o ano selecionado.")
        st.stop()

    st.success(f"✅ {len(df_base)} normas encontradas para {ano_escolhido}")
    st.dataframe(df_base.head(20))

    if len(df_base) > 50:
        st.warning("⚠️ Limite temporário: até 50 normas por execução.")
        st.stop()

    if st.button(f"🚀 Coletar textos das {len(df_base)} normas"):
        resultados = []
        barra = st.progress(0)
        total = len(df_base)

        for i, row in df_base.iterrows():
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
        st.dataframe(df_resultado.head(50))

        buffer = BytesIO()
        df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")

        st.download_button(
            "⬇️ Baixar CSV com os textos",
            data=buffer.getvalue(),
            file_name=f"textos_normas_{ano_escolhido}.csv",
            mime="text/csv"
        )
