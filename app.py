import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import re
import time

# ------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# ------------------------------------------
st.set_page_config(page_title="Rastreamento de Normas ALMG", layout="wide")
st.title("🔍 Coletor Automático de Normas - ALMG")
st.markdown("Coleta automática de normas **diretamente do site da ALMG**, sem necessidade de subir CSV.")

# ------------------------------------------
# FUNÇÃO: Buscar normas por ano
# ------------------------------------------
def buscar_normas_por_ano(ano, max_normas=200):
    url_base = f"https://www.almg.gov.br/legislacao-mineira/busca.html"
    normas = []
    pagina = 1
    coletadas = 0

    st.info(f"🔄 Buscando normas publicadas em {ano}...")

    while coletadas < max_normas:
        try:
            params = {
                "termo": "",
                "ano": ano,
                "tipoBusca": "texto",
                "pagina": pagina
            }
            r = requests.get(url_base, params=params, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            resultados = soup.find_all("a", href=re.compile(r"/legislacao-mineira/texto/"))

            if not resultados:
                break

            for a in resultados:
                href = a["href"]
                match = re.search(r"/texto/([A-Z]+)/(\d+)/(\d{4})", href)
                if match:
                    tipo, numero, ano_url = match.groups()
                    if int(ano_url) == int(ano):
                        normas.append({
                            "tipo_sigla": tipo,
                            "numero": numero,
                            "ano": ano_url
                        })
                        coletadas += 1
                        if coletadas >= max_normas:
                            break
            pagina += 1
            time.sleep(1)
        except Exception as e:
            st.error(f"Erro ao buscar página {pagina}: {e}")
            break

    return pd.DataFrame(normas)

# ------------------------------------------
# FUNÇÃO: Gerar links
# ------------------------------------------
def gerar_links(tipo, numero, ano):
    base = f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"
    return {
        "Original": base + "/",
        "Consolidado": base + "/?cons=1"
    }

# ------------------------------------------
# FUNÇÃO: Extrair texto da norma
# ------------------------------------------
def extrair_texto_html(url):
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Versão tradicional com <span>
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Versão por <main>
        main = soup.find("main")
        if main:
            for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
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
    except:
        return ""

# ------------------------------------------
# INTERFACE
# ------------------------------------------
anos_opcoes = list(reversed(range(2000, 2027)))
ano_escolhido = st.selectbox("📅 Escolha o ano para buscar normas", anos_opcoes)

limite = st.slider("🔢 Limite de normas a processar", min_value=10, max_value=500, value=100)

if st.button("🚀 Iniciar busca e coleta"):
    df_normas = buscar_normas_por_ano(ano_escolhido, max_normas=limite)

    if df_normas.empty:
        st.warning("Nenhuma norma encontrada para o ano selecionado.")
        st.stop()

    st.success(f"✅ {len(df_normas)} normas localizadas. Iniciando extração de textos…")
    barra = st.progress(0)
    resultados = []

    for i, row in df_normas.iterrows():
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
        barra.progress((i + 1) / len(df_normas))

    df_resultado = pd.DataFrame(resultados)
    st.success("🎉 Coleta concluída!")
    st.dataframe(df_resultado.head(50))

    buffer = BytesIO()
    df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")
    st.download_button("⬇️ Baixar CSV com os textos", data=buffer.getvalue(), file_name=f"normas_{ano_escolhido}.csv", mime="text/csv")
