import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(layout="wide")
st.title("Coletor Histórico de Textos ALMG")

API_BASE = "https://dadosabertos.almg.gov.br/api/v2"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}


def listar_normas_por_ano(ano):
    url = f"{API_BASE}/legislacao/mineira"
    params = {
        "ano": ano,
        "pagina": 1,
        "itensPorPagina": 1000
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if resp.status_code != 200:
            return []

        data = resp.json()
        return data.get("listaNorma", [])

    except Exception:
        return []


def buscar_texto(tipo, numero, ano, tipo_doc):
    url = f"{API_BASE}/legislacao/mineira/{tipo}/{numero}/{ano}/documento"
    params = {
        "conteudo": "true",
        "texto": "true",
        "tipoDoc": str(tipo_doc)
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if resp.status_code != 200:
            return None

        data = resp.json()
        lista = data.get("listaNormaDocumento", [])
        if not lista:
            return None

        return lista[0].get("texto", None)

    except Exception:
        return None


anos = st.slider("Selecione o intervalo de anos", 1947, 2026, (1947, 2026))

if st.button("🚀 Iniciar coleta automática"):

    resultados = []
    progress = st.progress(0)

    total_anos = anos[1] - anos[0] + 1
    ano_atual = 0

    for ano in range(anos[0], anos[1] + 1):

        normas = listar_normas_por_ano(ano)

        for norma in normas:
            tipo = norma.get("siglaTipoNorma")
            numero = norma.get("numero")

            texto_original = buscar_texto(tipo, numero, ano, 142)
            texto_consolidado = buscar_texto(tipo, numero, ano, 572)

            resultados.append({
                "tipo_sigla": tipo,
                "numero": numero,
                "ano": ano,
                "texto_original": texto_original,
                "texto_consolidado": texto_consolidado
            })

        ano_atual += 1
        progress.progress(ano_atual / total_anos)

    df = pd.DataFrame(resultados)

    st.success("Coleta finalizada!")

    st.dataframe(df.head())

    csv = df.to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        label="⬇️ Baixar CSV completo",
        data=csv,
        file_name="textos_normas_almg.csv",
        mime="text/csv"
    )
