import streamlit as st
import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(layout="wide")
st.title("Coletor de Textos ALMG - Histórico Completo")

API_BASE = "https://dadosabertos.almg.gov.br/api/v2/legislacao/mineira"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}

def buscar_texto(tipo, numero, ano, tipo_doc):
    url = f"{API_BASE}/{tipo}/{numero}/{ano}/documento"
    params = {
        "conteudo": "true",
        "texto": "true",
        "tipoDoc": str(tipo_doc)
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()

        lista = data.get("listaNormaDocumento", [])
        if not lista:
            return None

        return lista[0].get("texto", None)

    except Exception:
        return None


def processar_norma(row):
    tipo = row["tipo_sigla"]
    numero = row["numero"]
    ano = row["ano"]

    original = buscar_texto(tipo, numero, ano, 142)
    consolidado = buscar_texto(tipo, numero, ano, 572)

    return {
        "tipo_sigla": tipo,
        "numero": numero,
        "ano": ano,
        "texto_original": original,
        "texto_consolidado": consolidado
    }


st.markdown("### Envie um CSV com colunas: tipo_sigla, numero, ano")

arquivo = st.file_uploader("Arquivo CSV", type=["csv"])

if arquivo:

    df = pd.read_csv(arquivo)

    anos = sorted(df["ano"].unique())
    anos_sel = st.multiselect("Selecione os anos", anos, default=anos)

    df = df[df["ano"].isin(anos_sel)]

    st.write(f"Normas selecionadas: {len(df)}")

    if st.button("🚀 Iniciar coleta"):

        resultados = []
        progress = st.progress(0)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(processar_norma, row) for _, row in df.iterrows()]

            for i, future in enumerate(as_completed(futures)):
                resultados.append(future.result())
                progress.progress((i + 1) / len(futures))

        df_resultado = pd.DataFrame(resultados)

        st.success("Coleta finalizada!")

        st.dataframe(df_resultado.head())

        csv = df_resultado.to_csv(index=False).encode("utf-8-sig")

        st.download_button(
            label="⬇️ Baixar CSV completo",
            data=csv,
            file_name="textos_normas_almg.csv",
            mime="text/csv"
        )
