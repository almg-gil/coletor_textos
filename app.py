import streamlit as st
import pandas as pd
import requests
from io import BytesIO

st.set_page_config(page_title="Coletor Automático de Textos da ALMG", layout="wide")
st.title("📄 Coletor Automático de Normas da ALMG")
st.markdown("1. Digite o ano desejado  \n2. O app buscará todas as normas publicadas nesse ano  \n3. Os textos serão extraídos via API oficial da ALMG")

# ---------------------------
# Função para extrair texto via API
# ---------------------------
def extrair_texto_api(tipo, numero, ano, versao):
    try:
        tipo_doc = 142 if versao == "Original" else 572
        url = f"https://dadosabertos.almg.gov.br/api/v2/legislacao/mineira/{tipo}/{numero}/{ano}/documento"
        params = {
            "conteudo": "true",
            "texto": "false",
            "tipoDoc": tipo_doc
        }
        headers = {"accept": "application/json"}
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("conteudo", "").strip()
    except:
        return ""  # Se der erro, retorna texto vazio

# ---------------------------
# Função para buscar lista de normas por ano
# ---------------------------
def obter_normas_por_ano(ano):
    url = f"https://dadosabertos.almg.gov.br/arquivos/legislacao-mineira/{ano}.csv"
    try:
        df = pd.read_csv(url, sep=";", encoding="utf-8")
        df = df[["tipo", "numero", "ano"]].dropna().drop_duplicates()
        df.columns = ["tipo_sigla", "numero", "ano"]
        return df
    except Exception as e:
        st.error(f"Erro ao baixar normas do ano {ano}: {e}")
        return pd.DataFrame()

# ---------------------------
# Interface Streamlit
# ---------------------------
ano_desejado = st.text_input("📅 Digite o ano desejado", placeholder="Ex: 2025")

if ano_desejado:
    if st.button("🔎 Buscar normas"):
        st.info(f"Buscando normas publicadas em {ano_desejado}…")
        df_normas = obter_normas_por_ano(ano_desejado)

        if df_normas.empty:
            st.warning("⚠️ Nenhuma norma encontrada para o ano informado.")
            st.stop()

        st.success(f"✅ {len(df_normas)} normas encontradas para {ano_desejado}. Iniciando coleta dos textos…")

        resultados = []
        barra = st.progress(0)

        for i, row in df_normas.iterrows():
            tipo, numero, ano = row["tipo_sigla"], row["numero"], row["ano"]

            for versao in ["Original", "Consolidado"]:
                texto = extrair_texto_api(tipo, numero, ano, versao)
                resultados.append({
                    "tipo_sigla": tipo,
                    "numero": numero,
                    "ano": ano,
                    "versao": versao,
                    "texto": texto
                })

            barra.progress((i + 1) / len(df_normas))

        df_resultado = pd.DataFrame(resultados)
        st.success("✅ Coleta concluída!")
        st.dataframe(df_resultado.head(50))

        # Exportar como CSV
        buffer = BytesIO()
        df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")
        st.download_button(
            label="⬇️ Baixar CSV com os textos",
            data=buffer.getvalue(),
            file_name=f"textos_normas_{ano_desejado}.csv",
            mime="text/csv"
        )
