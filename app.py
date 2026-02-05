import streamlit as st
import pandas as pd
import requests
from io import BytesIO

# -------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -------------------------------------------------
st.set_page_config(page_title="Coletor de Textos ALMG (API)", layout="wide")
st.title("📄 Coletor de Textos de Normas da ALMG")
st.markdown("1. Envie um arquivo com `tipo_sigla`, `numero`, `ano`  \n2. Selecione os anos desejados  \n3. Gere o CSV com os textos via API oficial da ALMG")

# -------------------------------------------------
# FUNÇÃO DE EXTRAÇÃO USANDO A API
# -------------------------------------------------
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
        conteudo = data.get("conteudo", "").strip()

        if conteudo:
            return conteudo
        else:
            return ""  # Retorna vazio se não encontrou conteúdo

    except Exception as e:
        return ""  # Em caso de erro, retorna vazio para manter consistência

# -------------------------------------------------
# UPLOAD E TRATAMENTO DO ARQUIVO
# -------------------------------------------------
arquivo = st.file_uploader("📤 Envie um arquivo CSV ou Excel com as normas", type=["csv", "xlsx"])

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

    # Limpeza
    df = df[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
    df["ano"] = df["ano"].astype(str)

    # Filtro por ano
    anos_disponiveis = sorted(df["ano"].unique(), reverse=True)
    anos_selecionados = st.multiselect("📅 Selecione o(s) ano(s)", anos_disponiveis)

    if anos_selecionados:
        df_filtrado = df[df["ano"].isin(anos_selecionados)]
        st.markdown(f"🔎 Normas encontradas: **{len(df_filtrado)}**")

        if len(df_filtrado) > 100:
            st.warning("⚠️ Limite temporário: selecione até 100 normas por vez para evitar lentidão.")
            st.stop()

        if st.button(f"🚀 Coletar textos via API para {len(df_filtrado)} normas"):
            st.info("🔄 Coletando… aguarde alguns minutos.")
            resultados = []
            barra = st.progress(0)
            total = len(df_filtrado)

            for idx, row in df_filtrado.iterrows():
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

                barra.progress((idx + 1) / total)

            df_resultado = pd.DataFrame(resultados)
            st.success("✅ Coleta finalizada!")
            st.dataframe(df_resultado.head(50))

            # Download do CSV
            buffer = BytesIO()
            df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")
            st.download_button("⬇️ Baixar CSV com os textos", data=buffer.getvalue(),
                               file_name="textos_normas_api_almg.csv", mime="text/csv")
