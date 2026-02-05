import streamlit as st
import pandas as pd
import requests
import time
from io import BytesIO

# -------------------------------
# CONFIGURAÇÃO INICIAL DO APP
# -------------------------------
st.set_page_config(page_title="Coletor de Textos da ALMG", layout="wide")
st.title("📄 Coletor de Textos das Normas da ALMG")
st.markdown("Este app consulta a [API da ALMG](https://dadosabertos.almg.gov.br) para buscar textos oficiais de normas por ano.\n\n> **Atenção:** o processo é lento pois respeita limites de acesso da API.")

# -------------------------------
# FUNÇÃO PARA BUSCAR LISTA DE NORMAS
# -------------------------------
@st.cache_data(show_spinner=False)
def buscar_normas_do_ano(ano):
    try:
        url = f"https://dadosabertos.almg.gov.br/api/v2/legislacaoNorma?formato=json&ano={ano}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"Erro ao buscar normas do ano {ano}: {e}")
        return []

# -------------------------------
# FUNÇÃO PARA BUSCAR TEXTO DE UMA NORMA
# -------------------------------
def buscar_texto_da_norma(tipo, numero, ano, tipo_doc):
    base_url = f"https://dadosabertos.almg.gov.br/api/v2/legislacao/mineira/{tipo}/{numero}/{ano}/documento"
    params = {
        "conteudo": "true",
        "texto": "true",
        "tipoDoc": tipo_doc  # 142 = original, 572 = consolidado
    }
    try:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        dados = r.json()
        if dados and isinstance(dados, list) and "conteudo" in dados[0]:
            return dados[0]["conteudo"]
        else:
            return ""
    except:
        return ""

# -------------------------------
# ENTRADA DO USUÁRIO
# -------------------------------
ano = st.text_input("📅 Digite o ano desejado", value="2026")
if st.button("🔍 Buscar normas"):
    if not ano.isdigit():
        st.error("Ano inválido.")
        st.stop()

    st.info(f"📥 Buscando normas publicadas em {ano}…")
    normas = buscar_normas_do_ano(ano)

    if not normas:
        st.warning("⚠️ Nenhuma norma encontrada para o ano informado.")
        st.stop()

    st.success(f"✅ {len(normas)} normas encontradas.")
    progresso = st.progress(0)
    resultados = []

    for i, norma in enumerate(normas):
        tipo = norma.get("tipo", "").strip()
        numero = norma.get("numero")
        ano_norma = norma.get("ano")

        # Pega textos original e consolidado com pausa entre requisições
        texto_original = buscar_texto_da_norma(tipo, numero, ano_norma, tipo_doc=142)
        time.sleep(1.5)  # espera para evitar bloqueio

        texto_consolidado = buscar_texto_da_norma(tipo, numero, ano_norma, tipo_doc=572)
        time.sleep(1.5)  # espera para evitar bloqueio

        resultados.append({
            "tipo_sigla": tipo,
            "numero": numero,
            "ano": ano_norma,
            "texto_original": texto_original.strip(),
            "texto_consolidado": texto_consolidado.strip()
        })

        progresso.progress((i + 1) / len(normas))

    df_resultado = pd.DataFrame(resultados)
    st.dataframe(df_resultado.head(10))

    # Baixar CSV
    buffer = BytesIO()
    df_resultado.to_csv(buffer, index=False, encoding="utf-8-sig")
    st.download_button("⬇️ Baixar CSV com os textos", data=buffer.getvalue(),
                       file_name=f"normas_{ano}.csv", mime="text/csv")
