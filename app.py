import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="🔎 Buscar Normas por Ano - ALMG", layout="wide")
st.title("📘 Coletor de Normas da ALMG por Ano")
st.markdown("Este app busca automaticamente todas as normas de um ano na ALMG, com links para texto original e consolidado.")

# -------------------------------------------------
# Função para coletar normas da ALMG por ano
# -------------------------------------------------
def coletar_lista_normas_almg(ano):
    url = f"https://www.almg.gov.br/atividade-parlamentar/leis/legislacao-mineira/?pesquisou=true&aba=pesquisa&q=&grupo=&num=&ano={ano}&dataInicio=&dataFim=&sit=&ordem=2"
    r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    linhas = soup.select("table tbody tr")
    resultados = []

    for tr in linhas:
        tds = tr.find_all("td")
        if len(tds) >= 5:
            tipo_sigla = tds[0].get_text(strip=True)
            numero = tds[1].get_text(strip=True)
            ano_norma = tds[2].get_text(strip=True)

            # Os links de texto aparecem nas colunas de link
            texto_original = ""
            texto_atualizado = ""

            link_original = tds[3].find("a")
            if link_original:
                texto_original = "https://www.almg.gov.br" + link_original["href"]

            link_atualizado = tds[4].find("a")
            if link_atualizado:
                texto_atualizado = "https://www.almg.gov.br" + link_atualizado["href"]

            resultados.append({
                "tipo_sigla": tipo_sigla,
                "numero": numero,
                "ano": ano_norma,
                "url_original": texto_original,
                "url_consolidado": texto_atualizado
            })

    return pd.DataFrame(resultados)

# -------------------------------------------------
# Interface do usuário
# -------------------------------------------------

ano = st.number_input("📅 Digite o ano desejado", min_value=2000, max_value=2026, value=2026, step=1)

if st.button("🚀 Buscar normas"):
    with st.spinner(f"Buscando normas publicadas em {ano}..."):
        try:
            df_normas = coletar_lista_normas_almg(ano)
            if df_normas.empty:
                st.warning("⚠️ Nenhuma norma encontrada para o ano selecionado.")
            else:
                st.success(f"✅ {len(df_normas)} normas encontradas para {ano}")
                st.dataframe(df_normas)

                # Download
                buffer = BytesIO()
                df_normas.to_csv(buffer, index=False, encoding="utf-8-sig")
                st.download_button("⬇️ Baixar CSV com URLs", data=buffer.getvalue(),
                                   file_name=f"normas_{ano}.csv", mime="text/csv")

        except Exception as e:
            st.error(f"Erro ao buscar normas: {e}")
