import requests
from bs4 import BeautifulSoup
import pandas as pd

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
                texto_original = link_original["href"]

            link_atualizado = tds[4].find("a")
            if link_atualizado:
                texto_atualizado = link_atualizado["href"]

            resultados.append({
                "tipo_sigla": tipo_sigla,
                "numero": numero,
                "ano": ano_norma,
                "url_original": texto_original,
                "url_atualizado": texto_atualizado
            })

    return pd.DataFrame(resultados)
