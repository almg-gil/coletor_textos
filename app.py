def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Tentativa 1: span padrão usado em LEIs
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Tentativa 2: captura geral do conteúdo dentro da tag <main>
        main = soup.find("main")
        if main:
            texto = main.get_text(separator="\n", strip=True)
            if len(texto) > 100:
                return texto

        return "❌ Texto não encontrado em estruturas conhecidas"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"
