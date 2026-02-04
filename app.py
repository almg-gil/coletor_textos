def extrair_texto_html(url):
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Primeira tentativa: span usado por leis/decretos
        span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
        if span:
            texto = span.get_text(separator="\n", strip=True)
            if len(texto) > 50:
                return texto

        # Segunda tentativa: conteúdo textual dentro do <main>, ignorando menus
        main = soup.find("main")
        if main:
            # Ignora elementos de navegação ou botões
            for tag in main.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()

            texto_bruto = main.get_text(separator="\n", strip=True)

            # Limita a parte a partir de "DELIBERA:" ou "Art. 1º"
            for marcador in ["DELIBERA", "Art. 1º", "Art. 1o", "Art. 1", "RESOLVE"]:
                if marcador in texto_bruto:
                    texto_util = texto_bruto.split(marcador, 1)[-1]
                    return marcador + "\n" + texto_util.strip()

            # Se nada encontrado, retorna tudo
            if len(texto_bruto) > 100:
                return texto_bruto.strip()

        return "❌ Texto não encontrado no HTML"
    except Exception as e:
        return f"❌ Erro ao acessar: {str(e)}"
