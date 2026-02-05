import os
import re
import io
import json
import time
import hashlib
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple, List

import requests
from bs4 import BeautifulSoup

from whoosh import index
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from whoosh.analysis import RegexTokenizer, LowercaseFilter, CharsetFilter
from whoosh.support.charset import accent_map


# =================================================
# CONFIG
# =================================================
TYPES_DEFAULT = [
    "ADT","CON","DCS","DCJ","DEC","DNE","DSN","DEL","DLB","DCE","EMC",
    "LEI","LEA","LCP","LDL","LCO","OSV","PRT","PTC","RAL"
]

START_YEAR_DEFAULT = 1835

DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
ZIP_PATH = "index.zip"

DEFAULT_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))
PAUSE = float(os.getenv("PAUSE", "0.25"))
MAX_REQ = int(os.getenv("MAX_REQ", "60000"))

YEAR_FROM = int(os.getenv("YEAR_FROM", str(START_YEAR_DEFAULT)))
YEAR_TO = int(os.getenv("YEAR_TO", str(datetime.utcnow().year)))

TYPES_ENV = os.getenv("TYPES", "").strip()
if TYPES_ENV:
    TYPES = [t.strip().upper() for t in TYPES_ENV.split(",") if t.strip()]
else:
    TYPES = TYPES_DEFAULT

# versões
VERSIONS_ENV = os.getenv("VERSIONS", "Original,Consolidado").strip()
VERSIONS = [v.strip() for v in VERSIONS_ENV.split(",") if v.strip()]

# Robustez para “buracos”:
MISS_STREAK_STOP = int(os.getenv("MISS_STREAK_STOP", "30"))

# Índice cumulativo: baixa um index.zip existente e continua em cima dele
INDEX_URL = os.getenv("INDEX_URL", "").strip()


# =================================================
# UTIL
# =================================================
@dataclass
class Budget:
    max: int
    used: int = 0

    def ok(self) -> bool:
        return self.used < self.max

    def spend(self, n: int = 1) -> None:
        self.used += n


def log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)


def mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def limpar_texto(texto: str) -> str:
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def base_url(tipo: str, numero: int, ano: int) -> str:
    return f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"


def gerar_links(tipo: str, numero: int, ano: int) -> Dict[str, str]:
    b = base_url(tipo, numero, ano)
    return {"Original": b + "/", "Consolidado": b + "/?cons=1"}


def make_doc_id(tipo: str, numero: int, ano: int, versao: str) -> str:
    vs = "orig" if versao.lower().startswith("orig") else "cons"
    return f"{tipo.upper()}_{numero}_{ano}_{vs}"


def safe_extract(zip_bytes: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for m in z.infolist():
            p = m.filename.replace("\\", "/")
            if p.startswith("/") or ".." in p.split("/"):
                continue
            z.extract(m, ".")


def maybe_download_existing_index() -> None:
    if not INDEX_URL:
        return
    log(f"Baixando índice existente (cumulativo): {INDEX_URL}")
    r = requests.get(INDEX_URL, timeout=60, headers={"User-Agent": "index-builder"})
    r.raise_for_status()
    safe_extract(r.content)
    log("Índice existente extraído.")


# =================================================
# EXTRAÇÃO HTML (robusta p/ LEI/DEC/LCP)
# =================================================
def extrair_texto_main_anchor(main, preferir_versao: str) -> str:
    """
    Tenta pegar somente o trecho do <main> a partir do cabeçalho
    "Texto Original" / "Texto Consolidado".
    """
    preferir = (preferir_versao or "").strip().lower()
    alvo = None

    for h in main.find_all(["h2", "h3"]):
        t = h.get_text(" ", strip=True).lower()
        if preferir.startswith("orig") and "texto original" in t:
            alvo = h
            break
        if preferir.startswith("cons") and ("texto consolid" in t or "texto consolidado" in t):
            alvo = h
            break

    if not alvo:
        return ""

    chunks: List[str] = []
    for el in alvo.find_all_next():
        # Evita puxar scripts/estilos caso escapem
        if getattr(el, "name", "") in ("script", "style"):
            continue

        # Alguns portais têm blocos de rodapé/links que podemos cortar
        txt = el.get_text(" ", strip=True) if hasattr(el, "get_text") else ""
        if not txt:
            continue

        # Corta se achar sinais de fim
        low = txt.lower()
        if "portal da almg" in low and len(txt) < 60:
            break

        chunks.append(txt)

    texto = limpar_texto("\n".join(chunks))
    return texto


def extrair_texto_html_from_html(html: str, preferir_versao: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # 1) Tentativas de contêiner clássico (quando existir)
    span = soup.select_one("span.textNorma")
    if span:
        texto = limpar_texto(span.get_text(separator="\n", strip=True))
        if len(texto) > 80:
            return texto

    cand = soup.select_one(".textNorma, #textoNorma, #conteudoNorma")
    if cand:
        texto = limpar_texto(cand.get_text(separator="\n", strip=True))
        if len(texto) > 80:
            return texto

    # 2) Fallback: <main> (funciona para a LEI 25684/2026)
    main = soup.find("main")
    if not main:
        return ""

    # limpa lixo de navegação
    for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
        tag.decompose()

    # tenta ancorar em "Texto Original/Consolidado"
    anchored = extrair_texto_main_anchor(main, preferir_versao=preferir_versao)
    if len(anchored) > 120:
        return anchored

    # fallback total do main (sem cortar ementa!)
    texto = limpar_texto(main.get_text(separator="\n", strip=True))
    return texto if len(texto) > 120 else ""


# =================================================
# HTTP
# =================================================
def fetch(url: str, budget: Budget, headers: Optional[Dict[str, str]] = None) -> Tuple[int, str, Dict[str, str]]:
    if not budget.ok():
        return 0, "", {}

    h = {"User-Agent": "Mozilla/5.0"}
    if headers:
        h.update(headers)

    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT, headers=h)
        budget.spend(1)

        resp_h = {}
        if "ETag" in r.headers:
            resp_h["ETag"] = r.headers.get("ETag")
        if "Last-Modified" in r.headers:
            resp_h["Last-Modified"] = r.headers.get("Last-Modified")

        return r.status_code, (r.text if r.status_code == 200 else ""), resp_h
    except Exception:
        budget.spend(1)
        return 0, "", {}


def pagina_tem_texto(tipo: str, numero: int, ano: int, budget: Budget) -> bool:
    url = gerar_links(tipo, numero, ano)["Original"]
    code, html, _ = fetch(url, budget)
    if code != 200 or not html:
        return False
    texto = extrair_texto_html_from_html(html, preferir_versao="Original")
    return len(texto) > 80


# =================================================
# WHOOSH (com fold de acentos)
# =================================================
ANALYZER = RegexTokenizer() | LowercaseFilter() | CharsetFilter(accent_map)

def get_schema() -> Schema:
    return Schema(
        doc_id=ID(stored=True, unique=True),

        # tipo como ID exato (evita tokenização estranha)
        tipo_sigla=ID(stored=True),

        numero=NUMERIC(stored=True, numtype=int),
        ano=NUMERIC(stored=True, numtype=int),
        versao=ID(stored=True),
        url=ID(stored=True),

        # texto com analyzer sem acentos e lower
        texto=TEXT(stored=False, analyzer=ANALYZER),

        coletado_em=ID(stored=True),
        etag=ID(stored=True),
        last_modified=ID(stored=True),
        content_hash=ID(stored=True),
    )


def open_or_create_index():
    mkdirp(INDEX_DIR)
    if index.exists_in(INDEX_DIR):
        return index.open_dir(INDEX_DIR)
    return index.create_in(INDEX_DIR, get_schema())


def load_state():
    mkdirp(DATA_DIR)
    if not os.path.exists(STATE_PATH):
        return {"meta": {"created_at": now_iso()}, "years": {}}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"meta": {"created_at": now_iso()}, "years": {}}


def save_state(state):
    mkdirp(DATA_DIR)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def doc_meta(ix, doc_id: str) -> Optional[dict]:
    with ix.searcher() as s:
        d = s.document(doc_id=doc_id)
        return dict(d) if d else None


def fetch_doc(ix, tipo: str, numero: int, ano: int, versao: str, budget: Budget) -> Optional[dict]:
    links = gerar_links(tipo, numero, ano)
    url = links[versao]
    doc_id = make_doc_id(tipo, numero, ano, versao)

    prev = doc_meta(ix, doc_id)
    cond = {}
    if prev:
        if prev.get("etag"):
            cond["If-None-Match"] = prev["etag"]
        if prev.get("last_modified"):
            cond["If-Modified-Since"] = prev["last_modified"]

    code, html, rh = fetch(url, budget, headers=cond)

    if code == 304:
        return None
    if code != 200 or not html:
        return None

    texto = extrair_texto_html_from_html(html, preferir_versao=versao)
    if len(texto) <= 80:
        return None

    h = sha256_str(texto)
    if prev and prev.get("content_hash") == h:
        return None

    return {
        "doc_id": doc_id,
        "tipo_sigla": tipo.upper(),
        "numero": int(numero),
        "ano": int(ano),
        "versao": versao,
        "url": url,
        "texto": texto,
        "coletado_em": now_iso(),
        "etag": rh.get("ETag", "") if rh else "",
        "last_modified": rh.get("Last-Modified", "") if rh else "",
        "content_hash": h,
    }


def zip_data_dir() -> None:
    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(DATA_DIR):
            for fn in files:
                p = os.path.join(root, fn)
                z.write(p, arcname=p)


def main():
    budget = Budget(MAX_REQ)

    # cumulativo
    maybe_download_existing_index()

    ix = open_or_create_index()
    state = load_state()

    writer = ix.writer(limitmb=512, procs=1, multisegment=True)

    for ano in range(YEAR_FROM, YEAR_TO + 1):
        y = state.setdefault("years", {}).setdefault(str(ano), {}).setdefault("types", {})

        for tipo in TYPES:
            if not budget.ok():
                log("Budget esgotado.")
                break

            log(f"Iniciando {tipo}/{ano}")
            ty = y.setdefault(tipo, {"last_num_scanned": 0, "last_checked_at": None})

            # varredura robusta (buracos)
            n = int(ty.get("last_num_scanned", 0)) + 1
            if n < 1:
                n = 1

            miss = 0
            updated = 0

            while budget.ok() and miss < MISS_STREAK_STOP:
                exists = pagina_tem_texto(tipo, n, ano, budget)

                if exists:
                    miss = 0
                    for versao in VERSIONS:
                        if not budget.ok():
                            break
                        d = fetch_doc(ix, tipo, n, ano, versao, budget)
                        if d:
                            writer.update_document(**d)
                            updated += 1
                    ty["last_num_scanned"] = n
                else:
                    miss += 1

                if n % 100 == 0:
                    log(f"{tipo}/{ano}: n={n} miss={miss}/{MISS_STREAK_STOP} upd_docs={updated} req={budget.used}")

                n += 1
                time.sleep(PAUSE)

            ty["last_checked_at"] = now_iso()
            log(f"Finalizado {tipo}/{ano}: last_scanned={ty.get('last_num_scanned')} upd_docs={updated} req={budget.used}")

    writer.commit()
    save_state(state)
    zip_data_dir()

    log(f"OK. Requests usadas={budget.used}. Gerado {ZIP_PATH}")


if __name__ == "__main__":
    main()
