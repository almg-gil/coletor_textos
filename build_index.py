# build_index.py
import os, json, time, re, hashlib, zipfile
from datetime import datetime
from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup

from whoosh import index
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NUMERIC

START_YEAR = 1835
TYPES = ["ADT","CON","DCS","DCJ","DEC","DNE","DSN","DEL","DLB","DCE","EMC","LEI","LEA","LCP","LDL","LCO","OSV","PRT","PTC","RAL"]

DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
ZIP_PATH = "index.zip"

DEFAULT_TIMEOUT = 20
PAUSE = float(os.getenv("PAUSE", "0.2"))
MAX_REQ = int(os.getenv("MAX_REQ", "200000"))  # no bootstrap pode ser alto
RECHECK_WINDOW = int(os.getenv("RECHECK_WINDOW", "0"))  # no bootstrap geralmente 0

YEAR_FROM = int(os.getenv("YEAR_FROM", str(START_YEAR)))
YEAR_TO = int(os.getenv("YEAR_TO", str(datetime.utcnow().year)))

@dataclass
class Budget:
    max: int
    used: int = 0
    def ok(self): return self.used < self.max
    def spend(self, n=1): self.used += n

def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def mkdirp(p): os.makedirs(p, exist_ok=True)

def limpar_texto(t):
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def base_url(tipo, numero, ano):
    return f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}"

def gerar_links(tipo, numero, ano):
    b = base_url(tipo, numero, ano)
    return {"Original": b + "/", "Consolidado": b + "/?cons=1"}

def make_doc_id(tipo, numero, ano, versao):
    vs = "orig" if versao.lower().startswith("orig") else "cons"
    return f"{tipo.upper()}_{numero}_{ano}_{vs}"

def extrair_texto_html_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
    if span:
        texto = limpar_texto(span.get_text(separator="\n", strip=True))
        if len(texto) > 50:
            return texto

    main = soup.find("main")
    if main:
        for tag in main.find_all(["nav","header","footer","script","style","button","aside"]):
            tag.decompose()
        for div in main.find_all("div"):
            if "compartilhar" in div.get_text(strip=True).lower():
                div.decompose()

        texto = limpar_texto(main.get_text(separator="\n", strip=True))
        for marcador in ["DELIBERA", "RESOLVE", "Art. 1º", "Art. 1o", "Art. 1"]:
            if marcador in texto:
                return limpar_texto(marcador + "\n" + texto.split(marcador, 1)[-1])
        if len(texto) > 100:
            return texto
    return ""

def fetch(url, budget: Budget, headers=None):
    if not budget.ok(): return 0, "", {}
    h = {"User-Agent": "Mozilla/5.0"}
    if headers: h.update(headers)
    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT, headers=h)
        budget.spend(1)
        resp_h = {k: r.headers.get(k, "") for k in ["ETag","Last-Modified"] if k in r.headers}
        return r.status_code, (r.text if r.status_code == 200 else ""), resp_h
    except Exception:
        budget.spend(1)
        return 0, "", {}

def norma_existe(tipo, numero, ano, budget: Budget) -> bool:
    url = gerar_links(tipo, numero, ano)["Original"]
    code, html, _ = fetch(url, budget)
    if code != 200 or not html: return False
    return len(extrair_texto_html_from_html(html)) > 50

def achar_ultimo_numero(tipo, ano, budget: Budget, max_limite=300000) -> int:
    if not norma_existe(tipo, 1, ano, budget): return 0
    lo, hi = 1, 2
    while hi <= max_limite and budget.ok() and norma_existe(tipo, hi, ano, budget):
        lo, hi = hi, hi * 2
    left, right = lo, min(hi, max_limite)
    while left + 1 < right and budget.ok():
        mid = (left + right)//2
        if norma_existe(tipo, mid, ano, budget):
            left = mid
        else:
            right = mid
    return left

def schema():
    return Schema(
        doc_id=ID(stored=True, unique=True),
        tipo_sigla=KEYWORD(stored=True, commas=False, lowercase=False),
        numero=NUMERIC(stored=True, numtype=int),
        ano=NUMERIC(stored=True, numtype=int),
        versao=KEYWORD(stored=True, commas=False, lowercase=False),
        url=ID(stored=True),
        texto=TEXT(stored=False),
        coletado_em=ID(stored=True),
        etag=ID(stored=True),
        last_modified=ID(stored=True),
        content_hash=ID(stored=True),
    )

def open_or_create_index():
    mkdirp(INDEX_DIR)
    if index.exists_in(INDEX_DIR):
        return index.open_dir(INDEX_DIR)
    return index.create_in(INDEX_DIR, schema())

def load_state():
    mkdirp(DATA_DIR)
    if not os.path.exists(STATE_PATH):
        return {"meta":{"created_at": now_iso()}, "years":{}}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    mkdirp(DATA_DIR)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def doc_meta(ix, doc_id):
    with ix.searcher() as s:
        d = s.document(doc_id=doc_id)
        return dict(d) if d else None

def fetch_doc(ix, tipo, numero, ano, versao, budget: Budget):
    url = gerar_links(tipo, numero, ano)[versao]
    doc_id = make_doc_id(tipo, numero, ano, versao)
    prev = doc_meta(ix, doc_id)
    cond = {}
    if prev:
        if prev.get("etag"): cond["If-None-Match"] = prev["etag"]
        if prev.get("last_modified"): cond["If-Modified-Since"] = prev["last_modified"]
    code, html, rh = fetch(url, budget, headers=cond)
    if code == 304: return None
    if code != 200 or not html: return None
    texto = extrair_texto_html_from_html(html)
    if len(texto) <= 50: return None
    h = sha256_str(texto)
    if prev and prev.get("content_hash") == h: return None
    return dict(
        doc_id=doc_id, tipo_sigla=tipo.upper(), numero=int(numero), ano=int(ano), versao=versao,
        url=url, texto=texto, coletado_em=now_iso(),
        etag=rh.get("ETag",""), last_modified=rh.get("Last-Modified",""), content_hash=h
    )

def zip_index():
    # compacta data/index + data/state.json
    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(DATA_DIR):
            for fn in files:
                p = os.path.join(root, fn)
                z.write(p, arcname=p)

def main():
    budget = Budget(MAX_REQ)
    ix = open_or_create_index()
    state = load_state()

    writer = ix.writer(limitmb=512, procs=1, multisegment=True)

    for ano in range(YEAR_FROM, YEAR_TO + 1):
        y = state.setdefault("years", {}).setdefault(str(ano), {}).setdefault("types", {})
        for tipo in TYPES:
            if not budget.ok():
                print("Budget exhausted.")
                break

            ty = y.setdefault(tipo, {"last_num_known": 0, "last_probe_at": None})
            last_known = int(ty.get("last_num_known", 0))

            # probe do último número (poucas req)
            ultimo = achar_ultimo_numero(tipo, ano, budget)
            ty["last_probe_at"] = now_iso()
            ty["last_num_known"] = max(last_known, ultimo)

            if ty["last_num_known"] <= 0:
                continue

            # bootstrap: indexa 1..ultimo (você pode paralelizar fora, mas aqui é simples)
            for numero in range(1, ty["last_num_known"] + 1):
                if not budget.ok(): break
                for versao in ("Original", "Consolidado"):
                    d = fetch_doc(ix, tipo, numero, ano, versao, budget)
                    if d:
                        writer.update_document(**d)
                time.sleep(PAUSE)

            print(f"{tipo}/{ano}: ultimo={ty['last_num_known']} | req={budget.used}")

    writer.commit()
    save_state(state)
    zip_index()
    print(f"Done. Requests used={budget.used}. ZIP={ZIP_PATH}")

if __name__ == "__main__":
    main()
