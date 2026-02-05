import os
import re
import json
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import requests
import streamlit as st
from bs4 import BeautifulSoup

from whoosh import index
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NUMERIC
from whoosh.qparser import MultifieldParser, OrGroup
from whoosh.query import And, Term, Every


# =================================================
# CONFIG
# =================================================
st.set_page_config(page_title="Motor de Busca ALMG (Auto) - Whoosh", layout="wide")
APP_TITLE = "🔄📚 Motor de Busca ALMG — Auto-descoberta + Booleano + Campos (Whoosh)"

DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
STATE_PATH = os.path.join(DATA_DIR, "state.json")

START_YEAR = 1835
TYPES = [
    "ADT","CON","DCS","DCJ","DEC","DNE","DSN","DEL","DLB","DCE","EMC",
    "LEI","LEA","LCP","LDL","LCO","OSV","PRT","PTC","RAL"
]

DEFAULT_TIMEOUT = 20
PAUSE_ENTRE_REQ = 0.25  # reduz risco de rate-limit / sobrecarga

# limites para não “martelar” o portal
DEFAULT_MAX_REQUESTS_PER_RUN = 600     # orçamento por execução
DEFAULT_RECHECK_WINDOW = 600           # revalidar últimos N do ano (mudanças)
DEFAULT_MAX_YEARS_PER_RUN = 3          # não tentar varrer séculos de uma vez

# =================================================
# UI
# =================================================
st.title(APP_TITLE)
st.caption(
    "Sem CSV: o app descobre normas pelo padrão de URL e mantém índice incremental, "
    "com orçamento de requisições para não sobrecarregar o portal."
)

with st.expander("⚠️ Como este modo automático evita muitas requisições", expanded=False):
    st.markdown(
        """
**Otimizações**:
- Mantém `state.json` com o **último número conhecido** por `(tipo, ano)`.
- Para achar o último número: faz poucas requisições (exponencial + binária).
- Para alterações: revisa só uma **janela recente** (ex.: últimos 600 do ano).
- Usa `ETag/Last-Modified` quando disponível para GET condicional (retorna 304 quando não mudou).
"""
    )

# =================================================
# UTIL
# =================================================
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
    versao_slug = "orig" if versao.lower().startswith("orig") else "cons"
    return f"{tipo.upper()}_{numero}_{ano}_{versao_slug}"

# =================================================
# STATE (persistência leve)
# =================================================
def load_state() -> Dict:
    mkdirp(DATA_DIR)
    if not os.path.exists(STATE_PATH):
        return {"meta": {"created_at": now_iso()}, "years": {}}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"meta": {"created_at": now_iso()}, "years": {}}

def save_state(state: Dict) -> None:
    mkdirp(DATA_DIR)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)

def get_year_state(state: Dict, ano: int) -> Dict:
    y = state.setdefault("years", {}).setdefault(str(ano), {})
    y.setdefault("types", {})
    return y

def get_type_year_state(state: Dict, ano: int, tipo: str) -> Dict:
    y = get_year_state(state, ano)
    t = y["types"].setdefault(tipo, {})
    # campos que usamos
    t.setdefault("last_num_known", 0)          # último número conhecido existente
    t.setdefault("last_checked_at", None)      # última vez que tentamos atualizar
    t.setdefault("last_probe_at", None)        # última vez que recalculamos last_num via binária
    return t

# =================================================
# REQUEST BUDGET
# =================================================
@dataclass
class Budget:
    max_requests: int
    used: int = 0

    def ok(self) -> bool:
        return self.used < self.max_requests

    def spend(self, n: int = 1) -> None:
        self.used += n

# =================================================
# HTML EXTRACTION (sua lógica, com pequenos reforços)
# =================================================
def extrair_texto_html_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    span = soup.find("span", class_="js_interpretarLinks textNorma js_interpretarLinksDONE")
    if span:
        texto = limpar_texto(span.get_text(separator="\n", strip=True))
        if len(texto) > 50:
            return texto

    main = soup.find("main")
    if main:
        for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
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

def fetch_html(url: str, budget: Budget, headers: Optional[Dict[str, str]] = None) -> Tuple[int, str, Dict[str, str]]:
    """
    GET com orçamento. Retorna (status_code, html, response_headers_subset).
    """
    if not budget.ok():
        return 0, "", {}

    req_headers = {"User-Agent": "Mozilla/5.0"}
    if headers:
        req_headers.update(headers)

    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT, headers=req_headers)
        budget.spend(1)
        # pega só o que interessa pro cache condicional
        h = {}
        if "ETag" in r.headers:
            h["ETag"] = r.headers.get("ETag")
        if "Last-Modified" in r.headers:
            h["Last-Modified"] = r.headers.get("Last-Modified")
        return r.status_code, r.text if r.status_code == 200 else "", h
    except Exception:
        budget.spend(1)
        return 0, "", {}

def norma_existe(tipo: str, numero: int, ano: int, budget: Budget) -> bool:
    """
    Heurística barata: GET do Original e tenta extrair texto.
    """
    url = gerar_links(tipo, numero, ano)["Original"]
    code, html, _h = fetch_html(url, budget)
    if code != 200 or not html:
        return False
    texto = extrair_texto_html_from_html(html)
    return len(texto) > 50

# =================================================
# Descobrir último número com poucas requisições
# =================================================
def achar_ultimo_numero(tipo: str, ano: int, budget: Budget, max_limite: int = 300000) -> int:
    """
    Crescimento exponencial + busca binária.
    """
    if not budget.ok():
        return 0

    if not norma_existe(tipo, 1, ano, budget):
        return 0

    lo = 1
    hi = 2

    # crescimento exponencial
    while hi <= max_limite and budget.ok() and norma_existe(tipo, hi, ano, budget):
        lo = hi
        hi *= 2

    # busca binária
    left, right = lo, min(hi, max_limite)
    while left + 1 < right and budget.ok():
        mid = (left + right) // 2
        if norma_existe(tipo, mid, ano, budget):
            left = mid
        else:
            right = mid

    return left

# =================================================
# WHOOSH
# =================================================
def get_schema() -> Schema:
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

@st.cache_resource(show_spinner=False)
def open_or_create_index(index_dir: str):
    mkdirp(index_dir)
    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    return index.create_in(index_dir, schema=get_schema())

def doc_meta(ix, doc_id: str) -> Optional[Dict]:
    """
    Pega metadados armazenados do doc, ou None.
    """
    with ix.searcher() as s:
        d = s.document(doc_id=doc_id)
        if not d:
            return None
        return dict(d)

def upsert_docs(ix, docs: List[Dict]) -> Tuple[int, int]:
    ok, fail = 0, 0
    writer = ix.writer(limitmb=256, procs=1, multisegment=True)
    try:
        for d in docs:
            try:
                writer.update_document(**d)
                ok += 1
            except Exception:
                fail += 1
        writer.commit()
    except Exception:
        writer.cancel()
        raise
    return ok, fail

# =================================================
# Atualização incremental (novos + recheck janela)
# =================================================
def fetch_and_build_doc(tipo: str, numero: int, ano: int, versao: str, budget: Budget, ix) -> Optional[Dict]:
    """
    Faz GET condicional (se já temos ETag/Last-Modified).
    Retorna dict pronto pro Whoosh, ou None (sem mudanças/erro).
    """
    links = gerar_links(tipo, numero, ano)
    url = links[versao]
    doc_id = make_doc_id(tipo, numero, ano, versao)

    prev = doc_meta(ix, doc_id)
    cond_headers = {}
    if prev:
        # conditional GET, se suportado
        if prev.get("etag"):
            cond_headers["If-None-Match"] = prev["etag"]
        if prev.get("last_modified"):
            cond_headers["If-Modified-Since"] = prev["last_modified"]

    code, html, resp_h = fetch_html(url, budget, headers=cond_headers)

    # 304 = não mudou
    if code == 304:
        return None

    if code != 200 or not html:
        return None

    texto = extrair_texto_html_from_html(html)
    if len(texto) <= 50:
        return None

    content_hash = sha256_str(texto)

    # se hash igual ao que já está guardado, não precisa regravar
    if prev and prev.get("content_hash") == content_hash:
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
        "etag": resp_h.get("ETag", "") if resp_h else "",
        "last_modified": resp_h.get("Last-Modified", "") if resp_h else "",
        "content_hash": content_hash,
    }

def update_type_year(ix, state: Dict, tipo: str, ano: int, budget: Budget, recheck_window: int) -> Dict[str, int]:
    """
    Atualiza um (tipo, ano):
    - garante last_num_known (com probes ocasionais)
    - indexa novos números
    - revalida janela recente (alterações)
    Retorna contadores.
    """
    counters = {"new": 0, "updated": 0, "skipped": 0, "probed": 0}

    ty = get_type_year_state(state, ano, tipo)
    last_known = int(ty.get("last_num_known", 0))

    # Recalcular last_known raramente, ou se ainda não sabemos nada:
    # - se last_known == 0 -> precisamos descobrir
    # - se passou muito tempo desde last_probe -> revalidar (pega novos)
    need_probe = (last_known == 0)
    last_probe_at = ty.get("last_probe_at")
    if last_probe_at:
        try:
            dt = datetime.strptime(last_probe_at, "%Y-%m-%dT%H:%M:%SZ")
            if datetime.utcnow() - dt > timedelta(days=7):  # 1x por semana
                need_probe = True
        except Exception:
            need_probe = True
    else:
        # se temos last_known mas nunca “probeou”, probe 1x
        if last_known > 0:
            need_probe = True

    if need_probe and budget.ok():
        ultimo = achar_ultimo_numero(tipo, ano, budget)
        counters["probed"] += 1
        ty["last_probe_at"] = now_iso()
        if ultimo > last_known:
            last_known = ultimo
        if last_known < 0:
            last_known = 0
        ty["last_num_known"] = int(last_known)

    # 1) Indexar novos (se descobrimos que existem)
    # Se last_known == 0, nada a fazer.
    docs_to_write: List[Dict] = []
    if last_known > 0 and budget.ok():
        # indexar “novos” a partir do que já está no índice:
        # em vez de confiar só no state, vamos verificar onde o índice parou com doc_meta (Original)
        # (é barato porque é local)
        # Pegamos o maior número que já existe no índice para este (tipo, ano) e versão Original,
        # mas sem varrer tudo: começamos do last_known e descemos até achar um existente (até 50 passos).
        # (opcional – evita inconsistência se state perder algo)
        start_new = None
        probe = last_known
        steps = 0
        while probe >= 1 and steps < 50:
            did = make_doc_id(tipo, probe, ano, "Original")
            if doc_meta(ix, did):
                start_new = probe + 1
                break
            probe -= 1
            steps += 1
        if start_new is None:
            start_new = 1

        for numero in range(start_new, last_known + 1):
            if not budget.ok():
                break

            for versao in ("Original", "Consolidado"):
                d = fetch_and_build_doc(tipo, numero, ano, versao, budget, ix)
                if d:
                    docs_to_write.append(d)
                    # heurística: se doc era novo, conta como new; se existia e mudou, conta updated
                    prev = doc_meta(ix, d["doc_id"])
                    if prev is None:
                        counters["new"] += 1
                    else:
                        counters["updated"] += 1
                else:
                    counters["skipped"] += 1

            time.sleep(PAUSE_ENTRE_REQ)

    # 2) Revalidar janela recente (alterações): últimos N números do ano
    if last_known > 0 and budget.ok() and recheck_window > 0:
        start = max(1, last_known - recheck_window + 1)
        for numero in range(start, last_known + 1):
            if not budget.ok():
                break
            # revalida só Original (e opcionalmente Consolidado); aqui faço as duas,
            # mas você pode reduzir para só Consolidado se achar melhor.
            for versao in ("Original", "Consolidado"):
                d = fetch_and_build_doc(tipo, numero, ano, versao, budget, ix)
                if d:
                    docs_to_write.append(d)
                    counters["updated"] += 1
            time.sleep(PAUSE_ENTRE_REQ)

    if docs_to_write:
        ok, fail = upsert_docs(ix, docs_to_write)
        # fail não entra nos counters por simplicidade; se quiser, adiciona
        # (ok aqui é nº de docs gravados, não necessariamente novos)
    ty["last_checked_at"] = now_iso()
    return counters

# =================================================
# BUSCA
# =================================================
def search(ix, expr: str, filtros: Dict, limit: int = 20):
    parser = MultifieldParser(["texto"], schema=ix.schema, group=OrGroup)
    q_text = (expr or "").strip()
    base_q = parser.parse(q_text) if q_text else Every()

    filter_terms = []
    if filtros.get("tipo_sigla"):
        filter_terms.append(Term("tipo_sigla", str(filtros["tipo_sigla"]).upper()))
    if filtros.get("versao"):
        filter_terms.append(Term("versao", str(filtros["versao"])))
    if filtros.get("ano") not in (None, "", "Todos"):
        filter_terms.append(Term("ano", int(filtros["ano"])))
    if filtros.get("numero") not in (None, "", "Todos"):
        filter_terms.append(Term("numero", int(filtros["numero"])))

    q = And([base_q] + filter_terms) if filter_terms else base_q

    with ix.searcher() as s:
        results = s.search(q, limit=limit)
        out = []
        for r in results:
            out.append({
                "doc_id": r.get("doc_id"),
                "tipo_sigla": r.get("tipo_sigla"),
                "numero": r.get("numero"),
                "ano": r.get("ano"),
                "versao": r.get("versao"),
                "url": r.get("url"),
                "coletado_em": r.get("coletado_em"),
                "score": float(r.score),
            })
        return out

# =================================================
# INIT
# =================================================
mkdirp(DATA_DIR)
ix = open_or_create_index(INDEX_DIR)
state = load_state()

# =================================================
# SIDEBAR - UPDATE SETTINGS
# =================================================
with st.sidebar:
    st.header("🔄 Atualização automática (Estratégia A)")
    st.caption("Escolha o foco para evitar muitas requisições.")

    year_mode = st.radio("Modo de anos", ["Janela recente", "Ano específico", "Intervalo (limitado)"], index=0)

    current_year = datetime.utcnow().year
    if year_mode == "Janela recente":
        years_back = st.number_input("Quantos anos para trás", min_value=0, max_value=20, value=3, step=1)
        years = list(range(current_year - int(years_back), current_year + 1))
    elif year_mode == "Ano específico":
        y = st.number_input("Ano", min_value=START_YEAR, max_value=current_year, value=current_year, step=1)
        years = [int(y)]
    else:
        y1 = st.number_input("Ano inicial", min_value=START_YEAR, max_value=current_year, value=max(START_YEAR, current_year-3), step=1)
        y2 = st.number_input("Ano final", min_value=START_YEAR, max_value=current_year, value=current_year, step=1)
        y1, y2 = int(min(y1, y2)), int(max(y1, y2))
        # não deixa varrer um intervalo enorme em uma tacada só
        max_span = DEFAULT_MAX_YEARS_PER_RUN
        years = list(range(y2 - max_span + 1, y2 + 1)) if (y2 - y1 + 1) > max_span else list(range(y1, y2 + 1))
        if (y2 - y1 + 1) > max_span:
            st.warning(f"Intervalo grande. Limitando aos últimos {max_span} anos do intervalo para poupar requisições.")

    st.divider()
    tipos_sel = st.multiselect("Tipos", TYPES, default=["LEI", "DEC", "DCE"])
    recheck_window = st.number_input("Janela de rechecagem (alterações)", min_value=0, max_value=5000, value=DEFAULT_RECHECK_WINDOW, step=50)
    max_req = st.number_input("Orçamento de requisições por execução", min_value=50, max_value=5000, value=DEFAULT_MAX_REQUESTS_PER_RUN, step=50)
    auto_run = st.checkbox("Rodar atualização ao abrir (cuidado)", value=False)
    run_now = st.button("▶️ Atualizar agora")

# =================================================
# UPDATE RUNNER
# =================================================
def run_update(ix, state: Dict, tipos: List[str], years: List[int], max_requests: int, recheck_window: int):
    budget = Budget(max_requests=max_requests)
    totals = {"new": 0, "updated": 0, "skipped": 0, "probed": 0}

    st.info(f"Atualizando {len(tipos)} tipo(s) em {len(years)} ano(s) | orçamento={max_requests} req")
    barra = st.progress(0)
    steps = max(1, len(tipos) * len(years))
    done = 0

    for ano in years:
        for tipo in tipos:
            if not budget.ok():
                st.warning("Orçamento de requisições esgotado nesta execução.")
                save_state(state)
                return totals, budget.used

            c = update_type_year(ix, state, tipo, int(ano), budget, int(recheck_window))
            for k in totals:
                totals[k] += c.get(k, 0)

            done += 1
            barra.progress(done / steps)

    save_state(state)
    return totals, budget.used

# auto-run 1x por sessão
if auto_run and "auto_ran" not in st.session_state:
    st.session_state["auto_ran"] = True
    if tipos_sel and years:
        totals, used = run_update(ix, state, tipos_sel, years, int(max_req), int(recheck_window))
        st.success(f"Auto-update OK | req usadas={used} | novos={totals['new']} | atualizados={totals['updated']} | probes={totals['probed']}")

if run_now:
    if not tipos_sel:
        st.error("Selecione ao menos 1 tipo.")
    else:
        totals, used = run_update(ix, state, tipos_sel, years, int(max_req), int(recheck_window))
        st.success(f"Update OK | req usadas={used} | novos={totals['new']} | atualizados={totals['updated']} | probes={totals['probed']}")

st.divider()

# =================================================
# SEARCH UI
# =================================================
st.subheader("🔎 Buscar no índice (booleana + campos)")

col1, col2 = st.columns([2, 1])
expr = col1.text_input(
    "Busca booleana (AND/OR/NOT, parênteses, aspas)",
    value='("transparência" OR publicidade) AND contrato'
)
limit = col2.number_input("Qtde de resultados", min_value=5, max_value=100, value=20, step=5)

f1, f2, f3, f4 = st.columns(4)
f_tipo = f1.selectbox("tipo_sigla (opcional)", [""] + TYPES, index=0)
f_ano = f2.number_input("ano (opcional)", min_value=START_YEAR, max_value=datetime.utcnow().year, value=datetime.utcnow().year, step=1)
use_year_filter = f2.checkbox("Filtrar por ano", value=False)
f_num = f3.text_input("numero (opcional)", value="")
f_versao = f4.selectbox("versao (opcional)", ["", "Original", "Consolidado"], index=0)

if st.button("Buscar"):
    filtros = {
        "tipo_sigla": f_tipo.strip(),
        "ano": str(int(f_ano)) if use_year_filter else "",
        "numero": f_num.strip(),
        "versao": f_versao.strip(),
    }
    try:
        hits = search(ix, expr, filtros, limit=int(limit))
    except Exception as e:
        st.error(f"Erro na busca (sintaxe/consulta): {e}")
        st.stop()

    st.success(f"Resultados: {len(hits)}")
    for h in hits:
        meta = f"{h.get('tipo_sigla')} {h.get('numero')}/{h.get('ano')} — {h.get('versao')}"
        st.markdown(f"### {meta}")
        st.caption(f"doc_id: {h.get('doc_id')} | score: {h.get('score'):.3f} | coletado_em: {h.get('coletado_em')}")
        if h.get("url"):
            st.markdown(h["url"])
        st.divider()
