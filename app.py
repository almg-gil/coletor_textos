import os
import io
import json
import zipfile
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
import streamlit as st

from whoosh import index
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from whoosh.qparser import MultifieldParser, OrGroup
from whoosh.query import And, Term, Every, Or, NumericRange


TYPES = [
    "ADT","CON","DCS","DCJ","DEC","DNE","DSN","DEL","DLB","DCE","EMC",
    "LEI","LEA","LCP","LDL","LCO","OSV","PRT","PTC","RAL"
]

def _get_secret(path: List[str], default: str = "") -> str:
    cur = st.secrets
    try:
        for p in path:
            cur = cur[p]
        return str(cur)
    except Exception:
        return default

GITHUB_OWNER = os.getenv("GITHUB_OWNER", _get_secret(["github", "owner"], ""))
GITHUB_REPO = os.getenv("GITHUB_REPO", _get_secret(["github", "repo"], ""))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", _get_secret(["github", "token"], ""))  # opcional
RELEASE_TAG = os.getenv("RELEASE_TAG", _get_secret(["github", "tag"], "index-latest"))

ASSET_NAME = os.getenv("INDEX_ASSET_NAME", "index.zip")

DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
META_PATH = os.path.join(DATA_DIR, "release_meta.json")

REQUEST_TIMEOUT = 30


st.set_page_config(page_title="Motor de Busca ALMG (Release Index)", layout="wide")
st.title("📚 Motor de Busca ALMG — Índice via Release (Whoosh)")
st.caption("Baixa index.zip do GitHub Release e permite busca booleana + filtros por campos (com multiselect de tipos).")


def mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def file_exists(path: str) -> bool:
    return os.path.exists(path) and os.path.isfile(path)

def dir_exists(path: str) -> bool:
    return os.path.exists(path) and os.path.isdir(path)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def load_json(path: str, default: dict) -> dict:
    try:
        if file_exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default

def save_json(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def clean_extract_zip(zip_bytes: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for member in z.infolist():
            member_path = member.filename.replace("\\", "/")
            if member_path.startswith("/") or ".." in member_path.split("/"):
                continue
            z.extract(member, path=".")


def github_headers() -> Dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "User-Agent": "streamlit-almg-search"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def fetch_release_info_by_tag(owner: str, repo: str, tag: str) -> dict:
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    r = requests.get(url, headers=github_headers(), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def pick_asset(release_json: dict, asset_name: str) -> Optional[dict]:
    for a in release_json.get("assets", []):
        if a.get("name") == asset_name:
            return a
    return None

def download_asset_bytes(asset: dict) -> bytes:
    url = asset.get("browser_download_url")
    if not url:
        raise RuntimeError("Asset não tem browser_download_url.")
    r = requests.get(url, headers={"User-Agent": "streamlit-almg-search"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.content

def index_is_ready() -> bool:
    return dir_exists(INDEX_DIR) and index.exists_in(INDEX_DIR)

def current_local_release_meta() -> dict:
    return load_json(META_PATH, default={})

def should_download(remote_release: dict, local_meta: dict) -> bool:
    return remote_release.get("id") != local_meta.get("release_id")

def save_local_release_meta(remote_release: dict, zip_hash: str) -> None:
    meta = {
        "release_id": remote_release.get("id"),
        "tag_name": remote_release.get("tag_name"),
        "published_at": remote_release.get("published_at"),
        "downloaded_at": now_iso(),
        "asset_name": ASSET_NAME,
        "zip_sha256": zip_hash,
    }
    mkdirp(DATA_DIR)
    save_json(META_PATH, meta)


def get_schema() -> Schema:
    # fallback mínimo
    return Schema(
        doc_id=ID(stored=True, unique=True),
        tipo_sigla=ID(stored=True),
        numero=NUMERIC(stored=True, numtype=int),
        ano=NUMERIC(stored=True, numtype=int),
        versao=ID(stored=True),
        url=ID(stored=True),
        texto=TEXT(stored=False),
        coletado_em=ID(stored=True),
        etag=ID(stored=True),
        last_modified=ID(stored=True),
        content_hash=ID(stored=True),
    )

@st.cache_resource(show_spinner=False)
def open_index_cached(index_dir: str):
    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    mkdirp(index_dir)
    return index.create_in(index_dir, get_schema())

def count_docs(ix) -> int:
    with ix.searcher() as s:
        return s.doc_count()

def search(ix, expr: str, filtros: Dict, limit: int = 20):
    parser = MultifieldParser(["texto"], schema=ix.schema, group=OrGroup)
    q_text = (expr or "").strip()
    base_q = parser.parse(q_text) if q_text else Every()

    filter_terms = []

    tipos_sel = filtros.get("tipos", [])
    if tipos_sel:
        filter_terms.append(Or([Term("tipo_sigla", t.upper()) for t in tipos_sel]))

    versao = filtros.get("versao")
    if versao:
        filter_terms.append(Term("versao", versao))

    ano = filtros.get("ano")
    if ano:
        y = int(ano)
        filter_terms.append(NumericRange("ano", y, y))

    numero = filtros.get("numero")
    if numero:
        n = int(numero)
        filter_terms.append(NumericRange("numero", n, n))

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


def ensure_index_from_release(force: bool = False) -> Tuple[bool, str]:
    if not GITHUB_OWNER or not GITHUB_REPO:
        return False, "Configure owner/repo nos Secrets do Streamlit ([github] owner=... repo=...)."

    mkdirp(DATA_DIR)
    local_meta = current_local_release_meta()

    try:
        release = fetch_release_info_by_tag(GITHUB_OWNER, GITHUB_REPO, RELEASE_TAG)
    except Exception as e:
        if index_is_ready():
            return True, f"Não consegui consultar Release (tag {RELEASE_TAG}), usando índice local. Erro: {e}"
        return False, f"Falha ao consultar Release (tag {RELEASE_TAG}): {e}"

    asset = pick_asset(release, ASSET_NAME)
    if not asset:
        return False, f"Release (tag {RELEASE_TAG}) encontrado, mas não achei asset '{ASSET_NAME}'."

    need = force or (not index_is_ready()) or should_download(release, local_meta)
    if not need:
        return True, "Índice local já está atualizado."

    try:
        zip_bytes = download_asset_bytes(asset)
        zhash = sha256_bytes(zip_bytes)
        clean_extract_zip(zip_bytes)

        if not index_is_ready():
            return False, "Baixei e extraí, mas não encontrei um índice Whoosh válido em data/index."

        save_local_release_meta(release, zhash)
        return True, "Índice atualizado a partir do Release."
    except Exception as e:
        if index_is_ready():
            return True, f"Falha ao atualizar, usando índice local existente. Erro: {e}"
        return False, f"Falha ao baixar/extrair index.zip: {e}"


with st.sidebar:
    st.header("⚙️ Fonte do índice (GitHub Release)")
    st.write(f"Repo: **{GITHUB_OWNER}/{GITHUB_REPO}**" if (GITHUB_OWNER and GITHUB_REPO) else "Repo: **(não configurado)**")
    st.write(f"Tag do Release: **{RELEASE_TAG}**")
    st.write(f"Asset esperado: **{ASSET_NAME}**")

    local_meta = current_local_release_meta()
    if local_meta:
        st.write("**Release local:**")
        st.json(local_meta, expanded=False)

    colA, colB = st.columns(2)
    auto_on_start = colA.checkbox("Baixar ao abrir", value=True)
    force_update_btn = colB.button("Atualizar índice agora")

if auto_on_start and "bootstrapped" not in st.session_state:
    st.session_state["bootstrapped"] = True
    with st.spinner("Verificando/baixando índice do Release…"):
        ok, msg = ensure_index_from_release(force=False)
    if ok:
        st.success(msg)
    else:
        st.error(msg)

if force_update_btn:
    with st.spinner("Forçando atualização do índice…"):
        ok, msg = ensure_index_from_release(force=True)
        st.cache_resource.clear()
    if ok:
        st.success(msg)
    else:
        st.error(msg)

ix = open_index_cached(INDEX_DIR)

try:
    st.sidebar.metric("Docs no índice", count_docs(ix))
except Exception:
    st.sidebar.metric("Docs no índice", 0)


st.subheader("🔎 Buscar no índice (booleana + campos)")

col1, col2 = st.columns([2, 1])
expr = col1.text_input(
    "Consulta booleana (AND/OR/NOT, parênteses e aspas)",
    value='("utilidade pública") AND NOT ("servidão") AND NOT ("desapropriação")'
)
limit = col2.number_input("Qtde de resultados", min_value=5, max_value=200, value=30, step=5)

f1, f2, f3, f4 = st.columns(4)
f_tipos = f1.multiselect("tipo_sigla (opcional)", TYPES, default=["LEI"])
f_ano = f2.text_input("ano (opcional)", value="2026")
f_num = f3.text_input("numero (opcional)", value="")
f_versao = f4.selectbox("versao (opcional)", ["", "Original", "Consolidado"], index=0)

st.caption("Dica: deixe filtros vazios para buscar no acervo inteiro. Use tipos para restringir por categoria.")

if st.button("Buscar"):
    filtros = {
        "tipos": [t.strip().upper() for t in f_tipos if t.strip()],
        "ano": f_ano.strip(),
        "numero": f_num.strip(),
        "versao": f_versao.strip(),
    }

    if filtros["ano"] and not filtros["ano"].isdigit():
        st.error("O campo 'ano' deve ser numérico (ou vazio).")
        st.stop()
    if filtros["numero"] and not filtros["numero"].isdigit():
        st.error("O campo 'numero' deve ser numérico (ou vazio).")
        st.stop()

    try:
        hits = search(ix, expr, filtros, limit=int(limit))
    except Exception as e:
        st.error(f"Erro na busca (sintaxe/consulta): {e}")
        st.stop()

    st.success(f"Resultados: {len(hits)}")
    for h in hits:
        meta = f"{h.get('tipo_sigla')} {h.get('numero')}/{h.get('ano')} — {h.get('versao')}"
        st.markdown(f"### {meta}")
        st.caption(f"score: {h.get('score'):.3f} | coletado_em: {h.get('coletado_em')} | doc_id: {h.get('doc_id')}")
        if h.get("url"):
            st.markdown(h["url"])
        st.divider()
