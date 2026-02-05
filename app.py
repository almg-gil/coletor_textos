import os
import io
import re
import json
import time
import zipfile
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
import streamlit as st

from whoosh import index
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NUMERIC
from whoosh.qparser import MultifieldParser, OrGroup
from whoosh.query import And, Term, Every


# =================================================
# CONFIG BÁSICA (AJUSTE AQUI)
# =================================================
# Preferência: configurar via Streamlit Secrets:
# [github]
# owner="..."
# repo="..."
# token="..."  (opcional, ajuda com rate limit)
#
# Alternativa: variáveis de ambiente GITHUB_OWNER / GITHUB_REPO / GITHUB_TOKEN

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

ASSET_NAME = os.getenv("INDEX_ASSET_NAME", "index.zip")  # nome do asset no Release
DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
META_PATH = os.path.join(DATA_DIR, "release_meta.json")

REQUEST_TIMEOUT = 30


# =================================================
# STREAMLIT UI
# =================================================
st.set_page_config(page_title="Motor de Busca ALMG (Index Release)", layout="wide")
st.title("📚 Motor de Busca ALMG — Release Index (Whoosh)")
st.caption("O app baixa o índice pronto (index.zip) do Release 'latest' no GitHub e serve busca booleana com campos.")


# =================================================
# UTIL
# =================================================
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

def clean_extract_zip(zip_bytes: bytes, dest_dir: str) -> None:
    """
    Extrai zip com proteção básica contra Zip Slip.
    Espera que o zip contenha algo como: data/index/... e data/state.json
    """
    mkdirp(dest_dir)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for member in z.infolist():
            # bloqueia paths maliciosos
            member_path = member.filename.replace("\\", "/")
            if member_path.startswith("/") or ".." in member_path.split("/"):
                continue
            z.extract(member, path=".")


# =================================================
# GITHUB RELEASE (latest) - FETCH
# =================================================
def github_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "streamlit-almg-search",
    }
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def fetch_latest_release_info(owner: str, repo: str) -> dict:
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    r = requests.get(url, headers=github_headers(), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def pick_asset(release_json: dict, asset_name: str) -> Optional[dict]:
    for a in release_json.get("assets", []):
        if a.get("name") == asset_name:
            return a
    return None

def download_asset_bytes(asset: dict) -> bytes:
    """
    Baixa asset usando browser_download_url (público).
    Se o repo for privado, precisa token e usar asset API. Aqui assumo repo público.
    """
    url = asset.get("browser_download_url")
    if not url:
        raise RuntimeError("Asset não tem browser_download_url.")
    r = requests.get(url, headers={"User-Agent": "streamlit-almg-search"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.content

def current_local_release_meta() -> dict:
    return load_json(META_PATH, default={})

def should_download_new_release(remote_release: dict, local_meta: dict) -> bool:
    remote_id = remote_release.get("id")
    local_id = local_meta.get("release_id")
    return (remote_id is not None) and (remote_id != local_id)

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

def index_is_ready() -> bool:
    return dir_exists(INDEX_DIR) and index.exists_in(INDEX_DIR)

# =================================================
# WHOOSH
# =================================================
def get_schema() -> Schema:
    # Só pra fallback de criação (não deve acontecer se você sempre baixar o index.zip)
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
def open_index_cached(index_dir: str):
    # cache do objeto índice; se você atualizar o índice no disco,
    # chame st.cache_resource.clear() antes de reabrir
    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    # fallback: cria vazio (não recomendado, mas evita crash)
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
# BOOTSTRAP / UPDATE INDEX FROM RELEASE
# =================================================
def ensure_index_from_release(force: bool = False) -> Tuple[bool, str]:
    """
    Garante que existe índice local, baixando o release latest se necessário.
    Retorna (ok, mensagem).
    """
    if not GITHUB_OWNER or not GITHUB_REPO:
        return False, "Configure GITHUB_OWNER e GITHUB_REPO (secrets ou env)."

    mkdirp(DATA_DIR)

    local_meta = current_local_release_meta()

    try:
        release = fetch_latest_release_info(GITHUB_OWNER, GITHUB_REPO)
    except Exception as e:
        # Se já existe índice local, segue com ele
        if index_is_ready():
            return True, f"Não consegui consultar Release latest, usando índice local. Erro: {e}"
        return False, f"Falha ao consultar Release latest: {e}"

    asset = pick_asset(release, ASSET_NAME)
    if not asset:
        return False, f"Release latest encontrado, mas não achei asset '{ASSET_NAME}'."

    need_download = force or (not index_is_ready()) or should_download_new_release(release, local_meta)

    if not need_download:
        return True, "Índice local já está atualizado com o Release latest."

    # Download + extract
    try:
        zip_bytes = download_asset_bytes(asset)
        zhash = sha256_bytes(zip_bytes)

        # Extrai: o zip deve conter pasta data/index e data/state.json
        clean_extract_zip(zip_bytes, dest_dir=".")

        # valida
        if not index_is_ready():
            return False, "Baixei e extraí o zip, mas não encontrei um índice Whoosh válido em data/index."

        save_local_release_meta(release, zhash)
        return True, "Índice atualizado a partir do Release latest."
    except Exception as e:
        # se já havia índice, usa o existente
        if index_is_ready():
            return True, f"Falha ao atualizar, usando índice local existente. Erro: {e}"
        return False, f"Falha ao baixar/extrair index.zip: {e}"


# =================================================
# SIDEBAR: STATUS + UPDATE
# =================================================
with st.sidebar:
    st.header("⚙️ Fonte do índice")
    st.write(f"Repo: **{GITHUB_OWNER}/{GITHUB_REPO}**" if (GITHUB_OWNER and GITHUB_REPO) else "Repo: **(não configurado)**")
    st.write(f"Asset esperado: **{ASSET_NAME}**")
    st.caption("Se o repo for público, não precisa token. Se privado, use token em secrets.")

    local_meta = current_local_release_meta()
    if local_meta:
        st.write("**Release local:**")
        st.json(local_meta, expanded=False)

    colA, colB = st.columns(2)
    auto_on_start = colA.checkbox("Baixar ao abrir", value=True)
    force_update_btn = colB.button("Atualizar índice agora")

# Baixa (ou confirma) índice
if auto_on_start and "bootstrapped" not in st.session_state:
    st.session_state["bootstrapped"] = True
    with st.spinner("Verificando/baixando índice do Release latest…"):
        ok, msg = ensure_index_from_release(force=False)
    if ok:
        st.success(msg)
    else:
        st.error(msg)

if force_update_btn:
    with st.spinner("Forçando atualização do índice…"):
        ok, msg = ensure_index_from_release(force=True)
        # índice mudou em disco -> limpa cache do open_index
        st.cache_resource.clear()
    if ok:
        st.success(msg)
    else:
        st.error(msg)

# Abre índice (mesmo se não baixou — pode existir localmente)
ix = open_index_cached(INDEX_DIR)

# mostra métricas
try:
    st.sidebar.metric("Docs no índice", count_docs(ix))
except Exception:
    st.sidebar.metric("Docs no índice", 0)

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
f_tipo = f1.text_input("tipo_sigla (opcional)", value="")
f_ano = f2.text_input("ano (opcional)", value="")
f_num = f3.text_input("numero (opcional)", value="")
f_versao = f4.selectbox("versao (opcional)", ["", "Original", "Consolidado"], index=0)

# Dica de uso: não filtrar por ano sem querer
st.caption("Dica: deixe filtros vazios para buscar no acervo inteiro. Preencha apenas se quiser restringir.")

if st.button("Buscar"):
    filtros = {
        "tipo_sigla": f_tipo.strip(),
        "ano": f_ano.strip(),
        "numero": f_num.strip(),
        "versao": f_versao.strip(),
    }

    # valida números
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
        st.caption(f"doc_id: {h.get('doc_id')} | score: {h.get('score'):.3f} | coletado_em: {h.get('coletado_em')}")
        if h.get("url"):
            st.markdown(h["url"])
        st.divider()
