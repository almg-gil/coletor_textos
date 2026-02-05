import os
import re
import time
import glob
import json
import base64
import hashlib
from datetime import datetime
from io import BytesIO
from typing import List, Tuple

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

from whoosh import index
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from whoosh.analysis import RegexTokenizer, LowercaseFilter, CharsetFilter
from whoosh.support.charset import accent_map
from whoosh.qparser import MultifieldParser, OrGroup
from whoosh.query import And, Or, Term, NumericRange, Every


# =========================================================
# CONFIG / PATHS
# =========================================================
st.set_page_config(page_title="Motor de Busca ALMG (Whoosh)", layout="wide")
st.title("📚 Motor de Busca ALMG — Whoosh (booleano + campos)")
st.caption("Indexa a partir de CSVs no repositório, baixa textos via Links (Dados Abertos), busca booleana e exporta CSV.")

DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
STATE_PATH = os.path.join(DATA_DIR, "index_state.json")

CSV_DIR = "data_csv"
CSV_PATTERN = os.path.join(CSV_DIR, "LegislacaoMineira_*.csv*")

REQUEST_TIMEOUT = 30
MIN_INTERVAL_SECONDS = 1.0  # ~1 req/s
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "almg-whoosh-search/1.0"})

COL_TIPO = "Tipo"
COL_NUMERO = "Numero"
COL_ANO = "Ano"
COL_EMENTA = "Ementa"
COL_RESUMO = "Resumo"
COL_FONTE = "Fonte"
COL_ORIGEM = "Origem"
COL_LINK_ORIG = "LinkTextoOriginal"
COL_LINK_ATU = "LinkTextoAtualizado"


# =========================================================
# UTIL
# =========================================================
def mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def limpar_texto(texto: str) -> str:
    texto = re.sub(r"[ \t]+", " ", texto or "")
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()

def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()

def url_portal(tipo: str, numero: int, ano: int) -> str:
    return f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}/"

def extract_first_href(html_snippet: str) -> str:
    if not isinstance(html_snippet, str) or not html_snippet.strip():
        return ""
    m = re.search(r'href="([^"]+)"', html_snippet)
    return m.group(1) if m else ""

def read_csv_smart(path: str) -> pd.DataFrame:
    for sep in ["\t", ";", ","]:
        try:
            if path.endswith(".gz"):
                df = pd.read_csv(path, compression="gzip", sep=sep, dtype=str, engine="python")
            else:
                df = pd.read_csv(path, sep=sep, dtype=str, engine="python")
            if df.shape[1] >= 3:
                return df
        except Exception:
            pass
    if path.endswith(".gz"):
        return pd.read_csv(path, compression="gzip", dtype=str, engine="python")
    return pd.read_csv(path, dtype=str, engine="python")

@st.cache_data(show_spinner=False)
def list_year_files() -> List[Tuple[int, str]]:
    files = sorted(glob.glob(CSV_PATTERN))
    out = []
    for f in files:
        m = re.search(r"LegislacaoMineira_(\d{4})\.csv(\.gz)?$", os.path.basename(f))
        if m:
            out.append((int(m.group(1)), f))
    out.sort(key=lambda x: x[0], reverse=True)
    return out

@st.cache_data(show_spinner=False)
def load_year_csv(path: str) -> pd.DataFrame:
    return read_csv_smart(path)

def make_doc_id(tipo: str, numero: int, ano: int) -> str:
    return f"{tipo.upper()}_{int(numero)}_{int(ano)}"


# =========================================================
# DOWNLOAD DE TEXTO (API/HTML/base64) + LIMPEZA
# =========================================================
_last_req_ts = 0.0
def rate_limited_get(url: str) -> requests.Response:
    global _last_req_ts
    elapsed = time.time() - _last_req_ts
    if elapsed < MIN_INTERVAL_SECONDS:
        time.sleep(MIN_INTERVAL_SECONDS - elapsed)
    resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    _last_req_ts = time.time()
    return resp

def looks_like_base64(s: str) -> bool:
    if not s or len(s) < 200:
        return False
    return re.fullmatch(r"[A-Za-z0-9+/=\s]+", s) is not None

def strip_html(texto: str) -> str:
    soup = BeautifulSoup(texto or "", "html.parser")
    return limpar_texto(soup.get_text("\n", strip=True))

def parse_api_text(obj) -> str:
    if isinstance(obj, str):
        return obj.strip()
    if not isinstance(obj, dict):
        return ""
    for k in ["conteudo", "texto", "html", "content", "documento", "body"]:
        v = obj.get(k)
        if isinstance(v, str) and len(v.strip()) > 20:
            return v.strip()
    for k in ["conteudoBase64", "arquivoBase64", "base64", "pdfBase64", "binarioBase64"]:
        v = obj.get(k)
        if isinstance(v, str) and len(v.strip()) > 200:
            return v.strip()
    for k in ["dados", "data", "item", "resultado", "result"]:
        v = obj.get(k)
        if isinstance(v, dict):
            t = parse_api_text(v)
            if t:
                return t
    for k in ["itens", "items", "resultados", "results"]:
        arr = obj.get(k)
        if isinstance(arr, list):
            for el in arr:
                if isinstance(el, dict):
                    t = parse_api_text(el)
                    if t:
                        return t
    return ""

def strip_html_and_decode_if_needed(texto: str) -> str:
    if not texto:
        return ""
    texto = texto.strip()
    if looks_like_base64(texto):
        try:
            decoded = base64.b64decode(texto, validate=False)
            decoded_txt = decoded.decode("utf-8", errors="ignore").strip()
            if len(decoded_txt) > 50:
                texto = decoded_txt
        except Exception:
            pass
    if "<" in texto and ">" in texto:
        return strip_html(texto)
    return limpar_texto(texto)

def fetch_texto_por_link(url: str) -> Tuple[str, int, str]:
    if not url or not isinstance(url, str) or not url.strip():
        return "", 0, "url_vazia"
    try:
        r = rate_limited_get(url.strip())
        status = r.status_code
        ctype = (r.headers.get("Content-Type") or "").lower()
        if status != 200:
            return "", status, f"status={status} ctype={ctype}"

        if "application/json" in ctype or "dadosabertos.almg.gov.br/api/" in url:
            try:
                j = r.json()
                raw = parse_api_text(j)
                txt = strip_html_and_decode_if_needed(raw)
                if not txt:
                    keys = list(j.keys())[:25] if isinstance(j, dict) else []
                    return "", status, f"mode=json_sem_texto keys={keys}"
                return txt, status, "mode=json_ok"
            except Exception as e:
                return "", status, f"mode=json_erro {e}"

        if "text/" in ctype or "html" in ctype:
            html = r.text or ""
            soup = BeautifulSoup(html, "html.parser")
            main = soup.find("main") or soup
            for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
                tag.decompose()
            txt = limpar_texto(main.get_text("\n", strip=True))
            return txt, status, "mode=html_ok"

        if "pdf" in ctype or "octet-stream" in ctype:
            return "", status, f"mode=binary({ctype})"

        txt = strip_html_and_decode_if_needed(r.text or "")
        return txt, status, "mode=fallback"
    except Exception as e:
        return "", 0, f"erro={e}"


# =========================================================
# WHOOSH
# =========================================================
ANALYZER = RegexTokenizer() | LowercaseFilter() | CharsetFilter(accent_map)

def get_schema() -> Schema:
    return Schema(
        doc_id=ID(stored=True, unique=True),
        tipo_sigla=ID(stored=True),
        numero=NUMERIC(stored=True, numtype=int),
        ano=NUMERIC(stored=True, numtype=int),
        ementa=TEXT(stored=True),
        resumo=TEXT(stored=True),
        origem=ID(stored=True),
        fonte=TEXT(stored=True),
        url_portal=ID(stored=True),
        link_publicacao=ID(stored=True),
        link_original=ID(stored=True),
        link_atualizado=ID(stored=True),
        # stored=True para exportar CSV
        texto_original=TEXT(stored=True, analyzer=ANALYZER),
        texto_atualizado=TEXT(stored=True, analyzer=ANALYZER),
        coletado_em=ID(stored=True),
        hash_original=ID(stored=True),
        hash_atualizado=ID(stored=True),
    )

def open_or_create_index():
    mkdirp(INDEX_DIR)
    if index.exists_in(INDEX_DIR):
        return index.open_dir(INDEX_DIR)
    return index.create_in(INDEX_DIR, get_schema())

def load_state() -> dict:
    mkdirp(DATA_DIR)
    if not os.path.exists(STATE_PATH):
        return {"created_at": now_iso()}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"created_at": now_iso()}

def save_state(state: dict) -> None:
    mkdirp(DATA_DIR)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)

def count_docs(ix) -> int:
    with ix.searcher() as s:
        return s.doc_count()


# =========================================================
# INDEXAÇÃO
# =========================================================
def normalize_meta_df(df: pd.DataFrame) -> pd.DataFrame:
    needed = {COL_TIPO, COL_NUMERO, COL_ANO}
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"CSV sem coluna obrigatória: {c}")

    for col in [COL_LINK_ORIG, COL_LINK_ATU, COL_EMENTA, COL_RESUMO, COL_ORIGEM, COL_FONTE]:
        if col not in df.columns:
            df[col] = ""

    df = df.copy()
    df[COL_TIPO] = df[COL_TIPO].astype(str).str.upper().str.strip()
    df[COL_NUMERO] = pd.to_numeric(df[COL_NUMERO], errors="coerce")
    df[COL_ANO] = pd.to_numeric(df[COL_ANO], errors="coerce")
    df = df.dropna(subset=[COL_TIPO, COL_NUMERO, COL_ANO])
    df[COL_NUMERO] = df[COL_NUMERO].astype(int)
    df[COL_ANO] = df[COL_ANO].astype(int)

    df["LinkPublicacao"] = df[COL_FONTE].apply(extract_first_href) if COL_FONTE in df.columns else ""
    df = df.drop_duplicates(subset=[COL_TIPO, COL_NUMERO, COL_ANO]).reset_index(drop=True)
    return df

def indexar_df(ix, df: pd.DataFrame, coletar_original: bool, coletar_atualizado: bool,
              limite: int, force_rebuild: bool, debug_on: bool) -> Tuple[int, int]:
    writer = ix.writer(limitmb=512, procs=1, multisegment=True)
    updated, skipped = 0, 0
    total = min(len(df), limite) if limite else len(df)

    barra = st.progress(0)
    info = st.empty()

    # IMPORTANTE: buscar prev em um searcher "de leitura"
    # mas a decisão de update inclui o caso "texto armazenado vazio".
    with ix.searcher() as s:
        for i in range(total):
            row = df.iloc[i]
            tipo = str(row[COL_TIPO]).upper().strip()
            numero = int(row[COL_NUMERO])
            ano = int(row[COL_ANO])
            doc_id = make_doc_id(tipo, numero, ano)

            link_orig = str(row.get(COL_LINK_ORIG, "") or "").strip()
            link_atu = str(row.get(COL_LINK_ATU, "") or "").strip()

            ementa = str(row.get(COL_EMENTA, "") or "")
            resumo = str(row.get(COL_RESUMO, "") or "")
            origem = str(row.get(COL_ORIGEM, "") or "")
            fonte = str(row.get(COL_FONTE, "") or "")
            link_pub = str(row.get("LinkPublicacao", "") or "")

            prev = s.document(doc_id=doc_id)

            prev_txt_orig = (prev.get("texto_original") or "") if prev else ""
            prev_txt_atu = (prev.get("texto_atualizado") or "") if prev else ""
            prev_hash_orig = (prev.get("hash_original") or "") if prev else ""
            prev_hash_atu = (prev.get("hash_atualizado") or "") if prev else ""

            texto_original = prev_txt_orig
            texto_atualizado = prev_txt_atu
            hash_original = prev_hash_orig
            hash_atualizado = prev_hash_atu

            debug_o = ""
            debug_a = ""

            changed = force_rebuild or (not prev)

            # --- ORIGINAL
            if coletar_original and link_orig:
                txt, code, dbg = fetch_texto_por_link(link_orig)
                debug_o = f"{dbg} status={code} len={len(txt)}"
                if txt:
                    h = sha256_text(txt)

                    # ATUALIZA se:
                    # - hash mudou, OU
                    # - stored estava vazio, OU
                    # - force_rebuild
                    if force_rebuild or (h != prev_hash_orig) or (not prev_txt_orig.strip()):
                        texto_original = txt
                        hash_original = h
                        changed = True

            # --- ATUALIZADO
            if coletar_atualizado and link_atu:
                txt, code, dbg = fetch_texto_por_link(link_atu)
                debug_a = f"{dbg} status={code} len={len(txt)}"
                if txt:
                    h = sha256_text(txt)
                    if force_rebuild or (h != prev_hash_atu) or (not prev_txt_atu.strip()):
                        texto_atualizado = txt
                        hash_atualizado = h
                        changed = True

            if changed:
                writer.update_document(
                    doc_id=doc_id,
                    tipo_sigla=tipo,
                    numero=numero,
                    ano=ano,
                    ementa=ementa,
                    resumo=resumo,
                    origem=origem,
                    fonte=fonte,
                    url_portal=url_portal(tipo, numero, ano),
                    link_publicacao=link_pub,
                    link_original=link_orig,
                    link_atualizado=link_atu,
                    texto_original=texto_original or "",
                    texto_atualizado=texto_atualizado or "",
                    coletado_em=now_iso(),
                    hash_original=hash_original or "",
                    hash_atualizado=hash_atualizado or "",
                )
                updated += 1
            else:
                skipped += 1

            if debug_on and (i < 30):
                info.write(f"Ex.: {tipo} {numero}/{ano} | orig: {debug_o} | atu: {debug_a}")

            barra.progress((i + 1) / total)

    writer.commit()
    return updated, skipped


# =========================================================
# EXPORT / SEARCH
# =========================================================
def export_index(ix, tipos: List[str], ano: str, numero: str, limit: int) -> pd.DataFrame:
    filtros = []
    tipos = [t for t in (tipos or []) if t]
    if tipos:
        filtros.append(Or([Term("tipo_sigla", t.upper()) for t in tipos]))
    if ano and ano.isdigit():
        y = int(ano)
        filtros.append(NumericRange("ano", y, y))
    if numero and numero.isdigit():
        n = int(numero)
        filtros.append(NumericRange("numero", n, n))

    q = And([Every()] + filtros) if filtros else Every()

    rows = []
    with ix.searcher() as s:
        hits = s.search(q, limit=limit if limit else None)
        for r in hits:
            rows.append({
                "Tipo": r.get("tipo_sigla"),
                "Numero": r.get("numero"),
                "Ano": r.get("ano"),
                "Ementa": r.get("ementa"),
                "Resumo": r.get("resumo"),
                "URLPortal": r.get("url_portal"),
                "LinkTextoOriginal": r.get("link_original"),
                "LinkTextoAtualizado": r.get("link_atualizado"),
                "TextoOriginal": r.get("texto_original"),
                "TextoAtualizado": r.get("texto_atualizado"),
                "ColetadoEm": r.get("coletado_em"),
            })
    return pd.DataFrame(rows)

def run_search(ix, expr: str, tipos: List[str], ano: str, numero: str, campo: str, limit: int) -> pd.DataFrame:
    if campo == "Somente original":
        fields = ["texto_original"]
    elif campo == "Somente atualizado":
        fields = ["texto_atualizado"]
    else:
        fields = ["texto_original", "texto_atualizado"]

    parser = MultifieldParser(fields, schema=ix.schema, group=OrGroup)
    q_text = (expr or "").strip()
    base_q = parser.parse(q_text) if q_text else Every()

    filtros = []
    tipos = [t for t in (tipos or []) if t]
    if tipos:
        filtros.append(Or([Term("tipo_sigla", t.upper()) for t in tipos]))
    if ano and ano.isdigit():
        y = int(ano)
        filtros.append(NumericRange("ano", y, y))
    if numero and numero.isdigit():
        n = int(numero)
        filtros.append(NumericRange("numero", n, n))

    q = And([base_q] + filtros) if filtros else base_q

    rows = []
    with ix.searcher() as s:
        hits = s.search(q, limit=limit)
        for r in hits:
            rows.append({
                "Tipo": r.get("tipo_sigla"),
                "Numero": r.get("numero"),
                "Ano": r.get("ano"),
                "Ementa": r.get("ementa"),
                "URLPortal": r.get("url_portal"),
                "TextoOriginal": r.get("texto_original"),
                "TextoAtualizado": r.get("texto_atualizado"),
                "ColetadoEm": r.get("coletado_em"),
                "Score": float(r.score),
            })
    return pd.DataFrame(rows)


# =========================================================
# UI
# =========================================================
year_files = list_year_files()
if not year_files:
    st.error(f"Não encontrei arquivos `{CSV_DIR}/LegislacaoMineira_YYYY.csv` no repositório.")
    st.stop()

anos_disponiveis = [y for y, _ in year_files]
map_ano_arquivo = {y: p for y, p in year_files}

ix = open_or_create_index()
state = load_state()

TIPOS_PADRAO = ["ADT","CON","DCS","DCJ","DEC","DNE","DSN","DEL","DLB","DCE","EMC","LEI","LEA","LCP","LDL","LCO","OSV","PRT","PTC","RAL"]

with st.sidebar:
    st.header("⚙️ Indexação")
    st.metric("Docs no índice", count_docs(ix))

    anos_sel = st.multiselect("Anos (para indexar)", options=anos_disponiveis, default=[anos_disponiveis[0]])
    coletar_orig = st.checkbox("Baixar texto original", value=True)
    coletar_atu = st.checkbox("Baixar texto atualizado", value=False)
    force_rebuild = st.checkbox("Forçar regravar tudo", value=False)
    debug_on = st.checkbox("Debug (mostra 1ºs itens)", value=True)

    limite = st.number_input("Limite por execução", min_value=1, max_value=20000, value=300, step=50)
    btn_indexar = st.button("📥 Indexar/Atualizar agora")

if btn_indexar:
    dfs = []
    for a in anos_sel:
        df = load_year_csv(map_ano_arquivo[a])
        df = normalize_meta_df(df)
        dfs.append(df)
    df_all = pd.concat(dfs, ignore_index=True)

    st.info(f"Metadados carregados: {len(df_all)}. Indexando…")
    upd, skp = indexar_df(ix, df_all, coletar_orig, coletar_atu, int(limite), force_rebuild, debug_on)

    state["last_indexed_at"] = now_iso()
    save_state(state)

    # reabrir índice para garantir leitura do que foi gravado
    ix = index.open_dir(INDEX_DIR)

    st.success(f"Indexação concluída. Atualizados: {upd} | Ignorados: {skp}")


st.subheader("🔎 Buscar no índice (booleana + campos)")

col1, col2 = st.columns([2, 1])
query = col1.text_input("Consulta booleana", value='("utilidade pública") AND NOT ("servidão")')
limit_hits = col2.number_input("Qtde resultados", min_value=5, max_value=500, value=30, step=5)

f1, f2, f3, f4 = st.columns(4)
f_tipos = f1.multiselect("Tipo", options=TIPOS_PADRAO, default=[])
f_ano = f2.text_input("Ano (opcional)", value="")
f_num = f3.text_input("Número (opcional)", value="")
campo = f4.selectbox("Campo", ["Ambos (original+atualizado)", "Somente original", "Somente atualizado"], index=0)

if st.button("Buscar"):
    df_res = run_search(ix, query, f_tipos, f_ano.strip(), f_num.strip(), campo, int(limit_hits))
    st.success(f"Resultados: {len(df_res)}")
    st.dataframe(df_res, use_container_width=True)

    buf = BytesIO()
    df_res.to_csv(buf, index=False, encoding="utf-8-sig")
    st.download_button("⬇️ Baixar resultados (CSV)", data=buf.getvalue(), file_name="resultados_busca_almg.csv", mime="text/csv")


st.subheader("📦 Exportar base indexada (do Whoosh)")

e1, e2, e3, e4 = st.columns(4)
exp_tipos = e1.multiselect("Filtrar Tipo (export)", options=TIPOS_PADRAO, default=[])
exp_ano = e2.text_input("Ano (export, opcional)", value="")
exp_num = e3.text_input("Número (export, opcional)", value="")
exp_limit = e4.number_input("Limite (export)", min_value=1, max_value=200000, value=5000, step=500)

if st.button("Gerar CSV da base indexada"):
    df_base = export_index(ix, exp_tipos, exp_ano.strip(), exp_num.strip(), int(exp_limit))
    st.success(f"Linhas exportadas do índice: {len(df_base)}")
    st.dataframe(df_base.head(50), use_container_width=True)

    buf2 = BytesIO()
    df_base.to_csv(buf2, index=False, encoding="utf-8-sig")
    st.download_button("⬇️ Baixar base indexada (CSV)", data=buf2.getvalue(), file_name="base_indexada_whoosh.csv", mime="text/csv")
