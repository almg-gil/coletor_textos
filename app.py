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
st.caption("Indexa a partir de CSVs no repositório, permite busca booleana + filtros, e exporta resultados e base indexada.")

DATA_DIR = "data"
INDEX_DIR = os.path.join(DATA_DIR, "index")
STATE_PATH = os.path.join(DATA_DIR, "index_state.json")

CSV_DIR = "data_csv"
CSV_PATTERN = os.path.join(CSV_DIR, "LegislacaoMineira_*.csv*")  # .csv ou .csv.gz

REQUEST_TIMEOUT = 30
MIN_INTERVAL_SECONDS = 1.0  # ~1 req/s
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "almg-whoosh-search/1.0"})

# Colunas do CSV (seus nomes)
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
    """
    Seu CSV pode estar TAB-separated.
    Tenta \t, ;, , até conseguir algo com colunas suficientes.
    """
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

def fetch_texto_por_link(url: str) -> Tuple[str, int]:
    if not url or not isinstance(url, str) or not url.strip():
        return "", 0

    try:
        r = rate_limited_get(url.strip())
        status = r.status_code
        ctype = (r.headers.get("Content-Type") or "").lower()

        if status != 200:
            return "", status

        if "application/json" in ctype or "dadosabertos.almg.gov.br/api/" in url:
            j = r.json()
            raw = parse_api_text(j)
            txt = strip_html_and_decode_if_needed(raw)
            return txt, status

        if "text/" in ctype or "html" in ctype:
            return strip_html(r.text or ""), status

        if "pdf" in ctype or "octet-stream" in ctype:
            return "", status

        return strip_html_and_decode_if_needed(r.text or ""), status

    except Exception:
        return "", 0


# =========================================================
# WHOOSH SCHEMA / OPEN
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

        # IMPORTANTÍSSIMO: stored=True aqui para exportar CSV com textos
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
# INDEXAÇÃO A PARTIR DOS CSVs
# =========================================================
def normalize_meta_df(df: pd.DataFrame) -> pd.DataFrame:
    needed = {COL_TIPO, COL_NUMERO, COL_ANO}
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"CSV sem coluna obrigatória: {c}")

    # garante colunas opcionais
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

    df["LinkPublicacao"] = df[COL_FONTE].apply(extract_first_href)
    df = df.drop_duplicates(subset=[COL_TIPO, COL_NUMERO, COL_ANO]).reset_index(drop=True)
    return df

def indexar_df(ix, df: pd.DataFrame, coletar_original: bool, coletar_atualizado: bool,
              limite: int, force_rebuild: bool) -> Tuple[int, int]:
    """
    Indexa/atualiza documentos.
    Retorna (atualizados, ignorados).
    """
    writer = ix.writer(limitmb=512, procs=1, multisegment=True)
    updated = 0
    skipped = 0

    total = min(len(df), limite) if limite else len(df)

    barra = st.progress(0)
    info = st.empty()
    t0 = time.time()

    # IMPORTANTE: não reutilizar o mesmo searcher durante escrita prolongada para "ler dados antigos"
    # Aqui usamos apenas para checar existência e pegar hashes/textos armazenados.
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

            texto_original = prev.get("texto_original") if prev else ""
            texto_atualizado = prev.get("texto_atualizado") if prev else ""
            hash_original = prev.get("hash_original") if prev else ""
            hash_atualizado = prev.get("hash_atualizado") if prev else ""

            coletou_algo = False

            if coletar_original and link_orig:
                txt, _code = fetch_texto_por_link(link_orig)
                if txt:
                    h = sha256_text(txt)
                    if force_rebuild or (h != hash_original):
                        texto_original = txt
                        hash_original = h
                        coletou_algo = True

            if coletar_atualizado and link_atu:
                txt, _code = fetch_texto_por_link(link_atu)
                if txt:
                    h = sha256_text(txt)
                    if force_rebuild or (h != hash_atualizado):
                        texto_atualizado = txt
                        hash_atualizado = h
                        coletou_algo = True

            # Grava se necessário (ou cria novo)
            if (not prev) or force_rebuild or coletou_algo:
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

            if (i + 1) % 10 == 0:
                elapsed = time.time() - t0
                info.write(f"Indexando **{i+1}/{total}** | atualizados: **{updated}** | tempo: **{elapsed/60:.1f} min**")

            barra.progress((i + 1) / total)

    writer.commit()
    return updated, skipped


# =========================================================
# EXPORTAR BASE DO ÍNDICE (com textos)
# =========================================================
def export_index(ix, tipos: List[str], ano: str, numero: str, limit: int) -> pd.DataFrame:
    """
    Exporta documentos armazenados no índice (stored=True),
    aplicando filtros simples.
    """
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
                "Origem": r.get("origem"),
                "Fonte": r.get("fonte"),
                "URLPortal": r.get("url_portal"),
                "LinkPublicacao": r.get("link_publicacao"),
                "LinkTextoOriginal": r.get("link_original"),
                "LinkTextoAtualizado": r.get("link_atualizado"),
                "TextoOriginal": r.get("texto_original"),
                "TextoAtualizado": r.get("texto_atualizado"),
                "ColetadoEm": r.get("coletado_em"),
            })
    return pd.DataFrame(rows)


# =========================================================
# UI: Sidebar (Indexação)
# =========================================================
year_files = list_year_files()
if not year_files:
    st.error(f"Não encontrei arquivos `{CSV_DIR}/LegislacaoMineira_YYYY.csv` no repositório.")
    st.stop()

anos_disponiveis = [y for y, _ in year_files]
map_ano_arquivo = {y: p for y, p in year_files}

ix = open_or_create_index()
state = load_state()

with st.sidebar:
    st.header("⚙️ Indexação")
    st.metric("Docs no índice", count_docs(ix))

    anos_sel = st.multiselect("Anos (para indexar)", options=anos_disponiveis, default=[anos_disponiveis[0]])
    coletar_orig = st.checkbox("Durante indexação: baixar texto original", value=True)
    coletar_atu = st.checkbox("Durante indexação: baixar texto atualizado", value=False)
    force_rebuild = st.checkbox("Forçar regravar tudo (rebuild)", value=False)

    limite = st.number_input("Limite por execução", min_value=1, max_value=20000, value=300, step=50)
    btn_indexar = st.button("📥 Indexar/Atualizar agora")

if btn_indexar:
    if not anos_sel:
        st.warning("Selecione pelo menos um ano para indexar.")
        st.stop()

    dfs = []
    for a in anos_sel:
        df = load_year_csv(map_ano_arquivo[a])
        df = normalize_meta_df(df)
        dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)
    st.info(f"Metadados carregados: {len(df_all)} registros. Iniciando indexação…")

    upd, skp = indexar_df(
        ix,
        df_all,
        coletar_original=coletar_orig,
        coletar_atualizado=coletar_atu,
        limite=int(limite),
        force_rebuild=force_rebuild,
    )
    state["last_indexed_at"] = now_iso()
    save_state(state)
    st.success(f"Indexação concluída. Atualizados: {upd} | Ignorados: {skp}")

# =========================================================
# BUSCA (Whoosh)
# =========================================================
st.subheader("🔎 Buscar no índice (booleana + campos)")

col1, col2 = st.columns([2, 1])
query = col1.text_input(
    "Consulta booleana (AND/OR/NOT, parênteses, aspas)",
    value='("utilidade pública") AND NOT ("servidão")'
)
limit_hits = col2.number_input("Qtde de resultados", min_value=5, max_value=500, value=30, step=5)

TIPOS_PADRAO = ["ADT","CON","DCS","DCJ","DEC","DNE","DSN","DEL","DLB","DCE","EMC","LEI","LEA","LCP","LDL","LCO","OSV","PRT","PTC","RAL"]

f1, f2, f3, f4 = st.columns(4)
f_tipos = f1.multiselect("Tipo", options=TIPOS_PADRAO, default=[])
f_ano = f2.text_input("Ano (opcional)", value="")
f_num = f3.text_input("Número (opcional)", value="")
campo = f4.selectbox("Campo de busca", ["Ambos (original+atualizado)", "Somente original", "Somente atualizado"], index=0)

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
                "LinkTextoOriginal": r.get("link_original"),
                "LinkTextoAtualizado": r.get("link_atualizado"),
                "TextoOriginal": r.get("texto_original"),
                "TextoAtualizado": r.get("texto_atualizado"),
                "ColetadoEm": r.get("coletado_em"),
                "Score": float(r.score),
            })

    return pd.DataFrame(rows)

colA, colB = st.columns([1, 2])
btn_buscar = colA.button("Buscar")
st.caption("Se os textos estiverem vazios, é porque o índice ainda não foi alimentado com textos. Use a sidebar e marque “baixar texto original” durante a indexação.")

if btn_buscar:
    if f_ano and not f_ano.isdigit():
        st.error("Ano deve ser numérico ou vazio.")
        st.stop()
    if f_num and not f_num.isdigit():
        st.error("Número deve ser numérico ou vazio.")
        st.stop()

    try:
        df_res = run_search(ix, query, f_tipos, f_ano.strip(), f_num.strip(), campo, int(limit_hits))
    except Exception as e:
        st.error(f"Erro na busca (sintaxe/consulta): {e}")
        st.stop()

    st.success(f"Resultados: {len(df_res)}")
    st.dataframe(df_res, use_container_width=True)

    buf = BytesIO()
    df_res.to_csv(buf, index=False, encoding="utf-8-sig")
    st.download_button(
        "⬇️ Baixar resultados (CSV)",
        data=buf.getvalue(),
        file_name="resultados_busca_almg.csv",
        mime="text/csv"
    )

# =========================================================
# EXPORTAR BASE INDEXADA (CSV)
# =========================================================
st.subheader("📦 Exportar base indexada (do Whoosh)")

e1, e2, e3, e4 = st.columns(4)
exp_tipos = e1.multiselect("Filtrar Tipo (export)", options=TIPOS_PADRAO, default=[])
exp_ano = e2.text_input("Ano (export, opcional)", value="")
exp_num = e3.text_input("Número (export, opcional)", value="")
exp_limit = e4.number_input("Limite (export)", min_value=1, max_value=200000, value=5000, step=500)

if st.button("Gerar CSV da base indexada"):
    if exp_ano and not exp_ano.isdigit():
        st.error("Ano (export) deve ser numérico ou vazio.")
        st.stop()
    if exp_num and not exp_num.isdigit():
        st.error("Número (export) deve ser numérico ou vazio.")
        st.stop()

    df_base = export_index(ix, exp_tipos, exp_ano.strip(), exp_num.strip(), int(exp_limit))
    st.success(f"Linhas exportadas do índice: {len(df_base)}")
    st.dataframe(df_base.head(50), use_container_width=True)

    buf2 = BytesIO()
    df_base.to_csv(buf2, index=False, encoding="utf-8-sig")
    st.download_button(
        "⬇️ Baixar base indexada (CSV)",
        data=buf2.getvalue(),
        file_name="base_indexada_whoosh.csv",
        mime="text/csv"
    )
