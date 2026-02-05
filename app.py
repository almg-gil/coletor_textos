import os
import re
import time
import glob
import hashlib
from datetime import datetime
from io import BytesIO
from typing import List, Tuple

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


# =================================================
# CONFIG
# =================================================
st.set_page_config(page_title="Coletor ALMG (CSV por ano + links)", layout="wide")
st.title("📄 Coletor de Textos — ALMG (CSV por ano no GitHub)")
st.caption("Lê automaticamente LegislacaoMineira_YYYY.csv do repositório, usa LinkTextoOriginal/LinkTextoAtualizado para baixar textos e gera CSV final com textos.")

DATA_DIR = "data_csv"
FILE_PATTERN = os.path.join(DATA_DIR, "LegislacaoMineira_*.csv*")  # .csv ou .csv.gz

REQUEST_TIMEOUT = 30
MIN_INTERVAL_SECONDS = 1.0  # respeitar 1 req/s

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "streamlit-almg-coletor/1.0"})

# Colunas do seu CSV
COL_TIPO = "Tipo"
COL_NUMERO = "Numero"
COL_ANO = "Ano"
COL_LINK_ORIG = "LinkTextoOriginal"
COL_LINK_ATU = "LinkTextoAtualizado"


# =================================================
# UTIL
# =================================================
def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()

def limpar_texto(texto: str) -> str:
    texto = re.sub(r"[ \t]+", " ", texto or "")
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    return texto.strip()

def url_portal(tipo: str, numero: int, ano: int) -> str:
    return f"https://www.almg.gov.br/legislacao-mineira/texto/{tipo}/{numero}/{ano}/"

_last_req_ts = 0.0
def rate_limited_get(url: str) -> requests.Response:
    global _last_req_ts
    elapsed = time.time() - _last_req_ts
    if elapsed < MIN_INTERVAL_SECONDS:
        time.sleep(MIN_INTERVAL_SECONDS - elapsed)
    resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    _last_req_ts = time.time()
    return resp

def parse_api_text(resp_json) -> str:
    """
    Tenta extrair o conteúdo de vários formatos possíveis.
    """
    if isinstance(resp_json, str) and len(resp_json.strip()) > 20:
        return resp_json.strip()

    if not isinstance(resp_json, dict):
        return ""

    for key in ["conteudo", "content", "texto", "html", "documento", "body"]:
        v = resp_json.get(key)
        if isinstance(v, str) and len(v.strip()) > 20:
            return v.strip()

    for key in ["dados", "data", "item", "resultado", "result"]:
        v = resp_json.get(key)
        if isinstance(v, dict):
            t = parse_api_text(v)
            if t:
                return t

    for key in ["itens", "items", "resultados", "results"]:
        arr = resp_json.get(key)
        if isinstance(arr, list):
            for el in arr:
                if isinstance(el, dict):
                    t = parse_api_text(el)
                    if t:
                        return t
    return ""

def strip_html_if_needed(texto: str) -> str:
    if not texto:
        return ""
    if "<" in texto and ">" in texto:
        soup = BeautifulSoup(texto, "html.parser")
        return limpar_texto(soup.get_text("\n", strip=True))
    return limpar_texto(texto)

def fetch_texto_por_link(url: str) -> Tuple[str, int]:
    """
    Baixa texto a partir do link fornecido no CSV.
    Pode ser API (JSON) ou HTML.
    """
    if not url or not isinstance(url, str):
        return "", 0

    try:
        r = rate_limited_get(url)
        status = r.status_code
        if status != 200:
            return "", status

        ctype = (r.headers.get("Content-Type") or "").lower()

        # Se vier JSON
        if "application/json" in ctype or url.lower().endswith(".json") or "api/" in url.lower():
            j = r.json()
            raw = parse_api_text(j)
            return strip_html_if_needed(raw), status

        # Se vier HTML direto
        html = r.text or ""
        soup = BeautifulSoup(html, "html.parser")
        # tenta extrair texto principal
        main = soup.find("main") or soup
        for tag in main.find_all(["nav", "header", "footer", "script", "style", "button", "aside"]):
            tag.decompose()
        txt = limpar_texto(main.get_text("\n", strip=True))
        return txt, status

    except Exception:
        return "", 0


# =================================================
# ARQUIVOS NO REPO
# =================================================
@st.cache_data(show_spinner=False)
def list_year_files() -> List[Tuple[int, str]]:
    files = sorted(glob.glob(FILE_PATTERN))
    out = []
    for f in files:
        m = re.search(r"LegislacaoMineira_(\d{4})\.csv(\.gz)?$", os.path.basename(f))
        if m:
            out.append((int(m.group(1)), f))
    out.sort(key=lambda x: x[0], reverse=True)
    return out

@st.cache_data(show_spinner=False)
def load_year_csv(path: str) -> pd.DataFrame:
    if path.endswith(".gz"):
        return pd.read_csv(path, compression="gzip")
    return pd.read_csv(path)


year_files = list_year_files()
if not year_files:
    st.error(f"Não encontrei arquivos em `{DATA_DIR}` com o padrão `LegislacaoMineira_YYYY.csv`.")
    st.stop()

anos_disponiveis = [y for y, _ in year_files]
map_ano_arquivo = {y: p for y, p in year_files}

anos_sel = st.multiselect("📅 Selecione ano(s) para carregar", options=anos_disponiveis, default=[anos_disponiveis[0]])
if not anos_sel:
    st.stop()

dfs = [load_year_csv(map_ano_arquivo[a]) for a in anos_sel]
base = pd.concat(dfs, ignore_index=True)

# valida colunas
needed = {COL_TIPO, COL_NUMERO, COL_ANO, COL_LINK_ORIG, COL_LINK_ATU}
missing = [c for c in needed if c not in base.columns]
if missing:
    st.error(f"Colunas ausentes no CSV: {missing}")
    st.stop()

# normaliza e mantém metadados úteis
base[COL_TIPO] = base[COL_TIPO].astype(str).str.upper().str.strip()
base[COL_NUMERO] = pd.to_numeric(base[COL_NUMERO], errors="coerce")
base[COL_ANO] = pd.to_numeric(base[COL_ANO], errors="coerce")
base = base.dropna(subset=[COL_TIPO, COL_NUMERO, COL_ANO])
base[COL_NUMERO] = base[COL_NUMERO].astype(int)
base[COL_ANO] = base[COL_ANO].astype(int)

# remove duplicatas (por tipo/numero/ano)
base = base.drop_duplicates(subset=[COL_TIPO, COL_NUMERO, COL_ANO]).reset_index(drop=True)

st.markdown(f"✅ Normas carregadas: **{len(base)}**")

# mostra preview
with st.expander("👀 Ver prévia (metadados)", expanded=False):
    st.dataframe(base.head(50), use_container_width=True)

# =================================================
# TABELA FINAL
# =================================================
def montar_final(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tipo_sigla"] = out[COL_TIPO]
    out["numero"] = out[COL_NUMERO]
    out["ano"] = out[COL_ANO]
    out["url_portal"] = out.apply(lambda r: url_portal(r["tipo_sigla"], r["numero"], r["ano"]), axis=1)

    out["url_original"] = out[COL_LINK_ORIG].fillna("").astype(str)
    out["url_atualizado"] = out[COL_LINK_ATU].fillna("").astype(str)

    out["texto_original"] = ""
    out["texto_atualizado"] = ""
    out["status_original"] = ""
    out["status_atualizado"] = ""
    out["hash_original"] = ""
    out["hash_atualizado"] = ""
    out["coletado_em"] = ""
    return out

final = montar_final(base)

st.subheader("🚀 Coletar textos (usando links do CSV)")
col1, col2, col3, col4 = st.columns([1.2, 1.2, 2, 1])
coletar_orig = col1.checkbox("Coletar original", value=True)
coletar_atu = col2.checkbox("Coletar atualizado", value=True)
limite = col3.number_input("Limite por execução", min_value=1, max_value=10000, value=min(300, len(final)))
amostra_debug = col4.checkbox("Amostra 30 (debug)", value=False)

if st.button("▶️ Iniciar coleta"):
    if not coletar_orig and not coletar_atu:
        st.warning("Marque original e/ou atualizado.")
        st.stop()

    df = final.copy()
    if amostra_debug:
        df = df.head(30).reset_index(drop=True)
    else:
        df = df.head(int(limite)).reset_index(drop=True)

    barra = st.progress(0)
    info = st.empty()
    t0 = time.time()

    for i, row in df.iterrows():
        if coletar_orig:
            txt, code = fetch_texto_por_link(row["url_original"])
            df.at[i, "texto_original"] = txt
            df.at[i, "status_original"] = str(code)
            df.at[i, "hash_original"] = sha256_text(txt) if txt else ""

        if coletar_atu:
            txt, code = fetch_texto_por_link(row["url_atualizado"])
            df.at[i, "texto_atualizado"] = txt
            df.at[i, "status_atualizado"] = str(code)
            df.at[i, "hash_atualizado"] = sha256_text(txt) if txt else ""

        df.at[i, "coletado_em"] = now_iso()

        if (i + 1) % 10 == 0:
            elapsed = time.time() - t0
            info.write(f"Processadas **{i+1}/{len(df)}** | tempo: **{elapsed/60:.1f} min**")

        barra.progress((i + 1) / len(df))

    st.success("✅ Coleta finalizada para o lote.")
    st.dataframe(df.head(50), use_container_width=True)

    # Download CSV final
    buff = BytesIO()
    df.to_csv(buff, index=False, encoding="utf-8-sig")
    st.download_button(
        "⬇️ Baixar CSV final (com textos)",
        data=buff.getvalue(),
        file_name=f"LegislacaoMineira_{'_'.join(map(str, sorted(set(df['ano']))))}_com_textos.csv",
        mime="text/csv"
    )

    # Download gz opcional
    buff_gz = BytesIO()
    df.to_csv(buff_gz, index=False, encoding="utf-8-sig", compression="gzip")
    st.download_button(
        "⬇️ Baixar CSV final (gzip)",
        data=buff_gz.getvalue(),
        file_name=f"LegislacaoMineira_{'_'.join(map(str, sorted(set(df['ano']))))}_com_textos.csv.gz",
        mime="application/gzip"
    )
