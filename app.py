import os
import re
import time
import glob
import json
import hashlib
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


# =================================================
# CONFIG
# =================================================
st.set_page_config(page_title="Coletor ALMG (CSV por ano + API)", layout="wide")
st.title("📄 Coletor de Textos — ALMG (CSV por ano no GitHub)")
st.caption("Lê automaticamente os CSVs do repositório (LegislacaoMineira_YYYY.csv), baixa textos via API (142/572) e gera uma tabela final com links + textos.")

DATA_DIR = "data_csv"   # pasta no repo
FILE_PATTERN = os.path.join(DATA_DIR, "LegislacaoMineira_*.csv*")  # aceita .csv e .csv.gz

API_ORIGINAL = 142
API_ATUALIZADO = 572

REQUEST_TIMEOUT = 30
MIN_INTERVAL_SECONDS = 1.0  # respeitar 1 req/s (dados abertos)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "streamlit-almg-coletor/1.0"})


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

def url_api(tipo: str, numero: int, ano: int, tipo_doc: int) -> str:
    return f"https://dadosabertos.almg.gov.br/api/v2/legislacao/mineira/{tipo}/{numero}/{ano}/documento?conteudo=true&tipoDoc={tipo_doc}"

_last_req_ts = 0.0
def rate_limited_get(url: str) -> requests.Response:
    global _last_req_ts
    # garante intervalo mínimo entre o fim de uma e o início da outra (regra 1 req/s)
    elapsed = time.time() - _last_req_ts
    if elapsed < MIN_INTERVAL_SECONDS:
        time.sleep(MIN_INTERVAL_SECONDS - elapsed)
    resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    _last_req_ts = time.time()
    return resp

def parse_api_text(resp_json: dict) -> str:
    """
    A API pode devolver o conteúdo em chaves diferentes dependendo do endpoint/versão.
    Então tentamos várias possibilidades.
    """
    if not isinstance(resp_json, dict):
        return ""
    # tentativas comuns
    for key in ["conteudo", "content", "texto", "html", "documento", "body"]:
        v = resp_json.get(key)
        if isinstance(v, str) and len(v.strip()) > 20:
            return v.strip()

    # às vezes vem aninhado
    for key in ["dados", "data", "item", "resultado", "result"]:
        v = resp_json.get(key)
        if isinstance(v, dict):
            t = parse_api_text(v)
            if t:
                return t

    # arrays
    for key in ["itens", "items", "resultados", "results"]:
        arr = resp_json.get(key)
        if isinstance(arr, list) and arr:
            for el in arr:
                if isinstance(el, dict):
                    t = parse_api_text(el)
                    if t:
                        return t

    return ""

def strip_html_if_needed(texto: str) -> str:
    """
    Se a API devolver HTML, converte para texto simples (mantendo quebras).
    Se já for texto, retorna igual.
    """
    if not texto:
        return ""
    # Heurística simples: tem tags?
    if "<" in texto and ">" in texto:
        soup = BeautifulSoup(texto, "html.parser")
        return limpar_texto(soup.get_text("\n", strip=True))
    return limpar_texto(texto)

def fetch_texto_api(tipo: str, numero: int, ano: int, tipo_doc: int) -> tuple[str, int, str]:
    """
    Retorna (texto, status_code, url)
    """
    u = url_api(tipo, numero, ano, tipo_doc)
    try:
        r = rate_limited_get(u)
        status = r.status_code
        if status != 200:
            return "", status, u
        j = r.json()
        raw = parse_api_text(j)
        txt = strip_html_if_needed(raw)
        return txt, status, u
    except Exception:
        return "", 0, u


# =================================================
# CARREGAR CSVs DO REPO
# =================================================
@st.cache_data(show_spinner=False)
def list_year_files():
    files = sorted(glob.glob(FILE_PATTERN))
    out = []
    for f in files:
        # extrai YYYY do nome
        m = re.search(r"LegislacaoMineira_(\d{4})\.csv(\.gz)?$", os.path.basename(f))
        if m:
            out.append((int(m.group(1)), f))
    out.sort(key=lambda x: x[0], reverse=True)
    return out

@st.cache_data(show_spinner=False)
def load_year_csv(path: str) -> pd.DataFrame:
    if path.endswith(".gz"):
        df = pd.read_csv(path, compression="gzip")
    else:
        df = pd.read_csv(path)
    return df


# =================================================
# UI: selecionar anos e carregar base
# =================================================
year_files = list_year_files()
if not year_files:
    st.error(f"Não encontrei arquivos em `{DATA_DIR}` com o padrão `LegislacaoMineira_YYYY.csv`. Crie a pasta e suba os arquivos no repo.")
    st.stop()

anos_disponiveis = [y for y, _ in year_files]
map_ano_arquivo = {y: p for y, p in year_files}

colA, colB = st.columns([2, 1])
anos_sel = colA.multiselect("📅 Selecione ano(s) para carregar", options=anos_disponiveis, default=[anos_disponiveis[0]])
somente_amostra = colB.checkbox("Usar amostra (debug)", value=False, help="Carrega e processa só as primeiras 30 linhas para teste rápido.")

if not anos_sel:
    st.stop()

dfs = []
for a in anos_sel:
    df = load_year_csv(map_ano_arquivo[a])
    dfs.append(df)

base = pd.concat(dfs, ignore_index=True)

# normalização mínima
needed = {"tipo_sigla", "numero", "ano"}
if not needed.issubset(set(base.columns)):
    st.error(f"Os CSVs precisam ter pelo menos estas colunas: {sorted(list(needed))}")
    st.stop()

base = base[["tipo_sigla", "numero", "ano"]].dropna().drop_duplicates()
base["tipo_sigla"] = base["tipo_sigla"].astype(str).str.upper().str.strip()
base["numero"] = base["numero"].astype(int)
base["ano"] = base["ano"].astype(int)

if somente_amostra:
    base = base.head(30)

st.markdown(f"✅ Registros carregados (metadados): **{len(base)}**")

# Monta tabela final “larga”
def montar_tabela_final(df_meta: pd.DataFrame) -> pd.DataFrame:
    df = df_meta.copy()
    df["url_portal"] = df.apply(lambda r: url_portal(r["tipo_sigla"], r["numero"], r["ano"]), axis=1)
    df["url_api_original"] = df.apply(lambda r: url_api(r["tipo_sigla"], r["numero"], r["ano"], API_ORIGINAL), axis=1)
    df["url_api_atualizado"] = df.apply(lambda r: url_api(r["tipo_sigla"], r["numero"], r["ano"], API_ATUALIZADO), axis=1)

    # campos a preencher
    df["texto_original"] = ""
    df["texto_atualizado"] = ""
    df["status_original"] = ""
    df["status_atualizado"] = ""
    df["coletado_em"] = ""
    df["hash_original"] = ""
    df["hash_atualizado"] = ""
    return df

final = montar_tabela_final(base)

st.dataframe(final.head(50), use_container_width=True)

# =================================================
# COLETA
# =================================================
st.subheader("🚀 Coletar textos via Dados Abertos (142=original, 572=atualizado)")

col1, col2, col3 = st.columns([1.2, 1.2, 2])
coletar_original = col1.checkbox("Coletar texto original (142)", value=True)
coletar_atualizado = col2.checkbox("Coletar texto atualizado (572)", value=True)
limite = col3.number_input("Limite de normas a coletar agora (para evitar travas)", min_value=1, max_value=5000, value=min(300, len(final)))

if st.button("▶️ Iniciar coleta"):
    if not coletar_original and not coletar_atualizado:
        st.warning("Marque pelo menos uma opção (original e/ou atualizado).")
        st.stop()

    df = final.copy()
    df = df.head(int(limite)).reset_index(drop=True)

    barra = st.progress(0)
    status = st.empty()
    t0 = time.time()

    for i, row in df.iterrows():
        tipo = row["tipo_sigla"]
        numero = int(row["numero"])
        ano = int(row["ano"])

        if coletar_original:
            txt, code, u = fetch_texto_api(tipo, numero, ano, API_ORIGINAL)
            df.at[i, "texto_original"] = txt
            df.at[i, "status_original"] = str(code)
            df.at[i, "hash_original"] = sha256_text(txt) if txt else ""

        if coletar_atualizado:
            txt, code, u = fetch_texto_api(tipo, numero, ano, API_ATUALIZADO)
            df.at[i, "texto_atualizado"] = txt
            df.at[i, "status_atualizado"] = str(code)
            df.at[i, "hash_atualizado"] = sha256_text(txt) if txt else ""

        df.at[i, "coletado_em"] = now_iso()

        if (i + 1) % 10 == 0:
            elapsed = time.time() - t0
            status.write(f"Processadas **{i+1}/{len(df)}** | tempo decorrido: **{elapsed/60:.1f} min**")

        barra.progress((i + 1) / len(df))

    st.success("✅ Coleta finalizada (para o lote selecionado).")
    st.dataframe(df.head(50), use_container_width=True)

    # Download do CSV final
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding="utf-8-sig")
    st.download_button(
        "⬇️ Baixar CSV final (com textos)",
        data=buffer.getvalue(),
        file_name=f"LegislacaoMineira_{'_'.join(map(str, sorted(set(df['ano']))))}_com_textos.csv",
        mime="text/csv"
    )

    # Também oferece versão compactada (opcional)
    buffer_gz = BytesIO()
    df.to_csv(buffer_gz, index=False, encoding="utf-8-sig", compression="gzip")
    st.download_button(
        "⬇️ Baixar CSV final (gzip)",
        data=buffer_gz.getvalue(),
        file_name=f"LegislacaoMineira_{'_'.join(map(str, sorted(set(df['ano']))))}_com_textos.csv.gz",
        mime="application/gzip"
    )
