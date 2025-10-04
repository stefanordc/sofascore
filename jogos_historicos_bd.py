# -*- coding: utf-8 -*-
"""
Importa 'copa_do_brasil.csv' para MySQL 'jogos_historicos' com detecção robusta de encoding/sep
e normalização de colunas.
"""

import os
import re
import unicodedata
from datetime import datetime
import pandas as pd
import pymysql
from pymysql.constants import CLIENT

# ===== CONFIG =====
CSV_PATH = r"C:\Users\stefa\Desktop\bet_dados\banco_historico_jogos\Brasil\copa_do_brasil.csv"

DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "admin"
DB_PASSWORD = "1234"
DB_NAME = "bet_dados"

CHUNK_SIZE = 20_000

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS jogos_historicos (
  id INT AUTO_INCREMENT PRIMARY KEY,
  data DATE NOT NULL,
  campeonato VARCHAR(100) NOT NULL,
  temporada VARCHAR(15) NOT NULL,
  casa VARCHAR(100) NOT NULL,
  gols_casa INT NOT NULL,
  gols_fora INT NOT NULL,
  fora VARCHAR(100) NOT NULL,
  cadastrado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_UNIQUE = """
ALTER TABLE jogos_historicos
ADD UNIQUE KEY uq_jogo_unico (data, campeonato, temporada, casa, fora, gols_casa, gols_fora);
"""

INSERT_SQL = """
INSERT IGNORE INTO jogos_historicos
  (data, campeonato, temporada, casa, gols_casa, gols_fora, fora, cadastrado_em)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

_expected = ["data","campeonato","temporada","casa","gols_casa","gols_fora","fora"]
_printed_mapping_once = False


# ---------- DB ----------
def connect():
    print(f"-> Conectando em {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME} …")
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        client_flag=CLIENT.MULTI_STATEMENTS,
    )

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_CREATE)
        try:
            cur.execute(DDL_UNIQUE)
        except Exception as e:
            if "Duplicate key name" not in str(e):
                raise
    conn.commit()


# ---------- CSV helpers ----------
def normalize_token(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\ufeff","")  # BOM
    s = s.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def map_columns(cols):
    norm_cols = [normalize_token(c) for c in cols]
    candidates = {
        "data": {"data","dt","date"},
        "campeonato": {"campeonato","competicao","competicao_nome","torneio"},
        "temporada": {"temporada","season","ano"},
        "casa": {"casa","mandante","home","time_casa","time_mandante"},
        "gols_casa": {"gols_casa","gols_mandante","placar_casa","gols_home","home_goals"},
        "gols_fora": {"gols_fora","gols_visitante","placar_fora","gols_away","away_goals"},
        "fora": {"fora","visitante","away","time_fora","time_visitante"},
    }
    mapping, taken = {}, set()
    for target, keys in candidates.items():
        idx = None
        for i, n in enumerate(norm_cols):
            if i in taken: 
                continue
            if n == target or n in keys:
                idx = i; break
        if idx is None:  # fallback: contém token principal
            key_token = target.split("_")[0]
            for i, n in enumerate(norm_cols):
                if i in taken: 
                    continue
                if key_token in n:
                    idx = i; break
        if idx is not None:
            mapping[cols[idx]] = target
            taken.add(idx)
    return mapping

def smart_choose_sep_by_header(path):
    """Lê a primeira linha e decide o separador por contagem de candidatos."""
    with open(path, "rb") as f:
        head = f.read(4096)
    for enc in ("utf-8-sig","cp1252","latin1"):
        try:
            s = head.decode(enc, errors="ignore")
            break
        except Exception:
            continue
    s_line = s.splitlines()[0] if s else ""
    counts = {",": s_line.count(","), ";": s_line.count(";"), "\t": s_line.count("\t"), "|": s_line.count("|")}
    sep = max(counts, key=counts.get)
    # heurística: se só um deles aparece (>0), usa ele
    if counts[sep] > 0:
        return sep
    return ","  # default

def detect_csv_format(path):
    # 1) tenta decidir separador pelo header
    sep_hint = smart_choose_sep_by_header(path)
    encodings = ["utf-8-sig","utf-8","cp1252","latin1"]
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc, sep=sep_hint, nrows=20, engine="python")
            print(f"-> Formato detectado: encoding='{enc}' sep='{sep_hint}'")
            return enc, sep_hint
        except Exception:
            continue
    # fallback
    print("-> Formato não detectado; usando latin1 e ';'")
    return "latin1",";"

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    global _printed_mapping_once

    # Se veio 1 coluna só (sintoma clássico de sep errado) e o header contém ';', vamos reprocessar acima no main.
    # Aqui assumimos que já veio correto.

    # CSV sem header e com 7 colunas
    if (all(str(c).startswith("Unnamed") or isinstance(c, int) for c in df.columns) and len(df.columns) == 7):
        df.columns = _expected
        if not _printed_mapping_once:
            print("-> CSV sem header: atribuídos nomes esperados:", _expected)
            _printed_mapping_once = True
        return df

    mapping = map_columns(list(df.columns))
    df = df.rename(columns=mapping)

    if not _printed_mapping_once:
        print("-> Colunas no CSV:", list(df.columns))
        if mapping:
            print("-> Mapeamento aplicado:", mapping)
        else:
            print("-> Nenhum mapeamento aplicado (já padronizadas).")
        _printed_mapping_once = True

    missing = [c for c in _expected if c not in df.columns]
    if missing:
        raise ValueError(f"CSV sem colunas esperadas: {missing}\nVistas: {list(df.columns)}")
    return df

def normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    for c in ["gols_casa","gols_fora"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df["campeonato"] = df["campeonato"].astype(str).str.slice(0,100)
    df["temporada"]  = df["temporada"].astype(str).str.slice(0,15)
    df["casa"]       = df["casa"].astype(str).str.slice(0,100)
    df["fora"]       = df["fora"].astype(str).str.slice(0,100)
    df = df.dropna(subset=["data"]).copy()
    df["cadastrado_em"] = datetime.now()
    return df[["data","campeonato","temporada","casa","gols_casa","gols_fora","fora","cadastrado_em"]]

def insert_dataframe(conn, df: pd.DataFrame) -> int:
    rows = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    conn.commit()
    return cur.rowcount


# ---------- main ----------
def main():
    enc, sep = detect_csv_format(CSV_PATH)

    conn = connect()
    try:
        ensure_table(conn)
        print(f"-> Lendo: {CSV_PATH}")

        total_csv, total_ins = 0, 0
        # 1ª tentativa com enc/sep detectados
        for chunk in pd.read_csv(
            CSV_PATH, encoding=enc, sep=sep, chunksize=CHUNK_SIZE,
            engine="python", on_bad_lines="skip", dtype_backend="numpy_nullable"
        ):
            # Se veio 1 coluna só (erro de separador), re-le com ';'
            if len(chunk.columns) == 1:
                print("-> Detectado 1 coluna (header colado). Recarregando com sep=';'.")
                total_csv = 0; total_ins = 0
                for chunk2 in pd.read_csv(
                    CSV_PATH, encoding=enc, sep=";", chunksize=CHUNK_SIZE,
                    engine="python", on_bad_lines="skip", dtype_backend="numpy_nullable"
                ):
                    total_csv += len(chunk2)
                    df_norm = normalize_chunk(chunk2)
                    ins = insert_dataframe(conn, df_norm)
                    total_ins += ins
                    print(f"   - Processadas {len(df_norm)} | inseridas (novas): {ins}")
                break  # já tratamos tudo com ';'
            else:
                total_csv += len(chunk)
                df_norm = normalize_chunk(chunk)
                ins = insert_dataframe(conn, df_norm)
                total_ins += ins
                print(f"   - Processadas {len(df_norm)} | inseridas (novas): {ins}")

        print(f"\n✅ Concluído. Lidas: {total_csv} | Inseridas (novas): {total_ins} (duplicatas ignoradas).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
