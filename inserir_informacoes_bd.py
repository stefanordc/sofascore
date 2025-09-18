import os
import math
import datetime as dt
import logging
from typing import Iterable, Tuple, List, Optional

import pandas as pd
import pymysql

# ========================= CONFIGURAÇÕES ========================= #
CAMINHO_CSV = r"C:\Pessoal\Data_science\Projetos\sofascore\tempo_gols.csv"

DB_CONFIG = dict(
    host="localhost",
    user="admin",
    password="1234",
    database="bet_dados",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.Cursor,
    autocommit=False,
)

TABELA_ALVO = "tempo_gols"
CHUNK_SIZE = 5_000  # tamanho do lote para INSERT

# Colunas esperadas no CSV (sem o id auto_increment)
COLS_ESPERADAS = [
    "id_jogo",
    "temporada",
    "campeonato",
    "clube_gol",
    "minutos",
    "forma_gol",
    "placar",
]
COL_DATA = "cadastrado_em"  # timestamp no banco (opcional no CSV)

# Tentar detectar encoding com chardet, se disponível
TENTAR_DETECTAR_ENCODING = True

# ============================ LOGGING ============================ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ====================== FUNÇÕES AUXILIARES ====================== #
def _candidatos_encoding(caminho: str) -> List[str]:
    candidatos = ["utf-8", "iso-8859-1", "latin-1", "cp1252"]
    if not TENTAR_DETECTAR_ENCODING:
        return candidatos
    try:
        import chardet  # type: ignore
        with open(caminho, "rb") as f:
            raw = f.read(200_000)
        det = chardet.detect(raw)
        enc = (det.get("encoding") or "").lower()
        logging.info(f"chardet sugeriu encoding: {enc} (confidence={det.get('confidence')})")
        if enc:
            if enc in candidatos:
                candidatos.remove(enc)
            candidatos.insert(0, enc)
    except Exception as e:
        logging.warning(f"Não foi possível usar chardet: {e}")
    return candidatos


def _sniff_delimitador(caminho: str, encoding: str) -> Optional[str]:
    try:
        with open(caminho, "r", encoding=encoding, errors="replace") as f:
            header = f.readline()
        candidatos = [";", ",", "\t", "|"]
        contagens = {sep: header.count(sep) for sep in candidatos}
        melhor = max(contagens, key=contagens.get)
        return melhor if contagens[melhor] > 0 else None
    except Exception as e:
        logging.warning(f"Falha no sniff de delimitador: {e}")
        return None


def ler_csv_inteligente(caminho: str) -> pd.DataFrame:
    erros = []
    for enc in _candidatos_encoding(caminho):
        try:
            logging.info(f"Tentando ler CSV com encoding='{enc}' (sep=None, engine='python')...")
            df = pd.read_csv(caminho, encoding=enc, engine="python", sep=None)
            if df.shape[1] == 1:
                header_str = str(df.columns[0])
                if any(sep in header_str for sep in [";", ",", "\t", "|"]):
                    sep = _sniff_delimitador(caminho, enc) or ";"
                    logging.info(f"Apenas 1 coluna detectada. Recarregando com sep='{sep}'...")
                    df = pd.read_csv(caminho, encoding=enc, engine="python", sep=sep)
            logging.info(f"CSV OK. Linhas: {len(df)} | Colunas: {list(df.columns)}")
            return df
        except Exception as e:
            erros.append((enc, str(e)))
            logging.warning(f"Falha ao ler com '{enc}': {e}")

    msg = "Falha ao ler CSV. Tentativas:\n" + "\n".join([f"- {enc}: {err}" for enc, err in erros])
    raise RuntimeError(msg)


def limpar_valor(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    return df


def validar_e_ajustar_schema_csv(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    if COL_DATA not in cols:
        df[COL_DATA] = dt.datetime.now()
    else:
        df[COL_DATA] = pd.to_datetime(df[COL_DATA], errors="coerce").fillna(dt.datetime.now())

    faltantes = [c for c in COLS_ESPERADAS if c not in cols]
    if faltantes:
        raise ValueError(f"CSV está faltando colunas: {faltantes}. Colunas encontradas: {list(df.columns)}")

    # Conversões úteis
    if "minutos" in df.columns:
        df["minutos"] = pd.to_numeric(df["minutos"], errors="coerce").astype("Int64")
    if "id_jogo" in df.columns:
        df["id_jogo"] = pd.to_numeric(df["id_jogo"], errors="coerce").astype("Int64")

    # Ordena as colunas na ordem de inserção
    colunas_final = COLS_ESPERADAS + [COL_DATA]
    df = df[colunas_final]
    df = df.astype(object)
    return df


def preparar_tuplas(df: pd.DataFrame) -> List[Tuple]:
    return [tuple(limpar_valor(v) for v in linha) for linha in df.itertuples(index=False, name=None)]


def inserir_em_lotes(cursor, query: str, dados: Iterable[Tuple], chunk_size: int) -> int:
    total_inseridas = 0
    buf: List[Tuple] = []
    for t in dados:
        buf.append(t)
        if len(buf) >= chunk_size:
            cursor.executemany(query, buf)
            total_inseridas += cursor.rowcount
            buf.clear()
    if buf:
        cursor.executemany(query, buf)
        total_inseridas += cursor.rowcount
    return total_inseridas


def assert_id_auto_increment(conn, tabela: str = TABELA_ALVO, coluna: str = "id"):
    """
    Garante que `id` é NOT NULL AUTO_INCREMENT PRIMARY KEY.
    Se não for, levanta erro com instruções de correção (SQL).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_KEY, EXTRA
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
              AND column_name = %s
        """, (tabela, coluna))
        row = cur.fetchone()

        if row is None:
            raise RuntimeError(
                f"A tabela `{tabela}` não possui a coluna `{coluna}`.\n"
                f"Crie-a como AUTO_INCREMENT PRIMARY KEY:\n"
                f"  ALTER TABLE {tabela}\n"
                f"    ADD COLUMN {coluna} INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"
            )

        _, is_nullable, col_key, extra = row
        extra = (extra or "").lower()
        col_key = (col_key or "").upper()

        # Checa se já está OK
        if ("auto_increment" in extra) and (col_key == "PRI") and (is_nullable == "NO"):
            return  # tudo certo

        # Monta instruções de correção
        msg = [
            f"A coluna `{coluna}` da tabela `{tabela}` não é AUTO_INCREMENT PRIMARY KEY.",
            "Execute UMA das opções abaixo (conforme seu caso):",
            "",
            "Opção A — tabela sem PK ou `id` sem PK:",
            f"  ALTER TABLE {tabela}\n"
            f"    MODIFY COLUMN {coluna} INT NOT NULL,\n"
            f"    ADD PRIMARY KEY ({coluna}),\n"
            f"    MODIFY COLUMN {coluna} INT NOT NULL AUTO_INCREMENT;",
            "",
            "Opção B — `id` já é PK, só falta AUTO_INCREMENT:",
            f"  ALTER TABLE {tabela}\n"
            f"    MODIFY COLUMN {coluna} INT NOT NULL AUTO_INCREMENT;",
            "",
            "Opção C — existe outra PK (não é `id`):",
            f"  ALTER TABLE {tabela} DROP PRIMARY KEY, ADD PRIMARY KEY ({coluna});",
            f"  ALTER TABLE {tabela} MODIFY COLUMN {coluna} INT NOT NULL AUTO_INCREMENT;",
        ]
        raise RuntimeError("\n".join(msg))


# ============================== MAIN ============================== #
def main():
    if not os.path.isfile(CAMINHO_CSV):
        raise FileNotFoundError(f"CSV não encontrado: {CAMINHO_CSV}")

    # 1) Ler CSV
    df = ler_csv_inteligente(CAMINHO_CSV)
    df = normalizar_colunas(df)
    df = validar_e_ajustar_schema_csv(df)

    # 2) Conectar e verificar schema
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        assert_id_auto_increment(conn, TABELA_ALVO, "id")

        # 3) Preparar dados e inserir
        dados = preparar_tuplas(df)
        with conn.cursor() as cur:
            colunas = df.columns.tolist()
            cols_sql = ", ".join(f"`{c}`" for c in colunas)
            vals_sql = ", ".join(["%s"] * len(colunas))
            sql = f"INSERT INTO `{TABELA_ALVO}` ({cols_sql}) VALUES ({vals_sql})"

            logging.info(f"Iniciando inserção em '{TABELA_ALVO}'. Linhas: {len(dados)} | Colunas: {colunas}")
            inseridas = inserir_em_lotes(cur, sql, dados, CHUNK_SIZE)
        conn.commit()
        logging.info(f"✅ Importação concluída! Linhas inseridas: {inseridas}")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.exception(f"❌ Erro durante a importação: {e}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
