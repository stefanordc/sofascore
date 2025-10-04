import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np

caminho_csv = r"C:\Users\stefa\Desktop\backup_partidas.csv"

try:
    conexao = mysql.connector.connect(
        host='localhost',
        user='admin',
        password='1234',
        database='bet_dados'
    )

    if conexao.is_connected():
        print("✅ Conectado ao MySQL")

        df = pd.read_csv(caminho_csv, sep=';')
        print("📄 Colunas do CSV:", df.columns.tolist())

        colunas = [
            'id', 'temporada', 'id_jogo', 'data', 'hora', 'campeonato', 'pais', 'rodada',
            'casa', 'fora', 'gols_casa', 'gols_fora',
            'odd_casa', 'odd_empate', 'odd_fora', 'cadastrado_em'
        ]

        df = df[colunas]

        # Trata NaN e 'nan' como None
        df = df.replace({np.nan: None, 'nan': None, 'NaN': None, '': None})

        # Converte 'cadastrado_em' para datetime no formato aceito pelo MySQL
        df['cadastrado_em'] = pd.to_datetime(df['cadastrado_em'], format='%d/%m/%Y %H:%M', errors='coerce')
        df['cadastrado_em'] = df['cadastrado_em'].dt.strftime('%Y-%m-%d %H:%M:%S')

        cursor = conexao.cursor()

        query = """
            INSERT INTO partidas (
                id, temporada, id_jogo, data, hora, campeonato, pais, rodada,
                casa, fora, gols_casa, gols_fora,
                odd_casa, odd_empate, odd_fora, cadastrado_em
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                temporada = VALUES(temporada),
                id_jogo = VALUES(id_jogo),
                data = VALUES(data),
                hora = VALUES(hora),
                campeonato = VALUES(campeonato),
                pais = VALUES(pais),
                rodada = VALUES(rodada),
                casa = VALUES(casa),
                fora = VALUES(fora),
                gols_casa = VALUES(gols_casa),
                gols_fora = VALUES(gols_fora),
                odd_casa = VALUES(odd_casa),
                odd_empate = VALUES(odd_empate),
                odd_fora = VALUES(odd_fora),
                cadastrado_em = VALUES(cadastrado_em)
        """

        for idx, row in df.iterrows():
            try:
                cursor.execute(query, tuple(row))
            except Error as e:
                print(f"❌ Erro na linha {idx}: {e}")
                print("🔍 Linha com erro:", row.to_dict())
                break

        conexao.commit()
        print("✅ Todos os dados foram inseridos com sucesso!")

except Error as e:
    print(f"❌ Erro ao conectar ou inserir: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if conexao.is_connected():
        conexao.close()
        print("🔒 Conexão encerrada.")
