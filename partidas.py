import pandas as pd
import pymysql
import json
import datetime
import re
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# ========== DICIONÁRIOS DE SUBSTITUIÇÃO ==========
substituicoes = {
    'Atlético Mineiro': 'Atlético',
    'Atlético-MG': 'Atlético',
    'Maringá FC': 'Maringá'
}

substituicoes_campeonatos = {
    'Brasileirão Betano': 'Brasileiro',
    'Copa Betano do Brasil': 'Copa do Brasil'
}

# ========== CONEXÃO COM BANCO ==========
conn = pymysql.connect(host='localhost', user='admin', password='1234', database='bet_dados')
cursor = conn.cursor()

# ========== CONFIGURAÇÃO DO SELENIUM ==========
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_experimental_option('excludeSwitches', ['enable-logging'])
logging.getLogger('selenium').setLevel(logging.CRITICAL)
driver = webdriver.Chrome(options=options)

# ========== URL E ID DO JOGO ==========
url = "https://www.sofascore.com/pt/football/match/fortaleza-fluminense/lOsvP#id:13473346"
id_jogo = re.search(r'#id:(\d+)', url).group(1)

# ========== NOMES DOS CLUBES ==========
def substituir_nome(nome):
    return substituicoes.get(nome.strip(), nome.strip())

def substituir_campeonato(nome):
    return substituicoes_campeonatos.get(nome.strip(), nome.strip())

def obter_nomes_clubes():
    driver.get(url)
    time.sleep(3)
    elementos = driver.find_elements(By.CSS_SELECTOR, 'div.d_flex.flex-d_column.ai_center bdi')
    if len(elementos) >= 2:
        casa = substituir_nome(elementos[0].text)
        fora = substituir_nome(elementos[1].text)
    else:
        casa = 'NÃO ENCONTRADO'
        fora = 'NÃO ENCONTRADO'
    return casa, fora

home, away = obter_nomes_clubes()

# ========== FUNÇÕES AUXILIARES ==========
def obter_evento(id_jogo):
    driver.get(f'https://api.sofascore.com/api/v1/event/{id_jogo}')
    return json.loads(driver.find_element(By.TAG_NAME, 'pre').text)['event']

# ========== EXIBIÇÃO DOS DADOS (sem inserir no banco) ==========
cursor.execute("SELECT COUNT(*) FROM partidas WHERE id_jogo = %s", (id_jogo,))
if cursor.fetchone()[0] == 0:
    evento = obter_evento(id_jogo)
    dt = datetime.datetime.fromtimestamp(evento['startTimestamp'], tz=datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=-3)))
    data, hora = dt.strftime('%d/%m/%Y'), dt.strftime('%H:%M')
    campeonato = substituir_campeonato(evento['tournament']['name'])
    rodada = re.search(r'\d+', evento['roundInfo'].get('name', '') or str(evento['roundInfo'].get('round', ''))).group()
    gols_casa = evento['homeScore']['current']
    gols_fora = evento['awayScore']['current']

    df = pd.DataFrame([{
        'id_jogo': id_jogo,
        'data': data,
        'hora': hora,
        'campeonato': campeonato,
        'rodada': rodada,
        'casa': home,
        'fora': away,
        'gols_casa': gols_casa,
        'gols_fora': gols_fora
    }])

    print("\n✅ Dados que seriam inseridos no banco de dados:")
    print(df)

# ========== FINALIZA ==========
driver.quit()
cursor.close()
conn.close()
print("\n✅ Processo finalizado.")
