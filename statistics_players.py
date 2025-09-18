import json
import pandas as pd
import re
import pymysql
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Variável com o URL do jogo
URL = "https://www.sofascore.com/pt/football/match/atletico-mineiro-internacional/qOsCO#id:13472737"

# Extrair ID do jogo a partir do URL
id_jogo = re.search(r'#id:(\d+)', URL).group(1)

# Configuração Selenium para obter JSON
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=options)

# Obter JSON do lineup dinamicamente
api_url = f"https://www.sofascore.com/api/v1/event/{id_jogo}/lineups"
driver.get(api_url)
content = driver.find_element("tag name", "pre").text

# Obter nomes dos times da página do jogo
match_url = URL.split("#")[0]
driver.get(match_url)
teams = driver.find_elements("css selector", "div.d_flex.flex-d_column.ai_center bdi")
home_team = teams[0].text.strip()
away_team = teams[1].text.strip()
driver.quit()

# Salvar JSON obtido
data = json.loads(content)
with open("lineups.json", "w", encoding="utf-8") as file:
    json.dump(data, file, ensure_ascii=False, indent=4)

# Função para extrair as informações especificadas
def extract_player_info(player_data):
    player_info = player_data.get('player', {})
    statistics = player_data.get('statistics', {})

    extracted_info = {
        'id_jogo': int(id_jogo),
        'team': home_team if player_data.get('team') == 'home' else away_team,
        'name': player_info.get('name'),
        'position': player_info.get('position'),
        'jersey_number': player_info.get('jerseyNumber'),
        'height': int(player_info.get('height') or 0),
        'player_id': player_info.get('id'),
        'date_of_birth_timestamp': player_info.get('dateOfBirthTimestamp'),
        'country': player_info.get('country', {}).get('name'),
        'value': player_info.get('proposedMarketValueRaw', {}).get('value'),
        'substitute': player_data.get('substitute'),
        'captain': player_data.get('captain'),
        'rating': statistics.get('ratingVersions', {}).get('original'),
    }

    for key, value in statistics.items():
        if key != 'ratingVersions':
            key_snake = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
            extracted_info[key_snake] = value

    return extracted_info

# Extrair informações dos jogadores
all_players = []
for team in ['home', 'away']:
    players = data.get(team, {}).get('players', [])
    for player_data in players:
        player_data['team'] = team
        player_info = extract_player_info(player_data)
        all_players.append(player_info)

# Criar DataFrame
df_players = pd.DataFrame(all_players)

# Padronizar nomes das colunas
df_players.columns = [col.lower().replace(" ", "_") for col in df_players.columns]

# Tipos específicos
float_cols = ['goals_prevented', 'expected_goals', 'expected_assists', 'rating']
int_cols = [col for col in df_players.columns if col not in float_cols + ['id_jogo', 'name', 'position', 'country', 'team']]

# Converter tipos
for col in float_cols:
    if col in df_players.columns:
        df_players[col] = pd.to_numeric(df_players[col], errors='coerce')

for col in int_cols:
    if col in df_players.columns:
        df_players[col] = pd.to_numeric(df_players[col], errors='coerce').fillna(0).astype(int)

# Reorganizar colunas
cols_order = ['id_jogo', 'team'] + [col for col in df_players.columns if col not in ['id_jogo', 'team']]
df_players = df_players[cols_order]

# Inserir no MySQL
conn = pymysql.connect(host='localhost', user='admin', password='1234', database='bet_dados')
cursor = conn.cursor()

columns = ', '.join(df_players.columns)
placeholders = ', '.join(['%s'] * len(df_players.columns))
insert_query = f"INSERT INTO estatisticas_jogadores ({columns}) VALUES ({placeholders})"

for _, row in df_players.iterrows():
    values = tuple(None if pd.isna(x) else x for x in row)
    cursor.execute(insert_query, values)

conn.commit()
cursor.close()
conn.close()

print("✅ Dados inseridos com sucesso no banco de dados.")
