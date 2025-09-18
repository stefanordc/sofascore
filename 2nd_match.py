import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import re
import pandas as pd
import pymysql
import logging

# Configurações de conexão ao MySQL
conn = pymysql.connect(host='localhost', user='admin', password='1234', database='bet_dados')
cursor = conn.cursor()

# Suprime logs do Selenium
options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging'])
logging.getLogger('selenium').setLevel(logging.CRITICAL)

# Dicionário de substituição de nomes dos clubes
club_name_mapping = {
    "Atlético-MG": "Atlético"
}

# Função para extrair o id_jogo da URL
def extract_id_jogo(url: str) -> str:
    match = re.search(r'id:(\d+)', url)
    if match:
        return match.group(1)
    raise ValueError("id_jogo não encontrado na URL.")

# Obter o URL de estatísticas
def get_statistics_url(match_url: str) -> str:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(match_url)
        time.sleep(2)

        current_url = driver.current_url
        m = re.search(r'id:(\d+)', current_url)

        if not m:
            elems = driver.find_elements(By.CSS_SELECTOR, '[data-event-id]')
            if elems:
                m = re.search(r'\d+', elems[0].get_attribute('data-event-id'))

        if m:
            event_id = m.group(1)
            return f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
        else:
            raise ValueError("Não foi possível localizar o eventId.")
    finally:
        driver.quit()

# Função para obter estatísticas em formato JSON
def fetch_statistics_json(api_url: str) -> dict:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get(api_url)
        time.sleep(2)

        pre_tag_content = driver.find_element(By.TAG_NAME, "pre").text
        return json.loads(pre_tag_content)

    finally:
        driver.quit()

# Extrai nomes dos clubes diretamente da página do jogo
def get_club_names(match_url: str) -> tuple:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(match_url)
        time.sleep(2)

        clubes = driver.find_elements(By.CSS_SELECTOR, 'div.d_flex.flex-d_column.ai_center bdi')
        if len(clubes) >= 2:
            home = clubes[0].text.strip()
            away = clubes[1].text.strip()
            home = club_name_mapping.get(home, home)
            away = club_name_mapping.get(away, away)
            return home, away
        else:
            raise ValueError("Nomes dos clubes não encontrados")

    finally:
        driver.quit()

# Extrair estatísticas e preparar DataFrame
def extract_statistics(data: dict, period: str, id_jogo: str, home_club: str, away_club: str) -> pd.DataFrame:
    period_stats = next(item for item in data["statistics"] if item["period"] == period)

    home_stats = {"id_jogo": id_jogo, "clube": home_club}
    away_stats = {"id_jogo": id_jogo, "clube": away_club}

    for group in period_stats["groups"]:
        for item in group["statisticsItems"]:
            stat_name = item["name"]
            home_stats[stat_name] = item["home"]
            away_stats[stat_name] = item["away"]

    df = pd.DataFrame([home_stats, away_stats])

    return df

# Execução principal
if __name__ == "__main__":
    match_link = "https://www.sofascore.com/pt/football/match/atletico-mineiro-internacional/qOsCO#id:13472737"
    id_jogo = extract_id_jogo(match_link)

    cursor.execute("SELECT COUNT(*) FROM 2_tempo WHERE id_jogo = %s", (id_jogo,))
    exists = cursor.fetchone()[0]

    if exists == 0:
        home_club, away_club = get_club_names(match_link)
        api_link = get_statistics_url(match_link)

        stats_data = fetch_statistics_json(api_link)
        df_2nd = extract_statistics(stats_data, "2ND", id_jogo, home_club, away_club)

        cols = ','.join([f"`{col}`" for col in df_2nd.columns])
        vals = ','.join(['%s'] * len(df_2nd.columns))
        insert_query = f"INSERT INTO 2_tempo ({cols}) VALUES ({vals})"

        for i in range(len(df_2nd)):
            cursor.execute(insert_query, tuple(df_2nd.iloc[i]))
        conn.commit()

        print("Dados inseridos com sucesso:")
        print(df_2nd)
    else:
        print("O jogo já existe no banco de dados.")

# Fecha conexão
cursor.close()
conn.close()