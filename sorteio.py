"""
Flashscore – scraper de mandante, visitante, data e odds 1x2
Autor: Tefin | jun/2025
"""

# ======================= IMPORTS & CONFIG =======================
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

MATCH_ID = "rXVNTUHF"   # ID da partida na URL
HEADLESS = True          # False se quiser ver a janela

BASE        = f"https://www.flashscore.com/match/{MATCH_ID}"
URL_SUMMARY = BASE + "/#/match-summary/match-summary"
URL_ODDS    = BASE + "/#/odds-comparison/1x2-odds/full-time"

# --------- mata o WinError 6: destruidor “mudo” ----------
def _silent_del(self):        # substitui o __del__ original
    try:
        self.quit()
    except Exception:
        pass
uc.Chrome.__del__ = _silent_del
# -------------------------------------------------------------

# --------------- abre o Chrome camuflado ----------------
opts = uc.ChromeOptions()
opts.headless = HEADLESS
opts.add_argument("--window-size=1920,1080")
opts.add_argument(
    "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

with uc.Chrome(options=opts) as driver:
    wait = WebDriverWait(driver, 25)

    # =================== “SUMMARY” ====================
    driver.get(URL_SUMMARY)

    # Aceita cookies se surgir
    try:
        wait.until(EC.element_to_be_clickable(
            (By.ID, "onetrust-accept-btn-handler"))
        ).click()
    except TimeoutException:
        pass

    home_team = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR,
         "div.duelParticipant__home .participant__participantName"))
    ).text.strip()

    away_team = driver.find_element(
        By.CSS_SELECTOR,
        "div.duelParticipant__away .participant__participantName"
    ).text.strip()

    # ---- data (dia + hora) ----
    match_date = "-"
    try:
        # O horário aparece dentro de div.duelParticipant__startTime :contentReference[oaicite:3]{index=3}
        date_box = driver.find_element(
            By.CSS_SELECTOR, "div.duelParticipant__startTime")
        match_date = date_box.text.strip() or "-"
    except Exception:
        # fallback: alguns jogos carregam o unix-timestamp em window.tudate :contentReference[oaicite:4]{index=4}
        try:
            ts = driver.execute_script("return window.tudate || null;")
            if ts:
                from datetime import datetime
                match_date = datetime.utcfromtimestamp(int(ts)).strftime(
                    "%d/%m/%Y %H:%M")
        except Exception:
            pass

    # =================== “ODDS” =======================
    driver.get(URL_ODDS)
    wait.until(lambda d: len(d.find_elements(
        By.CSS_SELECTOR, "div.ui-table__body a.oddsCell__odd")) >= 3
    )
    odd_cells = driver.find_elements(
        By.CSS_SELECTOR, "div.ui-table__body a.oddsCell__odd")[:3]
    odd_1, odd_x, odd_2 = [c.text.strip() or "-" for c in odd_cells]

# =================== RESULTADO NO CONSOLE ===================
print(f"Data do jogo:  {match_date}")
print(f"Mandante:      {home_team}")
print(f"Visitante:     {away_team}")
print(f"Odd 1 (casa):  {odd_1}")
print(f"Odd X (empate):{odd_x}")
print(f"Odd 2 (fora):  {odd_2}")
