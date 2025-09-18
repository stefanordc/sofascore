import time, logging, unicodedata
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager

URL_TORNEIO    = "https://www.sofascore.com/pt/futebol/2025-09-12"
RODADA_INICIAL = 1
RODADA_FINAL   = 38
HEADLESS       = True
WINDOW_SIZE    = "1920,1080"
TIMEOUT        = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")


# ───── Selenium setup ───── #
def criar_driver() -> webdriver.Chrome:
    op = Options()
    if HEADLESS:
        op.add_argument("--headless=new")
    op.add_argument(f"--window-size={WINDOW_SIZE}")
    op.add_argument("--disable-blink-features=AutomationControlled")
    op.add_argument("--log-level=3")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                            options=op)


def fechar_overlays(w: WebDriverWait):
    for xp in (
        "//button[contains(translate(.,'ACEITAR','aceitar'),'aceitar')]",
        "//button[@aria-label='Close' or @aria-label='Fechar']",
    ):
        try:
            w.until(EC.element_to_be_clickable((By.XPATH, xp))).click()
            time.sleep(.2)
        except Exception:
            pass


def abrir_aba_jogos(w: WebDriverWait):
    try:
        w.until(EC.element_to_be_clickable((
            By.XPATH,
            "//button[@role='tab' and (contains(.,'Jogos') or contains(.,'Matches'))]"
        ))).click()
    except Exception:
        pass


def botao_dropdown(w: WebDriverWait):
    for xp in (
        "//div[@data-panelid='round']//button[@role='combobox']",
        "//div[@data-panelid='round']//button[contains(@class,'DropdownButton')]",
    ):
        try:
            return w.until(EC.element_to_be_clickable((By.XPATH, xp)))
        except Exception:
            continue
    raise TimeoutError("Botão do dropdown não encontrado.")
# ────────────────────────── #


def selecionar_rodada(w: WebDriverWait, rodada: int):
    drv = w._driver
    drv.execute_script("window.scrollTo(0, 0);")
    btn = botao_dropdown(w)
    try:
        btn.click()
    except ElementClickInterceptedException:
        drv.execute_script("arguments[0].click();", btn)

    xp = f"//li[@role='option' and normalize-space()='Rodada {rodada}']"
    alvo = w.until(EC.element_to_be_clickable((By.XPATH, xp)))
    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", alvo)
    drv.execute_script("arguments[0].click();", alvo)
    time.sleep(2)


BAN = ("adiad", "cancel", "conced", "postpon", "abandon", "award")


def links_da_rodada(w: WebDriverWait) -> list[str]:
    """
    Rola o container .list-wrapper até o final
    e devolve todos os links válidos da rodada.
    """
    cont = w.until(EC.presence_of_element_located((
        By.CSS_SELECTOR, "div[data-panelid='round'] .list-wrapper")))

    drv        = w._driver
    seen_links = set()
    last_y     = -1

    while True:
        # coleta âncoras visíveis
        for a in cont.find_elements(By.XPATH, ".//a[contains(@href,'/football/match/')]"):
            txt = unicodedata.normalize("NFKD", a.text).lower()
            if any(b in txt for b in BAN):
                continue
            seen_links.add(a.get_attribute("href"))

        # scroll até o fundo; se altura não mudar, já carregamos tudo
        drv.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", cont)
        time.sleep(.6)
        y = drv.execute_script("return arguments[0].scrollTop;", cont)
        if y == last_y:
            break
        last_y = y

    # volta para o topo (estética)
    drv.execute_script("arguments[0].scrollTop = 0;", cont)
    return sorted(seen_links)


def main():
    drv  = criar_driver()
    wait = WebDriverWait(drv, TIMEOUT)
    try:
        drv.get(URL_TORNEIO)
        fechar_overlays(wait)
        abrir_aba_jogos(wait)

        print("LINKS ABAIXO")
        for rodada in range(RODADA_INICIAL, RODADA_FINAL + 1):
            selecionar_rodada(wait, rodada)
            links = links_da_rodada(wait)

            print(f"# Rodada {rodada}")
            for lk in links:
                print(f"    '{lk}',")

    finally:
        drv.quit()


if __name__ == "__main__":
    main()
