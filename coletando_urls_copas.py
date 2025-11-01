import re, time, logging, unicodedata
from typing    import List, Set, Dict

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options  import Options
from selenium.webdriver.common.by       import By
from selenium.webdriver.support.ui      import WebDriverWait
from selenium.webdriver.support         import expected_conditions as EC
from selenium.common.exceptions         import (
    ElementClickInterceptedException,
    TimeoutException,
)
from webdriver_manager.chrome import ChromeDriverManager


URL_TORNEIO = (
    "https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325#id:72034"
)
HEADLESS = True
WINDOW_SIZE = "1920,1080"
TIMEOUT = 60

BAN_LIST = ("adiad", "cancel", "postpon", "award")
ID_RE    = re.compile(r"#id:(\d+)")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")


# ─────────── helpers ──────────── #
def criar_driver() -> webdriver.Chrome:
    op = Options()
    if HEADLESS:
        op.add_argument("--headless=new")
    op.add_argument(f"--window-size={WINDOW_SIZE}")
    op.add_argument("--disable-blink-features=AutomationControlled")
    op.add_argument("--log-level=3")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=op)


def fechar_overlays(w: WebDriverWait):
    for xp in (
        "//button[contains(translate(.,'ACEITAR','aceitar'),'aceitar')]",
        "//button[@aria-label='Close' or @aria-label='Fechar']",
    ):
        try:
            w.until(EC.element_to_be_clickable((By.XPATH, xp))).click()
            time.sleep(0.2)
        except Exception:
            pass


def abrir_aba_jogos(w: WebDriverWait):
    try:
        w.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[@role='tab' and (contains(.,'Jogos') or contains(.,'Matches'))]",
                )
            )
        ).click()
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
    raise TimeoutError("Combobox não encontrado.")


def obter_fases(w: WebDriverWait) -> List[str]:
    btn = botao_dropdown(w)
    btn.click()
    fases = [
        li.text.strip()
        for li in w.until(EC.presence_of_all_elements_located((By.XPATH, "//li[@role='option']")))
    ]
    w._driver.execute_script("arguments[0].click();", btn)  # fecha dropdown
    return fases


def normaliza(txt: str) -> str:
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode().lower().strip()


def selecionar_fase(w: WebDriverWait, fase: str):
    """Abre o dropdown, acha a opção pelo texto normalizado e clica via JS."""
    drv = w._driver
    drv.execute_script("window.scrollTo(0,0);")

    btn = botao_dropdown(w)
    drv.execute_script("arguments[0].click();", btn)  # garante abertura mesmo se overlay

    fase_norm = normaliza(fase)
    lis = w.until(EC.presence_of_all_elements_located((By.XPATH, "//li[@role='option']")))

    alvo = None
    for li in lis:
        if normaliza(li.text) == fase_norm:
            alvo = li
            break
    if alvo is None:
        raise TimeoutException(f"Fase '{fase}' não localizada no dropdown.")

    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", alvo)
    drv.execute_script("arguments[0].click();", alvo)
    time.sleep(1)  # give time for the matches list to refresh


def scroll_ate_fim(drv: webdriver.Chrome, cont):
    prev_height = -1
    while True:
        drv.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", cont)
        time.sleep(0.5)
        height = drv.execute_script("return arguments[0].scrollHeight;", cont)
        if height == prev_height:
            break
        prev_height = height


def links_validos(w: WebDriverWait) -> Dict[str, str]:
    cont = w.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-panelid='round'] .list-wrapper"))
    )
    scroll_ate_fim(w._driver, cont)

    links = {}
    for a in cont.find_elements(By.XPATH, ".//a[contains(@href,'/football/match/')]"):
        txt = normaliza(a.text)
        if any(b in txt for b in BAN_LIST):
            continue
        href = a.get_attribute("href")
        if (m := ID_RE.search(href)):
            links[m.group(1)] = href
    return links
# ──────────────────────────────────── #


def main():
    driver = criar_driver()
    wait   = WebDriverWait(driver, TIMEOUT)
    ids_vistos: Set[str] = set()

    try:
        driver.get(URL_TORNEIO)
        fechar_overlays(wait)
        abrir_aba_jogos(wait)

        fases = obter_fases(wait)

        print("LINKS ABAIXO")
        for fase in fases:
            selecionar_fase(wait, fase)
            novos = links_validos(wait)

            print(f"# {fase}")
            for jogo_id, url in novos.items():
                if jogo_id in ids_vistos:
                    continue
                ids_vistos.add(jogo_id)
                print(f"    '{url}',")

        print(f"\nTotal único de links: {len(ids_vistos)}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
