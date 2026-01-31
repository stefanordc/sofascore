import re
import time
import logging
import unicodedata
from typing import List, Set

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    ElementClickInterceptedException
)
from webdriver_manager.chrome import ChromeDriverManager

# ──────────────── CONFIGURAÇÕES ──────────────── #
URL_TORNEIO = "https://www.sofascore.com/pt/football/tournament/austria/ofb-cup/445#id:77237"

# Execução oculta (sem abrir janela visual)
HEADLESS = True  
WINDOW_SIZE = "1920,1080"
TIMEOUT = 30

BAN_LIST = ("adiad", "cancel", "postpon", "award")
ID_RE = re.compile(r"#id:(\d+)")

# Log apenas para info do sistema, output limpo no console
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


# ──────────────── FUNÇÕES AUXILIARES ──────────────── #

def criar_driver() -> webdriver.Chrome:
    op = Options()
    if HEADLESS:
        op.add_argument("--headless=new")
    
    op.add_argument(f"--window-size={WINDOW_SIZE}")
    op.add_argument("--disable-blink-features=AutomationControlled")
    op.add_argument("--log-level=3") # Suprime logs do chrome
    op.add_argument('--ignore-certificate-errors')
    op.add_argument("--no-sandbox")
    op.add_argument("--disable-dev-shm-usage")
    
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=op)


def fechar_overlays(w: WebDriverWait):
    """Fecha banners de cookies e anúncios."""
    xpaths = (
        "//button[contains(translate(.,'ACEITAR','aceitar'),'aceitar')]",
        "//button[@aria-label='Close']",
        "//div[contains(@class, 'fc-dialog')]//button[contains(@class, 'fc-cta-consent')]"
    )
    for xp in xpaths:
        try:
            elem = w.until(EC.element_to_be_clickable((By.XPATH, xp)))
            elem.click()
            time.sleep(0.2)
        except Exception:
            pass


def abrir_aba_jogos(w: WebDriverWait):
    try:
        xp = "//button[@role='tab' and contains(., 'Jogos')]"
        tab = w.until(EC.element_to_be_clickable((By.XPATH, xp)))
        if "selected" not in tab.get_attribute("class") and tab.get_attribute("aria-selected") != "true":
            tab.click()
            time.sleep(2)
    except Exception:
        pass


def encontrar_botao_dropdown(w: WebDriverWait):
    """
    Encontra o botão correto.
    Geralmente existem dois: [Ano/Temporada] e [Rodada].
    Queremos o da Rodada.
    """
    xp_btn = "//button[contains(@class, 'dropdown__button')]"
    
    try:
        botoes = w.until(EC.presence_of_all_elements_located((By.XPATH, xp_btn)))
        
        # Lógica: O botão de rodada geralmente tem "Rodada" ou "Round" no texto
        # OU é o último botão da lista de filtros.
        candidato = None
        for btn in botoes:
            txt = btn.text.lower()
            if "rodada" in txt or "round" in txt or "final" in txt:
                return btn
            candidato = btn 
        
        # Se não achou por texto, retorna o último encontrado (comportamento padrão do site)
        return candidato
            
    except Exception:
        pass
    raise TimeoutError("Botão Dropdown não encontrado.")


def obter_fases(w: WebDriverWait) -> List[str]:
    """Abre o dropdown, ROLA A LISTA para carregar tudo e retorna os textos."""
    driver = w._driver
    btn = encontrar_botao_dropdown(w)
    
    # Abre o menu
    driver.execute_script("arguments[0].click();", btn)
    time.sleep(1)
    
    # Localiza o container da lista para fazer o scroll
    # O Sofascore usa classes como 'beautiful-scrollbar__content' ou 'dropdown__listContainer'
    # Vamos tentar achar o UL e rolar o pai dele
    try:
        xp_list_container = "//div[contains(@class, 'beautiful-scrollbar')]"
        container = w.until(EC.presence_of_element_located((By.XPATH, xp_list_container)))
        
        # Scroll agressivo para garantir que o lazy load carregue a rodada 1 a 30
        for _ in range(5):
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
            time.sleep(0.5)
            
        # Agora pega todos os itens
        xp_opcoes = "//ul[contains(@class, 'dropdown__list')]//li | //li[@role='option']"
        opcoes_elems = driver.find_elements(By.XPATH, xp_opcoes)
        
        fases = [elem.text.strip() for elem in opcoes_elems if elem.text.strip()]
        
        # Fecha
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.5)
        
        return fases
        
    except Exception as e:
        logging.error(f"Erro ao manipular lista: {e}")
        try:
            driver.execute_script("arguments[0].click();", btn)
        except:
            pass
        return []


def normaliza(txt: str) -> str:
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode().lower().strip()


def selecionar_fase(w: WebDriverWait, nome_fase: str):
    driver = w._driver
    driver.execute_script("window.scrollTo(0,0);")
    
    btn = encontrar_botao_dropdown(w)
    driver.execute_script("arguments[0].click();", btn)
    time.sleep(0.5)
    
    xp_opcoes = "//ul[contains(@class, 'dropdown__list')]//li | //li[@role='option']"
    opcoes = w.until(EC.presence_of_all_elements_located((By.XPATH, xp_opcoes)))
    
    nome_alvo = normaliza(nome_fase)
    
    for op in opcoes:
        if normaliza(op.text) == nome_alvo:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", op)
            driver.execute_script("arguments[0].click();", op)
            time.sleep(3) # Tempo crítico para carregar a nova lista de jogos
            return

    # Se falhar, tenta fechar
    driver.execute_script("arguments[0].click();", btn)


def scroll_pagina_inteira(driver: webdriver.Chrome):
    """Scroll na janela principal para carregar jogos."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    # Fazemos alguns scrolls para garantir
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def extrair_links(w: WebDriverWait) -> List[str]:
    driver = w._driver
    scroll_pagina_inteira(driver)
    
    # IMPORTANTE: Busca apenas dentro da tag <main> para evitar sidebar/footer
    # A estrutura do Sofascore geralmente coloca o conteúdo principal dentro de <main>
    try:
        main_content = driver.find_element(By.TAG_NAME, "main")
    except:
        main_content = driver # Fallback se não achar main
        
    elementos = main_content.find_elements(By.XPATH, ".//a[contains(@href, '/football/match/')]")
    
    links_encontrados = []
    
    for el in elementos:
        try:
            url = el.get_attribute("href")
            txt = normaliza(el.text)
            
            if any(b in txt for b in BAN_LIST):
                continue
            
            if url and ID_RE.search(url):
                # Evita duplicatas na mesma rodada
                if url not in links_encontrados:
                    links_encontrados.append(url)
                    
        except StaleElementReferenceException:
            continue
            
    return links_encontrados


# ──────────────── MAIN ──────────────── #

def main():
    driver = criar_driver()
    wait = WebDriverWait(driver, TIMEOUT)
    
    # Conjunto global para evitar imprimir links repetidos (da sidebar, caso vazem)
    # Mas como filtramos pelo <main>, a chance é menor. 
    # Se quiser ver a repetição em rodadas diferentes (ex: jogo adiado), remova esta verificação.
    ids_globais_vistos: Set[str] = set() 

    try:
        # logging.info("Acessando Sofascore (Headless)...")
        driver.get(URL_TORNEIO)
        fechar_overlays(wait)
        abrir_aba_jogos(wait)
        time.sleep(3)

        fases = obter_fases(wait)
        
        print("\n" + "="*30)
        print("LINKS COLETADOS")
        print("="*30 + "\n")

        # Inverte a lista se quiser começar da Rodada 1
        # fases.reverse()

        for fase in fases:
            print(f"# {fase}")
            
            try:
                selecionar_fase(wait, fase)
                links = extrair_links(wait)
                
                count = 0
                for link in links:
                    # Opcional: filtro global de unicidade
                    if link not in ids_globais_vistos:
                        print(f"'{link}',")
                        ids_globais_vistos.add(link)
                        count += 1
                
                # Se não achou nada, pode ser que a rodada ainda não tenha jogos ou falhou o load
                if count == 0:
                    # logging.info(f"Nenhum link novo encontrado em {fase}")
                    pass
                
                print("") 
                
            except Exception as e:
                # logging.error(f"Erro em {fase}")
                continue

    finally:
        driver.quit()

if __name__ == "__main__":
    main()