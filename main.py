# -*- coding: utf-8 -*-
import json, datetime, random, time, re, logging, sys, os, subprocess, tempfile, itertools
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import pymysql
import requests
import backoff
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from urllib.parse import urlparse
try:
    from substituicoes_clubes import substituicoes_por_id
except Exception:
    substituicoes_por_id = {}
    # logging básico antes do setup para não quebrar caso import falhe
    print("AVISO: substituicoes_clubes.py não encontrado ou inválido. Seguiremos sem substituições de clubes.")

# =============== CONFIG ===============
temporada = "2026"
MAX_MATCHES_PER_DRIVER = 30
HEADLESS = True

USAR_PROXY = True
proxy_user = ""
proxy_pass = ""
proxy_host = "200.174.198.86"
proxy_port = "8888"

PROXY_FAIL_STRIKES = 0
PROXY_FAIL_LIMIT = 2

EVENT_FAIL_STREAK = 0
EVENT_FAIL_BREAK_1 = 2
EVENT_FAIL_BREAK_2 = 4

COOLDOWN_403_429 = (8, 14)

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
]
_ua_cycle = itertools.cycle(random.sample(_UA_POOL, k=len(_UA_POOL)))

def random_user_agent() -> str:
    return next(_ua_cycle)

# ======= URLs: carregar de urls.txt e normalizar =======
def _normalize_url_line(ln: str) -> Optional[str]:
    if not ln:
        return None
    s = ln.strip()
    if not s or s.startswith("#"):
        return None
    if s.endswith(","):
        s = s[:-1].rstrip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1].strip()
    else:
        if s and s[0] in "'\"":
            s = s[1:].strip()
        if s and s[-1] in "'\"":
            s = s[:-1].strip()
    pr = urlparse(s)
    if pr.scheme not in ("http", "https") or not pr.netloc:
        logging.warning("Linha do urls.txt ignorada (URL inválida): %r", ln)
        return None
    return s

def carregar_urls(path: str = "urls.txt") -> List[str]:
    if not os.path.exists(path):
        logging.warning("Arquivo %s não encontrado. Nenhuma URL será processada.", path)
        return []
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            u = _normalize_url_line(raw)
            if u:
                urls.append(u)
    logging.info("Total de URLs carregadas do %s: %d", path, len(urls))
    return urls

# ======= Mapa de campeonatos =======
def substituir_campeonato(n: str) -> str:
    substituicoes_campeonatos = {
        'Brasileirão Betano': 'Brasileiro',
        'Copa Betano do Brasil': 'Copa do Brasil',
        'FIFA Club World Cup': 'Copa do Mundo de Clubes',
        'Brasileiro Série B': 'Brasileiro - Série B',
        'World Cup Qual. UEFA L': 'Eliminatórias',
        'World Cup Qual. UEFA H': 'Eliminatórias',
        'World Cup Qual. UEFA G': 'Eliminatórias',
        'World Cup Qual. UEFA C': 'Eliminatórias',
        'World Cup Qual. UEFA J': 'Eliminatórias',
        'World Cup Qual. UEFA D': 'Eliminatórias',
        'World Cup Qual. UEFA B': 'Eliminatórias',
        'World Cup Qual. UEFA A': 'Eliminatórias',
        'CONMEBOL Libertadores': 'Libertadores',
        'CONMEBOL Sudamericana': 'Sul-Americana',
        'Stars Cup': 'Copa do Catar I',
        'World Cup Qualification': 'Eliminatórias',
        'World Cup Qual. CONCACAF Gr. 2': 'Eliminatórias',
        'World Cup Qual. CONCACAF Group A': 'Eliminatórias',
        'World Cup Qual. CONCACAF Gr. 1': 'Eliminatórias',
        'World Cup Qual. CONCACAF Gr. 3': 'Eliminatórias',
        'World Cup Qual. UEFA L': 'Eliminatórias',
        'World Cup Qual. UEFA H': 'Eliminatórias',
        'World Cup Qual. UEFA G': 'Eliminatórias',
        'World Cup Qual. UEFA C': 'Eliminatórias',
        'World Cup Qual. UEFA J': 'Eliminatórias',
        'World Cup Qual. UEFA D': 'Eliminatórias',
        'World Cup Qual. UEFA K': 'Eliminatórias',
        'World Cup Qual. CONCACAF Group A': 'Eliminatórias',
        
        'Mineiro Módulo I': 'Mineiro',
        'Paulista Série A1': 'Paulista',
        'Carioca Série A – Taça Guanabara': 'Carioca',
        'Gaúcho - Mata-mata': 'Gaúcho',
        'Baiano, Série A': 'Baiano',
        'Cearense, Série A': 'Cearense',
        'CONMEBOL Recopa': 'Recopa Sul-Americana',
        'Mineiro Módulo I - Troféu Inconfidência': 'Mineiro',
        'Goiano, 1ª Divisão': 'Goiano',
        'UEFA Europa League': 'Liga Europa',
        'UEFA Champions League': 'Liga dos Campeões',
        'UEFA Conference League': 'Conference League',
        'UEFA Europa Conference League': 'Conference League',
        'NM Cup': 'Copa da Noruega',
        'Primera A': 'Colombiano',
        'Svenska Cupen': 'Copa da Suécia',
        'Liga de Primera': 'Chileno',
        'Liga AUF Uruguaya': 'Uruguaio',
        'J1 League': 'Japonês',
        'Primera Division': 'Venezuelano',
        'Copa Venezuela': 'Copa da Venezuela',
        'División Profesional': 'Boliviano',
        'Copa Division Profesional': 'Copa da Bolívia',
        'Super Cup': 'Supercopa',
        'Copa Colombia': 'Copa da Colômbia',
        'Liga 1': 'Peruano',
        'Premier Division': 'Irlandês',
        'ÖFB Cup': 'Copa da Áustria',
        'LigaPro Serie A': 'Equatoriano',
        'Eliteserien': 'Norueguês',
        'Copa Argentina': 'Copa da Argentina',
        'World Cup Qual. CAF': 'Eliminatórias',
        'World Cup Qual. UEFA I': 'Eliminatórias',
        'World Cup Qual. UEFA F': 'Eliminatórias',
        'World Cup Qual. CONCACAF Group C': 'Eliminatórias',
        'World Cup Qual. CONCACAF Group B': 'Eliminatórias',
        'Gaucho': 'Gaúcho',
        'Stars League': 'Catar',
        'Qatar Cup': 'Catar Cup',
        'Allsvenskan': 'Sueco',
        'Primera División': 'Paraguaio',
        'Supercopa Internacional': 'Supercopa da Argentina',
        'Liga Profesional de Fútbol': 'Argentino',
        'Liga Profesional': 'Argentino',
        'Copa Paraguay': 'Copa do Paraguai',
        'Austrian Bundesliga': 'Austríaco',
        'Campeonato Paraense': 'Paraense',
        'Liga de Primera - Relegation Playoffs': 'Chileno',
        'Copa Chile': 'Copa do Chile',
        'Copa Ecuador': 'Copa do Equador',
        'Supercopa': 'Supercopa do Equador',
        'MLS Pre Season': 'MLS Pré-Temporada',
        'Leinster Senior Cup': 'Copa da Liga da Irlanda',
        'FAI Presidents Cup': 'Supercopa da Irlanda',
        'Emperor Cup': 'Copa do Imperador',
        'J. League Cup': 'Copa da Liga do Japão',
        'FAI Cup': 'Copa da Irlanda',
        'Mozzart Kup Srbije': 'Copa da Sérvia',
        'División de Honor': 'Paraguaio',
        'Supercopa Uruguaya': 'Supercopa do Uruguai',
        'Baianão Mansão Green 2026': 'Baiano',
        'Recopa Gaucha': 'Recopa Gaúcha',
        'QSL Cup': 'Copa do Catar I',
        'Supercopa Venezuela': 'Supercopa da Venezuela',
        'Community Shield': 'Supercopa da Inglaterra',
        'FA Cup': 'Copa da Inglaterra',
        'EFL Cup': 'Copa da Liga Inglesa',
        'World Cup Qual. CONCACAF Gr. 3': 'Eliminatórias',
        'World Cup Qual. CAF': 'Eliminatórias',
        'Copa Bolivia': 'Copa da Bolívia',
        'LaLiga': 'La Liga',
        'Mineiro Módulo I - Classificação contra o rebaixamento': 'Mineiro',
        'Carioca Série A – Mata-mata': 'Carioca',
        'Carioca - Taça Rio': 'Carioca',
        'Liga DIMAYOR': 'Colombiano',
        'Africa Cup of Nations': 'Copa das Nações Africanas',
        'World Cup Qual. CAF': 'Eliminatórias',
        'World Cup Qual. UEFA E': 'Eliminatórias',
        'Carioca - Semifinais': 'Carioca',
        'Carioca - Classificação preliminar': 'Carioca',
        'Carioca - Classificação contra o rebaixamento': 'Carioca',
        'Carioca - Mata-mata contra o rebaixamento': 'Carioca',
        'Carioca - Mata-mata da Taça Guanabara': 'Carioca',
        'Gaúcho - Final': 'Gaúcho',
        'Gaúcho 2ª fase': 'Gaúcho',
        'Pernambucano - Luta contra o rebaixamento': 'Pernambucano',
        'Cearense - Luta contra o rebaixamento': 'Cearense',
        'Goiano - Grupo B': 'Goiano',
        'Goiano - Grupo A': 'Goiano',
        'Supercopa de España': 'Supercopa da Espanha',
        'Copa del Rey': 'Copa do Rei',
        'Coppa Italia': 'Copa da Itália',
        'Supercoppa Italiana': 'Supercopa da Itália',
        'DFL Supercup': 'Supercopa da Alemanha',
        'Supercup': 'Supercopa da Alemanha',
        'DFB Pokal': 'Copa da Alemanha',
        'Trophée des Champions': 'Supercopa da França',
        'Coupe de France': 'Copa da França',
        'Copa Bicentenario': 'Copa do Peru',
        'Supercopa Peruana': 'Supercopa do Peru',
        'Liga Portugal Betclic': 'Português',
        'Taça de Portugal': 'Copa de Portugal',
        'League Cup': 'Copa da Liga Portuguesa',
        'Johan Cruijff Schaal': 'Supercopa da Holanda',
        'KNVB beker': 'Copa da Holanda',
        'Ukraine Cup': 'Copa da Ucrânia',
        'Trendyol Süper Lig': 'Turco',
        'Super Kupa': 'Supercopa da Turquia',
        'Turkiye Kupasi': 'Copa da Turquia',
        'Stoiximan Super League': 'Grego',
        'Greek Football Cup': 'Copa da Grécia',
        'Saudi Pro League': 'Saudita',
        "King's Cup": 'Copa da Arábia',
        'UEFA Super Cup': 'Supercopa da Europa',
        'Liga dos Campeões da UEFA': 'Liga dos Campeões',
        'AFC Champions League Elite': 'Liga dos Campeões da Ásia',
        'CONCACAF Champions Cup': 'Copa dos Campeões da Concacaf',
        'Liga MX': 'Mexicano',
        'Supercopa Liga MX': 'Supercopa da Liga Mexicana',
        'Russian Cup': 'Copa da Rússia',
        'Danish Superliga': 'Dinamarquês',
        'Oddset Pokalen': 'Copa da Dinamarca',
        'Pro League': 'Belga',
        'Mozzart Bet Superliga': "Sérvio",
        'Beker van Belgie': 'Copa da Bélgica',
        'Primera LPF': 'Argentino',
        'World Cup Qualificação': 'Eliminatórias',
        'Int. Friendly Games': 'Amistoso',
        'CONMEBOL Copa América': 'Copa América',
        'UEFA Nations League': 'Nations League',
        'Euro': 'Eurocopa',
        'FIFA World Cup': 'Copa do Mundo',
        'Copa Uruguay': 'Copa do Uruguai',
        'CONCACAF Gold Cup': 'Copa Ouro',
        'FIFA Intercontinental Cup': 'Mundial de Clubes',
        'VriendenLoterij Eredivisie': 'Eredivisie',
        'Brasileirão Série B': 'Brasileiro - Série B',
        'Greece Cup': 'Copa da Grécia',
    }
    nome = (n or "").strip()
    return substituicoes_campeonatos.get(nome, nome)

# ============== LOGGING ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("sofascore")

# ====== Notificação Windows (opcional) ======
def _xml_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")\
                    .replace('"', "&quot;").replace("'", "&apos;")

def notify_windows(title: str, message: str, duration: int = 10) -> bool:
    try:
        from winotify import Notification, audio
        toast = Notification(app_id="SofaScore Scraper", title=title, msg=message,
                             duration="short" if duration <= 7 else "long")
        toast.set_audio(audio.Default, loop=False)
        toast.show()
        return True
    except Exception:
        pass
    try:
        from win10toast import ToastNotifier
        ToastNotifier().show_toast(title, message, threaded=False, duration=duration)
        return True
    except Exception:
        pass
    try:
        from plyer import notification
        notification.notify(title=title, message=message, timeout=duration)
        return True
    except Exception:
        pass
    try:
        t = _xml_escape(title); m = _xml_escape(message)
        ps_code = f'''
Add-Type -AssemblyName System.Runtime.WindowsRuntime | Out-Null
$null = [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime]
$null = [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom, ContentType = WindowsRuntime]
$xml = @"
<toast><visual><binding template="ToastGeneric"><text>{t}</text><text>{m}</text></binding></visual></toast>
"@
$doc = New-Object Windows.Data.Xml.Dom.XmlDocument
$doc.LoadXml($xml)
$notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("SofaScore Scraper")
$toast = [Windows.UI.Notifications.ToastNotification]::new($doc)
$notifier.Show($toast)
'''
        with tempfile.NamedTemporaryFile(delete=False, suffix=".ps1", mode="w", encoding="utf-8") as f:
            f.write(ps_code)
            ps1_path = f.name
        subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ps1_path],
                       capture_output=True, text=True, timeout=10)
        try: os.unlink(ps1_path)
        except Exception: pass
        return True
    except Exception:
        pass
    return False

def beep_ok():
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_ICONASTERISK)
    except Exception:
        pass

# ====== Sessão HTTP ======
session: Optional[requests.Session] = None
SESSION_UA = random_user_agent()

def rebuild_session():
    global session, SESSION_UA
    SESSION_UA = random_user_agent()
    try:
        if session:
            session.close()
    except Exception:
        pass
    s = requests.Session()
    s.trust_env = False
    s.headers.update({
        "User-Agent": SESSION_UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.sofascore.com",
        "Referer": "https://www.sofascore.com/",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    if USAR_PROXY and proxy_host and proxy_port:
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        s.proxies.update({"http": proxy_url, "https": proxy_url})
        log.info("🌐 Proxy requests habilitado: %s | UA=%s", proxy_url, SESSION_UA)
    else:
        log.info("🌐 Requests sem proxy | UA=%s", SESSION_UA)
    session = s

# ====== Proxy helpers ======
def desativar_proxy_instavel(motivo: str):
    global USAR_PROXY, PROXY_FAIL_STRIKES, driver, matches_done_with_this_driver
    if not USAR_PROXY:
        return
    USAR_PROXY = False
    PROXY_FAIL_STRIKES = 0
    rebuild_session()
    log.warning("🛑 Proxy desativado (%s). Reiniciando Chrome sem proxy...", motivo)
    try:
        driver.quit()
    except Exception:
        pass
    driver = criar_driver()
    matches_done_with_this_driver = 0

def alternar_proxy(motivo: str):
    global USAR_PROXY, PROXY_FAIL_STRIKES
    USAR_PROXY = not USAR_PROXY
    PROXY_FAIL_STRIKES = 0
    estado = "ON" if USAR_PROXY else "OFF"
    log.warning("🔁 Alternando proxy (%s). Novo estado: %s", motivo, estado)
    rebuild_session()
    restart_driver()

def circuit_breaker_event_failures():
    global EVENT_FAIL_STREAK
    if EVENT_FAIL_STREAK >= EVENT_FAIL_BREAK_2:
        alternar_proxy("falhas consecutivas em 'event'")
        EVENT_FAIL_STREAK = 0
        return
    if EVENT_FAIL_STREAK >= EVENT_FAIL_BREAK_1:
        log.warning("🧯 Circuit breaker: restart driver + rebuild session (falhas seguidas em 'event').")
        restart_driver()
        rebuild_session()

# ====== Chrome Driver ======
def criar_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-webgl")
    opts.add_argument("--disable-3d-apis")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-features=IsolateOrigins,site-per-process,NetworkServiceInProcess")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.page_load_strategy = 'eager'
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2
    })
    ua = random_user_agent()
    opts.add_argument(f"--user-agent={ua}")

    if USAR_PROXY and proxy_host and proxy_port:
        proxy_spec = f"http={proxy_host}:{proxy_port};https={proxy_host}:{proxy_port}"
        opts.add_argument(f"--proxy-server={proxy_spec}")
        log.info("🌐 Proxy Chrome habilitado: %s | UA=%s", proxy_spec, ua)
    else:
        log.info("🌐 Chrome sem proxy | UA=%s", ua)

    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(30)
    drv.set_script_timeout(25)
    try:
        drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return drv

rebuild_session()
driver = criar_driver()
matches_done_with_this_driver = 0

def restart_driver():
    global driver, matches_done_with_this_driver
    try:
        driver.quit()
    except Exception:
        pass
    driver = criar_driver()
    matches_done_with_this_driver = 0
    log.info("♻️  ChromeDriver reiniciado")

# ====== DB ======
conn = pymysql.connect(host="localhost", user="admin", password="1234",
                       database="bet_dados", charset="utf8mb4", autocommit=False)
cursor = conn.cursor()

# ====== FIX SEGURO DA COLUNA partidas.data ======
def ensure_partidas_date_column_safe():
    try:
        cursor.execute("SHOW COLUMNS FROM partidas LIKE 'data_fix'")
        has_fix = cursor.fetchone() is not None
        if not has_fix:
            cursor.execute("ALTER TABLE partidas ADD COLUMN data_fix DATE NULL")
            conn.commit()

        cursor.execute("SHOW COLUMNS FROM partidas LIKE 'data'")
        row = cursor.fetchone()
        if not row:
            log.warning("Tabela 'partidas' não tem coluna 'data'. Pulando normalização.")
            return
        col_type = row[1].lower()

        if "date" in col_type:
            cursor.execute("UPDATE partidas SET data_fix = data WHERE data IS NOT NULL")
            conn.commit()
        else:
            log.info("🧼 Normalizando partidas.data (tipo atual: %s) → DATE (via data_fix)...", col_type)
            cursor.execute("UPDATE partidas SET data_fix = NULL")
            conn.commit()

            cursor.execute("""
                UPDATE partidas
                   SET data_fix = DATE(SUBSTRING(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(data, UNHEX('C2A0'), ''), '\t',''), '\r',''), '\n','')),1,10))
                 WHERE TRIM(REPLACE(REPLACE(REPLACE(REPLACE(data, UNHEX('C2A0'), ''), '\t',''), '\r',''), '\n','')) LIKE '____-__-__%%'
            """)
            conn.commit()

            cursor.execute("""
                UPDATE partidas
                   SET data_fix = STR_TO_DATE(SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(data, UNHEX('C2A0'), ''), '\t',''), '\r',''), '\n','')), ' ', 1), '%d/%m/%Y')
                 WHERE data_fix IS NULL
                   AND TRIM(REPLACE(REPLACE(REPLACE(REPLACE(data, UNHEX('C2A0'), ''), '\t',''), '\r',''), '\n','')) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}'
            """)
            conn.commit()

            cursor.execute("""
                UPDATE partidas
                   SET data_fix = STR_TO_DATE(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(data, UNHEX('C2A0'), ''), '\t',''), '\r',''), '\n','')), '%d-%m-%Y')
                 WHERE data_fix IS NULL
                   AND TRIM(REPLACE(REPLACE(REPLACE(REPLACE(data, UNHEX('C2A0'), ''), '\t',''), '\r',''), '\n','')) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            """)
            conn.commit()

        if "date" not in col_type:
            cursor.execute("ALTER TABLE partidas CHANGE COLUMN data data_old VARCHAR(50) NULL")
            cursor.execute("ALTER TABLE partidas CHANGE COLUMN data_fix data DATE NULL")
            conn.commit()
            try:
                cursor.execute("ALTER TABLE partidas DROP COLUMN data_old")
                conn.commit()
            except Exception:
                pass

        log.info("✅ partidas.data está consistente como DATE.")
    except Exception as e:
        conn.rollback()
        log.warning("Não consegui normalizar partidas.data agora (%s). Seguindo assim mesmo.", e)

ensure_partidas_date_column_safe()

# ====== Helpers DOM / API ======
def _deep_find(d: Any, key: str) -> Any:
    if isinstance(d, dict):
        if key in d:
            return d[key]
        for v in d.values():
            r = _deep_find(v, key)
            if r is not None:
                return r
    elif isinstance(d, list):
        for item in d:
            r = _deep_find(item, key)
            if r is not None:
                return None if r is ... else r
    return None

def _load_next_data_from_dom(drv: webdriver.Chrome) -> Optional[dict]:
    for sel in ["script#__NEXT_DATA__", "script[id='__NEXT_DATA__']"]:
        try:
            el = drv.find_element(By.CSS_SELECTOR, sel)
            txt = el.get_attribute("innerHTML") or ""
            if txt.strip().startswith("{"):
                return json.loads(txt)
        except NoSuchElementException:
            continue
        except Exception:
            continue
    try:
        txt = drv.execute_script("return window.__NEXT_DATA__ ? JSON.stringify(window.__NEXT_DATA__) : null;")
        if txt:
            return json.loads(txt)
    except Exception:
        pass
    return None

def extract_event_from_dom(drv: webdriver.Chrome, id_jogo: str) -> Optional[Dict]:
    try:
        data = _load_next_data_from_dom(drv)
        if not data:
            return None
        evt = _deep_find(data, "event")
        if isinstance(evt, dict) and str(evt.get("id")) == str(id_jogo):
            return evt
        page_props = _deep_find(data, "pageProps")
        evt2 = _deep_find(page_props, "event") if page_props else None
        if isinstance(evt2, dict) and str(evt2.get("id")) == str(id_jogo):
            return evt2
    except Exception:
        return None
    return None

def _selenium_api_json(url_api: str) -> Dict:
    global PROXY_FAIL_STRIKES
    try:
        driver.get(url_api)
    except WebDriverException as e:
        if "ERR_CONNECTION_RESET" in str(e):
            PROXY_FAIL_STRIKES += 1
            if USAR_PROXY and PROXY_FAIL_STRIKES >= PROXY_FAIL_LIMIT:
                desativar_proxy_instavel("ERR_CONNECTION_RESET no Selenium")
        raise

    try:
        try:
            el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
            txt = el.text.strip()
            if txt:
                return json.loads(txt)
        except TimeoutException:
            pass

        try:
            txt = driver.find_element(By.TAG_NAME, "body").text.strip()
            if txt.strip().startswith("{") or txt.strip().startswith("["):
                return json.loads(txt)
        except Exception:
            pass

        src = driver.page_source
        m = re.search(r"<pre[^>]*>(.*?)</pre>", src, flags=re.DOTALL | re.IGNORECASE)
        if m:
            raw = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            return json.loads(raw)

        raise ValueError("Não consegui capturar JSON via Selenium")
    except TimeoutException:
        PROXY_FAIL_STRIKES += 1
        if USAR_PROXY and PROXY_FAIL_STRIKES >= PROXY_FAIL_LIMIT:
            desativar_proxy_instavel("Timeout ao ler JSON via Selenium")
        raise
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido na página da API: {e}")

def _api_url(url: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_={int(time.time()*1000)}"

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, ValueError, TimeoutException, WebDriverException),
    factor=2, max_time=90
)
def obter_dados_api(url_api: str) -> Dict:
    global PROXY_FAIL_STRIKES
    try:
        if session:
            session.cookies.clear()
        for c in driver.get_cookies():
            session.cookies.set(c["name"], c["value"])
    except Exception:
        pass

    url_api_cb = _api_url(url_api)
    try:
        resp = session.get(url_api_cb, timeout=20)
    except requests.exceptions.RequestException as e:
        PROXY_FAIL_STRIKES += 1
        if USAR_PROXY and PROXY_FAIL_STRIKES >= PROXY_FAIL_LIMIT:
            desativar_proxy_instavel(f"Falhas consecutivas no requests: {type(e).__name__}")
        log.debug("requests exception (%s). Fallback Selenium JSON.", type(e).__name__)
        return _selenium_api_json(url_api_cb)

    if resp.status_code in (403, 429):
        log.warning("🚧 API %s retornou %s. Aguardando cooldown e tentando via Selenium.",
                    url_api, resp.status_code)
        time.sleep(random.uniform(*COOLDOWN_403_429))
        return _selenium_api_json(url_api_cb)

    if resp.status_code >= 500:
        log.warning("🛠️ API %s retornou %s. Fallback Selenium.", url_api, resp.status_code)
        return _selenium_api_json(url_api_cb)

    try:
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.debug("Falha ao parsear JSON (%s). Fallback Selenium.", e)
        return _selenium_api_json(url_api_cb)

def safe_get_event(id_jogo: str) -> Dict:
    global EVENT_FAIL_STREAK
    for tentativa in range(4):
        try:
            data = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}")
            if "event" in data:
                EVENT_FAIL_STREAK = 0
                return data["event"]
            else:
                log.debug("API sem 'event' (keys=%s). Tentando DOM.", list(data.keys()))
        except Exception as e:
            log.debug("API falhou (%s) – tentando DOM", e)

        dom_evt = extract_event_from_dom(driver, id_jogo)
        if dom_evt:
            EVENT_FAIL_STREAK = 0
            return dom_evt

        log.warning("Tentativa %d – não consegui 'event'", tentativa + 1)
        time.sleep(4 + random.random() * 2)

    try:
        evt = _selenium_api_json(_api_url(f"https://api.sofascore.com/api/v1/event/{id_jogo}")).get("event")
        if evt:
            EVENT_FAIL_STREAK = 0
            return evt
    except Exception:
        pass

    EVENT_FAIL_STREAK += 1
    circuit_breaker_event_failures()
    raise RuntimeError(f"Não consegui 'event' para {id_jogo}")

# ====== Odds / Região ======
def get_region_text(drv: webdriver.Chrome) -> str:
    for s in drv.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']"):
        try:
            d = json.loads(s.get_attribute("innerHTML"))
            if isinstance(d, dict) and d.get("@type") == "BreadcrumbList":
                return d["itemListElement"][1]["name"]
        except Exception:
            pass
    anchors = drv.find_elements(By.CSS_SELECTOR, "a[href^='/pt/futebol/']:not([href='/pt/futebol/'])")
    return anchors[0].text.strip() if anchors else "Desconhecido"

def get_odds(drv: webdriver.Chrome, max_scrolls: int = 6, wait_each: float = 0.7) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    X_RES_FINAL = ("//span[translate(.,'RESULTADO FINAL','resultado final')='resultado final']"
                   "/ancestor::div[contains(@class,'bg_surface')]")
    try:
        for _ in range(max_scrolls):
            try:
                bloco = WebDriverWait(drv, 5).until(EC.presence_of_element_located((By.XPATH, X_RES_FINAL)))
                links = bloco.find_elements(
                    By.XPATH,
                    ".//a[.//span[@class='textStyle_assistive.default c_neutrals.nLv3 w_100% ta_center trunc_true']]")
                mapa = {}
                for l in links:
                    rot = l.find_element(By.XPATH, ".//span[contains(@class,'textStyle_assistive.default')]").text.strip()
                    val = l.find_element(By.XPATH, ".//span[contains(@class,'textStyle_display.micro')]").text.strip()
                    mapa[rot] = val
                return mapa.get("1"), mapa.get("X"), mapa.get("2")
            except Exception:
                try:
                    drv.execute_script("window.scrollBy(0, 1000);")
                except Exception:
                    pass
                time.sleep(wait_each)
    except (TimeoutException, WebDriverException) as e:
        logging.debug("get_odds travou: %s", e)
    return None, None, None

# ====== NOVO: helper para aplicar substituição por ID do clube ======
def nome_clube(team: dict) -> str:
    """
    Recebe o objeto de time retornado pela API do SofaScore (ex.: ev['homeTeam']).
    Se existir um ID e ele estiver em substituicoes_por_id, retorna o nome padronizado.
    Caso contrário, retorna o name original.
    """
    try:
        tid = int(team.get("id") or 0)
    except Exception:
        tid = 0
    original = (team.get("name") or "").strip()
    if tid and tid in substituicoes_por_id:
        return (substituicoes_por_id.get(tid) or original).strip()
    return original

# ====== Inserções de estatísticas ======
def inserir_estatisticas(id_jogo: str, home: str, away: str,
                         periodo: str, tabela: str, stats: dict, conn=conn, cursor=cursor):
    cursor.execute(f"SELECT 1 FROM {tabela} WHERE id_jogo=%s LIMIT 1", (id_jogo,))
    if cursor.fetchone():
        return
    grp = next((g for g in stats.get("statistics", []) if g["period"] == periodo), None)
    if not grp or not grp.get("groups"):
        return
    df = pd.DataFrame([{"id_jogo": id_jogo, "clube": home},
                       {"id_jogo": id_jogo, "clube": away}])
    df["temporada"] = temporada
    for g in grp["groups"]:
        for it in g.get("statisticsItems", []):
            k = it["name"].lower().replace(" ", "_").replace("(", "").replace(")", "")
            df.loc[0, k] = it.get("home", 0)
            df.loc[1, k] = it.get("away", 0)

    cursor.execute(f"SHOW COLUMNS FROM {tabela}")
    cols_db = {c[0] for c in cursor.fetchall()}
    for col in cols_db:
        if col not in df.columns and col != "id":
            df[col] = datetime.datetime.now() if col == "cadastrado_em" else 0
    df = df[[c for c in df.columns if c in cols_db]]
    cols_sql = ",".join(f"`{c}`" for c in df.columns)
    vals_sql = ",".join(["%s"] * len(df.columns))
    cursor.executemany(f"INSERT INTO {tabela} ({cols_sql}) VALUES ({vals_sql})",
                       df.fillna(0).values.tolist())
    conn.commit()
    log.info("✅  Estatísticas inseridas: %s", tabela)

# ====== Tempo dos gols ======
def _disp(x):
    return x.get("display", 0) if isinstance(x, dict) else (x or 0)

def inserir_tempo_gols(id_jogo: str, campeonato_padronizado: str, home: str, away: str, conn=conn, cursor=cursor):
    cursor.execute("SELECT 1 FROM tempo_gols WHERE id_jogo=%s LIMIT 1", (id_jogo,))
    if cursor.fetchone():
        log.info("ℹ️  tempo_gols já possui id_jogo=%s – nada a fazer.", id_jogo)
        return

    data = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}/incidents")
    incs = data.get("incidents", []) or []

    rows = []
    for inc in incs:
        if inc.get("incidentType") != "goal":
            continue

        home_s = _disp(inc.get("homeScore"))
        away_s = _disp(inc.get("awayScore"))

        actions = inc.get("footballPassingNetworkAction") or [inc]
        for ac in actions:
            tipo = ac.get("eventType", inc.get("incidentType"))
            if tipo != "goal":
                continue
            is_home = ac.get("isHome", inc.get("isHome"))
            minutos = ac.get("time", inc.get("time"))
            forma = ac.get("bodyPart", inc.get("bodyPart"))
            clube_gol = home if is_home else away

            rows.append({
                "id_jogo": id_jogo,
                "temporada": temporada,
                "campeonato": campeonato_padronizado,
                "clube_gol": clube_gol,
                "minutos": int(minutos or 0),
                "forma_gol": forma or "",
                "placar": f"{home_s}-{away_s}",
            })
            break

    if not rows:
        log.info("⚠️  Sem gols (provável 0x0)")
        return

    df = pd.DataFrame(rows)
    cursor.execute("SHOW COLUMNS FROM tempo_gols")
    cols_db = [c[0] for c in cursor.fetchall()]
    if "cadastrado_em" in cols_db and "cadastrado_em" not in df.columns:
        df["cadastrado_em"] = datetime.datetime.now()

    keep = [c for c in ["id_jogo","temporada","campeonato","clube_gol","minutos","forma_gol","placar","cadastrado_em"] if c in cols_db]
    df = df[keep]

    cols_sql = ",".join(f"`{c}`" for c in df.columns)
    vals_sql = ",".join(["%s"] * len(df.columns))
    cursor.executemany(f"INSERT INTO tempo_gols ({cols_sql}) VALUES ({vals_sql})", df.values.tolist())
    conn.commit()
    log.info("✅  tempo_gols inserido: %d linha(s) para id_jogo=%s", len(df), id_jogo)

# ====== MAIN LOOP ======
def main():
    inicio = time.time()
    erros = 0

    urls = carregar_urls()
    if not urls:
        log.warning("Nenhuma URL para processar (verifique urls.txt).")
        return

    global matches_done_with_this_driver

    for idx, url in enumerate(urls, start=1):
        if not url.strip():
            continue
        if matches_done_with_this_driver >= MAX_MATCHES_PER_DRIVER:
            restart_driver()

        log.info("⚽  (%d/%d) %s", idx, len(urls), url)
        try:
            m = re.search(r"#id:(\d+)", url)
            if not m:
                log.warning("URL sem id_jogo: %s", url); continue
            id_jogo = m.group(1)

            try:
                driver.get(url)
            except TimeoutException:
                log.warning("⏱️ page load timeout; tentando novamente 1x")
                restart_driver()
                driver.get(url)
            except WebDriverException as e:
                if "ERR_CONNECTION_RESET" in str(e) and USAR_PROXY:
                    desativar_proxy_instavel("ERR_CONNECTION_RESET ao abrir a página do jogo")
                    driver.get(url)
                else:
                    raise

            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR,
                         "script[type='application/ld+json'],"
                         "a[href^='/pt/futebol/']:not([href='/pt/futebol/'])")))
            except TimeoutException:
                log.warning("⚠️ DOM lento — seguindo sem aguardar completamente.")

            pais = get_region_text(driver)
            odd_casa, odd_empate, odd_fora = get_odds(driver)

            ev = safe_get_event(id_jogo)

            # ====== NOVO: aplicar substituição por ID do clube ======
            home_team, away_team = ev["homeTeam"], ev["awayTeam"]
            home = nome_clube(home_team)
            away = nome_clube(away_team)

            log.info("Partida: %s x %s | odds %s/%s/%s", home, away, odd_casa, odd_empate, odd_fora)

            cursor.execute("SELECT pais, odd_casa, odd_empate, odd_fora, campeonato FROM partidas WHERE id_jogo=%s",
                           (id_jogo,))
            row = cursor.fetchone()
            campeonato_padronizado = None

            if not row:
                dt_ini_utc = datetime.datetime.fromtimestamp(ev["startTimestamp"], tz=datetime.timezone.utc)
                dt_brt = dt_ini_utc.astimezone(datetime.timezone(datetime.timedelta(hours=-3)))
                data_date = dt_brt.date()
                hora_str = dt_brt.strftime("%H:%M")

                campeonato_padronizado = substituir_campeonato((ev["tournament"]["name"] or "").split(",")[0])
                round_info = ev.get("roundInfo") or {}
                rodada_m = re.search(r"\d+", str(round_info.get("name") or round_info.get("round") or "0"))
                rodada = rodada_m.group() if rodada_m else "0"
                gols_casa = ev["homeScore"].get("normaltime", ev["homeScore"].get("current", 0))
                gols_fora = ev["awayScore"].get("normaltime", ev["awayScore"].get("current", 0))

                cursor.execute("""
                    INSERT INTO partidas
                    (id_jogo, data, hora, pais, campeonato, rodada, casa, fora,
                     gols_casa, gols_fora, temporada, odd_casa, odd_empate, odd_fora, cadastrado_em)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (id_jogo, data_date, hora_str, pais, campeonato_padronizado, rodada, home, away,
                      gols_casa, gols_fora, temporada, odd_casa, odd_empate, odd_fora,
                      datetime.datetime.now()))
                conn.commit()
                log.info("✅  Partida inserida (id_jogo=%s)", id_jogo)
            else:
                pais_db, oc_db, oe_db, of_db, camp_db = row
                campeonato_padronizado = camp_db

                sets, vals = [], []
                if pais and pais != "Desconhecido" and (not pais_db or not str(pais_db).strip()):
                    sets.append("pais=%s");           vals.append(pais)
                if odd_casa and (not oc_db or str(oc_db).strip() == ""):
                    sets.append("odd_casa=%s");       vals.append(odd_casa)
                if odd_empate and (not oe_db or str(oe_db).strip() == ""):
                    sets.append("odd_empate=%s");     vals.append(odd_empate)
                if odd_fora and (not of_db or str(of_db).strip() == ""):
                    sets.append("odd_fora=%s");       vals.append(odd_fora)
                if not campeonato_padronizado:
                    campeonato_padronizado = substituir_campeonato((ev["tournament"]["name"] or "").split(",")[0])
                    sets.append("campeonato=%s");     vals.append(campeonato_padronizado)

                # (Opcional) Atualizar nomes caso mudem após substituições – geralmente não precisa,
                # mas se quiser garantir que 'casa' e 'fora' reflitam o nome padronizado:
                # sets.append("casa=%s"); vals.append(home)
                # sets.append("fora=%s"); vals.append(away)

                if sets:
                    vals.append(id_jogo)
                    cursor.execute(f"UPDATE partidas SET {', '.join(sets)} WHERE id_jogo=%s", vals)
                    conn.commit()
                    log.info("✅  Partida atualizada (id_jogo=%s)", id_jogo)
                else:
                    log.info("ℹ️  Partida já existia e não precisou de atualização (id_jogo=%s)", id_jogo)

            # Estatísticas
            try:
                stats = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}/statistics")
                inserir_estatisticas(id_jogo, home, away, "1ST", "1_tempo", stats)
                inserir_estatisticas(id_jogo, home, away, "2ND", "2_tempo", stats)
                inserir_estatisticas(id_jogo, home, away, "ALL", "estatisticas_partidas", stats)
            except Exception as e:
                log.warning("⚠️  Não consegui estatísticas (%s). Seguindo.", e)

            # Lineups / estatísticas de jogadores
            try:
                cursor.execute("SELECT 1 FROM estatisticas_jogadores WHERE id_jogo=%s LIMIT 1", (id_jogo,))
                if not cursor.fetchone():
                    lineups = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}/lineups")
                    players = []
                    # Aqui já usamos 'home' e 'away' padronizados
                    for side, team_name in [("home", home), ("away", away)]:
                        for p in lineups.get(side, {}).get("players", []):
                            ply, st = p.get("player", {}), p.get("statistics", {})
                            rp = {
                                "id_jogo": id_jogo, "team": team_name, "name": ply.get("name"),
                                "position": ply.get("position"),
                                "jersey_number": int(ply.get("jerseyNumber") or 0),
                                "height": int(ply.get("height") or 0), "player_id": ply.get("id"),
                                "date_of_birth_timestamp": ply.get("dateOfBirthTimestamp"),
                                "country": ply.get("country", {}).get("name"),
                                "value": ply.get("proposedMarketValueRaw", {}).get("value"),
                                "substitute": p.get("substitute"), "captain": p.get("captain"),
                                "rating": st.get("ratingVersions", {}).get("original"),
                            }
                            for k, v in st.items():
                                if k != "ratingVersions":
                                    rp[re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower()] = v
                            players.append(rp)
                    if players:
                        dfp = pd.DataFrame(players).fillna(0); dfp["temporada"] = temporada
                        cursor.execute("SHOW COLUMNS FROM estatisticas_jogadores")
                        cols_db = {c[0] for c in cursor.fetchall()}
                        for col in cols_db:
                            if col not in dfp.columns and col not in ("id", "mando"):
                                dfp[col] = datetime.datetime.now() if col == "cadastrado_em" else 0
                        dfp = dfp[[c for c in dfp.columns if c in cols_db and c != "mando"]]
                        cols_sql = ",".join(f"`{c}`" for c in dfp.columns)
                        vals_sql = ",".join(["%s"] * len(dfp.columns))
                        cursor.executemany(
                            f"INSERT INTO estatisticas_jogadores ({cols_sql}) VALUES ({vals_sql})",
                            dfp.values.tolist())
                        conn.commit()
                        log.info("✅  Jogadores inseridos: %d (id_jogo=%s)", len(players), id_jogo)
            except Exception as e:
                log.warning("⚠️  Não consegui lineups (%s). Seguindo.", e)

            # Tempo dos gols
            try:
                inserir_tempo_gols(id_jogo, campeonato_padronizado, home, away)
            except Exception as e:
                log.warning("⚠️  Não consegui tempo_gols (%s). Seguindo.", e)

        except Exception as e:
            if "Timed out receiving message from renderer" in str(e):
                log.warning("🧹 Renderer travou — reiniciando driver para recuperar.")
                restart_driver()
            log.error("❌  Erro em %s: %s", url, e, exc_info=True)
            conn.rollback()
            erros += 1
        else:
            conn.commit()

        matches_done_with_this_driver += 1
        time.sleep(random.uniform(4, 8))

    # Encerramento
    try:
        driver.quit()
    except Exception:
        pass
    try:
        cursor.close()
        conn.close()
    except Exception:
        pass

    dur = int(time.time() - inicio)
    mins, secs = divmod(dur, 60)
    resumo = f"Concluído em {mins}m{secs}s • URLs: {len(urls)} • Erros: {erros}"
    log.info("✅✅✅  Processo finalizado. " + resumo)

    ok = notify_windows("Scraper finalizado ✅", resumo, duration=10)
    if not ok:
        log.warning("⚠️ Nenhum método de notificação funcionou (veja logs).")
    beep_ok()

if __name__ == "__main__":
    main()
