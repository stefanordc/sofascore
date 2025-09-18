import json, datetime, random, time, re, logging, sys, collections, os
from typing import Dict, List, Tuple, Any
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
from selenium.common.exceptions import TimeoutException, WebDriverException
import substituicoes_clubes

# ───────────────────────────── CONFIGURAÇÕES ───────────────────────────── #
temporada = "2025"
MAX_MATCHES_PER_DRIVER = 30
HEADLESS = True

# ------------------------- PROXY (SEM AUTENTICAÇÃO) ---------------------- #
USAR_PROXY = True
proxy_user = ""
proxy_pass = ""
proxy_host = "200.174.198.86"
proxy_port = "8888"

# falhas consecutivas do proxy (requests/selenium) para desligar automático
PROXY_FAIL_STRIKES = 0
PROXY_FAIL_LIMIT = 2

# ----------------------------- LISTA DE URLS ----------------------------- #
urls: List[str] = [
    'https://www.sofascore.com/pt/football/match/mirassol-botafogo/iOsHOi#id:14550035',
    'https://www.sofascore.com/pt/football/match/pafos-fc-olympiacos-fc/VobsBHtb#id:14566868',
    'https://www.sofascore.com/pt/football/match/sk-slavia-praha-bodoglimt/gnsqU#id:14566884',
    'https://www.sofascore.com/pt/football/match/afc-ajax-inter/Xdbsdjb#id:14566873',
    'https://www.sofascore.com/pt/football/match/fc-bayern-munchen-chelsea/Nsxdb#id:14566570',
    'https://www.sofascore.com/pt/football/match/atletico-madrid-liverpool/UsLgb#id:14566637',
    'https://www.sofascore.com/pt/football/match/atalanta-paris-saint-germain/UHsLdb#id:14566657',
    'https://www.sofascore.com/pt/football/match/river-plate-palmeiras/nOslob#id:14508122',
    'https://www.sofascore.com/pt/football/match/bolivar-atletico-mineiro/COsjQc#id:14249746',
    'https://www.sofascore.com/pt/football/match/independiente-del-valle-once-caldas/txcsyUp#id:14249735',
    'https://www.sofascore.com/pt/football/match/feyenoord-fortuna-sittard/hjbsjjb#id:14053685',
    'https://www.sofascore.com/pt/football/match/fatih-karagumruk-basaksehir-fk/LlbseZb#id:14613411',
    'https://www.sofascore.com/pt/football/match/alanyaspor-fenerbahce/clbsmCc#id:14613405',
    'https://www.sofascore.com/pt/football/match/kasimpasa-samsunspor/dlbsnwc#id:14613415',
    'https://www.sofascore.com/pt/football/match/rc-sporting-charleroi-krc-genk/PhbsYhb#id:14607840',
    'https://www.sofascore.com/pt/football/match/swansea-city-nottingham-forest/ozb#id:14598952',
    'https://www.sofascore.com/pt/football/match/karadeniz-eregli-belediyespor-bursaspor/flbsFOje#id:14647056',
    'https://www.sofascore.com/pt/football/match/dersimspor-sanliurfaspor/PlbsODH#id:14647050',
    'https://www.sofascore.com/pt/football/match/karaman-fk-kahramanmarasspor/RlbsTDH#id:14645407',
    'https://www.sofascore.com/pt/football/match/malatya-yesilyurt-belediyespor-adana-01-fk/bjUdsXWGg#id:14645419',
    'https://www.sofascore.com/pt/football/match/mazidagi-fosfat-sk-mus-1984-musspor/nQtdsoPje#id:14645412',
    'https://www.sofascore.com/pt/football/match/1461-trabzon-amasyaspor-fk/oNHbsYQtc#id:14645418',
    'https://www.sofascore.com/pt/football/match/denizli-idman-yurdu-antalya-kepezspor/MmYdsebGg#id:14645424',
    'https://www.sofascore.com/pt/football/match/erbaaspor-pazarspor/BCcskDQb#id:14645403',
    'https://www.sofascore.com/pt/football/match/galata-sk-beykoz-anadolu-spors/YTWsVwie#id:14647058',
    'https://www.sofascore.com/pt/football/match/karabuk-idman-yurdu-fk-guzide-gebze-spor/ycGsrBEd#id:14645404',
    'https://www.sofascore.com/pt/football/match/fatsa-belediyespor-kastamonuspor/OdpsWQtc#id:14645429',
    'https://www.sofascore.com/pt/football/match/belediye-kutahyaspor-altinordu/ZUjsVDH#id:14645416',
    'https://www.sofascore.com/pt/football/match/kucukcekmece-sinop-spor-inegolspor/opcsWiUd#id:14645400',
    'https://www.sofascore.com/pt/football/match/silifke-belediyespor-kahramanmaras-istiklalspor/gnFgsJnFg#id:14647052',
    'https://www.sofascore.com/pt/football/match/beyoglu-yeni-carsi-fas-silivrispor/CcGsgitb#id:14645415',
    'https://www.sofascore.com/pt/football/match/somaspor-cankaya-fk/UAWsBkFc#id:14645417',
    'https://www.sofascore.com/pt/football/match/corluspor-1947-ayvalikgucu-belediyespor/WTWsNXWd#id:14647057',
    'https://www.sofascore.com/pt/football/match/siran-yildizspor-sivasspor/BlbshwQg#id:14645401',
    'https://www.sofascore.com/pt/football/match/12-bingolspor-elazigspor/tlbsoHub#id:14645423',
    'https://www.sofascore.com/pt/football/match/1926-bulancakspor-24-erzincanspor/bRtcsFPje#id:14645409',
    'https://www.sofascore.com/pt/football/match/yalova-fk-77-sk-balikesirspor/MdpsZNje#id:14645408',
    'https://www.sofascore.com/pt/football/match/bornova-1877-aliaga-fk/TiUdsYiUd#id:14647054',
    'https://www.sofascore.com/pt/football/match/bursa-yildirimspor-karsiyaka/ZObskdGc#id:14645405',
    'https://www.sofascore.com/pt/football/match/anadolu-universitesi-menemen-fk/fMrsajUd#id:14645426',
    'https://www.sofascore.com/pt/football/match/nigde-belediyesi-spor-68-aksarayspor/LTWsEdvg#id:14647051',
    'https://www.sofascore.com/pt/football/match/sebat-genclik-spor-orduspor-1967/qpfdsdjUd#id:14645402',
    'https://www.sofascore.com/pt/football/match/tigres-uanl-cd-guadalajara/NNsPN#id:13984769',
    'https://www.sofascore.com/pt/football/match/new-york-city-fc-columbus-crew/eabsTcAb#id:14387685',
    'https://www.sofascore.com/pt/football/match/los-angeles-fc-real-salt-lake/IccsaTjc#id:14037794',
    'https://www.sofascore.com/pt/football/match/gv-san-jose-academia-de-balompie-boliviano/Nnudstfce#id:14671973',
    'https://www.sofascore.com/pt/football/match/nacional-potosi-jorge-wilstermann/Dhdsaeu#id:14671449',
    'https://www.sofascore.com/pt/football/match/club-atletico-tembetary-cerro-porteno/QucsBnCc#id:14636105',
    'https://www.sofascore.com/pt/football/match/alianza-atletico-de-sullana-club-sporting-cristal/cWshW#id:14558493',
    'https://www.sofascore.com/pt/football/match/comerciantes-unidos-sport-huancayo/VCnsjxKb#id:14558497',
    'https://www.sofascore.com/pt/football/match/sport-boys-cienciano/bWsmW#id:14558496',
    'https://www.sofascore.com/pt/football/match/newells-old-boys-belgrano/dobsmob#id:14557466',
    'https://www.sofascore.com/pt/football/match/chengdu-rongcheng-ulsan-hd/dddsQzNb#id:14469213',
    'https://www.sofascore.com/pt/football/match/shanghai-port-vissel-kobe/WmbsMFq#id:14469214',
    'https://www.sofascore.com/pt/football/match/kfum-oslo-kongsvinger/xnsLp#id:14592306',
    'https://www.sofascore.com/pt/football/match/sk-brann-mjondalen/dtsjy#id:14592303',
    'https://www.sofascore.com/pt/football/match/sandefjord-fotball-tromsdalen-uil/unsBn#id:14592297',
    'https://www.sofascore.com/pt/football/match/austin-fc-minnesota-united/tHqsyjbd#id:14148833',
    'https://www.sofascore.com/pt/football/match/aab-fc-midtjylland/OAsPA#id:14658176',
    'https://www.sofascore.com/pt/football/match/if-lyseng-fc-roskilde/yXbsKTi#id:14658179',
    'https://www.sofascore.com/pt/football/match/kolding-bk-silkeborg-if/KAsITi#id:14664353',
    'https://www.sofascore.com/pt/football/match/lnz-cherkasy-fc-metalist-kharkiv/oqbsCNnc#id:14573517',
    'https://www.sofascore.com/pt/football/match/fc-chernihiv-fc-kryvbas-kryvyi-rih/YFrcsmlFc#id:14578562',
    'https://www.sofascore.com/pt/football/match/polissya-stavky-shakhtar-donetsk/nqbsDLFc#id:14573509',
    'https://www.sofascore.com/pt/football/match/fc-livyi-bereh-kyiv-victoria-sumy/Txkcsjofd#id:14573514',
    'https://www.sofascore.com/pt/football/match/oleksandria-dynamo-kyiv/fqbsEjk#id:14573513',
    'https://www.sofascore.com/pt/football/match/bk-olympic-malmo-ff/RMsmgu#id:14613077',
    'https://www.sofascore.com/pt/football/match/akhmat-grozny-zenit-st-petersburg/wWsGcc#id:14117945',
    'https://www.sofascore.com/pt/football/match/fk-krasnodar-krylya-sovetov-samara/xWsANn#id:14117949',
    'https://www.sofascore.com/pt/football/match/fc-sochi-dynamo-moscow/pWsJW#id:14117948',
    'https://www.sofascore.com/pt/football/match/ae-kifisia-asteras-aktor/RBcszeY#id:14609086',
    'https://www.sofascore.com/pt/football/match/panetolikos-aris-thessaloniki/cpbsePc#id:14609089',
    'https://www.sofascore.com/pt/football/match/aek-athens-ao-egaleo/Uobsapb#id:14609087',
    'https://www.sofascore.com/pt/football/match/apo-ellas-syrou-aps-atromitos-athinon/mbcsOvvd#id:14609090',
    'https://www.sofascore.com/pt/football/match/ao-kavala-ofi-crete/Qobsmpb#id:14609093',
    'https://www.sofascore.com/pt/football/match/apo-levadiakos-paok/bpbsnbc#id:14609084',
    'https://www.sofascore.com/pt/football/match/athens-kallithea-fc-panathinaikos/Yobsfpb#id:14609091',
    'https://www.sofascore.com/pt/football/match/gs-marko-ae-larisa/ppbsrLXb#id:14613038'
]

# ------------- tabelão de substituições de campeonatos ------------- #
substituicoes_campeonatos = {
    'Brasileirão Betano': 'Brasileiro',
    'Copa Betano do Brasil': 'Copa do Brasil',
    'FIFA Club World Cup': 'Copa do Mundo de Clubes',
    'Brasileiro Série B': 'Brasileiro - Série B',
    'CONMEBOL Libertadores': 'Libertadores',
    'CONMEBOL Sudamericana': 'Sul-Americana',
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
    'Copa Colombia': 'Copa da Colombia',
    'Liga 1': 'Peruano',
    'Premier Division': 'Irlandês',
    'LigaPro Serie A': 'Equatoriano',
    'Eliteserien': 'Norueguês',
    'Copa Argentina': 'Copa da Argentina',
    'Gaucho': 'Gaúcho',
    'Allsvenskan': 'Sueco',
    'Supercopa Internacional': 'Supercopa da Argentina',
    'Liga Profesional de Fútbol': 'Argentino',
    'Copa Chile': 'Copa do Chile',
    'Copa Ecuador': 'Copa do Equador',
    'Supercopa': 'Supercopa do Equador',
    'Leinster Senior Cup': 'Copa da Liga da Irlanda',
    'FAI Presidents Cup': 'Supercopa da Irlanda',
    'Emperor Cup': 'Copa do Imperador',
    'J. League Cup': 'Copa da Liga do Japão',
    'FAI Cup': 'Copa da Irlanda',
    'División de Honor': 'Paraguaio',
    'Supercopa Uruguaya': 'Supercopa do Uruguai',
    'Supercopa Venezuela': 'Supercopa da Venezuela',
    'Community Shield': 'Supercopa da Inglaterra',
    'FA Cup': 'Copa da Inglaterra',
    'EFL Cup': 'Copa da Liga Inglesa',
    'LaLiga': 'La Liga',
    'Mineiro Módulo I - Classificação contra o rebaixamento': 'Mineiro',
    'Carioca Série A – Mata-mata': 'Carioca',
    'Carioca - Taça Rio': 'Carioca',
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
    'Beker van Belgie': 'Copa da Bélgica',
    'Primera LPF': 'Argentino',
    'World Cup Qualificação': 'Eliminatórias',
    'Int. Friendly Games': 'Amistoso',
    'CONMEBOL Copa América': 'Copa América',
    'UEFA Nations League': 'Nations League',
    'Euro': 'Eurocopa',
    'World Cup Qual. UEFA L': 'Eliminatórias',
    'World Cup Qual. UEFA K': 'Eliminatórias',
    'World Cup Qual. UEFA J': 'Eliminatórias',
    'World Cup Qual. UEFA H': 'Eliminatórias',
    'World Cup Qual. UEFA A': 'Eliminatórias',
    'World Cup Qual. UEFA G': 'Eliminatórias',
    'World Cup Qual. UEFA E': 'Eliminatórias',
    'World Cup Qual. UEFA B': 'Eliminatórias',
    'World Cup Qual. UEFA C': 'Eliminatórias',
    'World Cup Qual. UEFA D': 'Eliminatórias',
    'World Cup Qual. UEFA F': 'Eliminatórias',
    'World Cup Qual. UEFA I': 'Eliminatórias',
    'FIFA World Cup': 'Copa do Mundo',
    'World Cup Qualification': 'Eliminatórias',
    'CONCACAF Gold Cup': 'Copa Ouro',
    'FIFA Intercontinental Cup': 'Mundial de Clubes',
    'Primera División': 'Paraguaio',
    'Liga Profesional': 'Argentino',
    'Russian Cup Regions Path': 'Copa da Rússia',
    'Copa de la Liga': 'Copa da Liga Argentina',
    'VriendenLoterij Eredivisie': 'Eredivisie',
    'Mineiro Módulo I - Playoffs': 'Mineiro',
    'Paulista Série A1 - Playoffs': 'Paulista',
    'Paulista Série A1 - Troféu do Interior - Playoffs': 'Paulista',
    'Cearense - 2ª fase': 'Cearense',
    'Carioca – Taça Rio': 'Carioca',
    'Copa Uruguay': 'Copa do Uruguai',
    'Brasileirão Série B': 'Brasileiro - Série B',
}

# ─────────────────────────────── LOGGING ─────────────────────────────── #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("sofascore")

# ─────────────────────── sessão HTTP (ignora proxies do ambiente) ─────────────────────── #
session: requests.Session | None = None

def rebuild_session():
    """Cria/recria a sessão de requests, ignorando HTTP(S)_PROXY do ambiente.
    Injeta proxy apenas se USAR_PROXY=True."""
    global session
    try:
        if session:
            session.close()
    except Exception:
        pass
    session = requests.Session()
    session.trust_env = False  # <— ignora variáveis de ambiente de proxy
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.sofascore.com",
        "Referer": "https://www.sofascore.com/",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    })
    if USAR_PROXY and proxy_host and proxy_port:
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        session.proxies.update({"http": proxy_url, "https": proxy_url})
        log.info("🌐 Proxy requests habilitado: %s", proxy_url)

# ─────────────────────── helpers de proxy ─────────────────────── #
def desativar_proxy_instavel(motivo: str):
    """Desliga proxy (requests + selenium) e reinicia o driver."""
    global USAR_PROXY, PROXY_FAIL_STRIKES, session, driver, matches_done_with_this_driver
    if not USAR_PROXY:
        return
    USAR_PROXY = False
    PROXY_FAIL_STRIKES = 0
    rebuild_session()  # garante que o requests pare de usar proxy do ambiente
    log.warning("🛑 Proxy desativado automaticamente (%s). Reiniciando Chrome sem proxy...", motivo)
    try:
        driver.quit()
    except Exception:
        pass
    driver = criar_driver()  # respeita USAR_PROXY=False
    matches_done_with_this_driver = 0

# ──────────────────────── CHROME DRIVER HELPERS ──────────────────────── #
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
    # endurecimentos anti-bloqueio
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # carregar DOM mais rápido
    opts.page_load_strategy = 'eager'
    # reduzir carga (imagens off)
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2
    })
    # >>> PROXY NO CHROME (cobre http e https)
    if USAR_PROXY and proxy_host and proxy_port:
        proxy_spec = f"http={proxy_host}:{proxy_port};https={proxy_host}:{proxy_port}"
        opts.add_argument(f"--proxy-server={proxy_spec}")
        log.info("🌐 Proxy Chrome habilitado: %s", proxy_spec)

    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(30)
    drv.set_script_timeout(20)
    # remove navigator.webdriver
    try:
        drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return drv

# cria sessão requests e driver
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

# ───────────────────────── BASE DE DADOS ─────────────────────────────── #
conn = pymysql.connect(host="localhost", user="admin", password="1234",
                       database="bet_dados", charset="utf8mb4")
cursor = conn.cursor()

# ────────────────────── UTIL: BUSCAR 'event' NO DOM ───────────────────── #
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
                return r
    return None

def extract_event_from_dom(drv: webdriver.Chrome, id_jogo: str) -> Dict | None:
    try:
        script_tag = drv.find_element(By.CSS_SELECTOR, "script#__NEXT_DATA__")
    except Exception:
        return None
    try:
        data = json.loads(script_tag.get_attribute("innerHTML"))
    except json.JSONDecodeError:
        return None
    evt = _deep_find(data, "event")
    if isinstance(evt, dict) and str(evt.get("id")) == str(id_jogo):
        return evt
    return None

# ──────────────────── REQUISIÇÃO À API COM FALLBACKS ──────────────────── #
def _selenium_api_json(url_api: str) -> Dict:
    """Abre a URL da API no navegador e tenta capturar o JSON da página."""
    global PROXY_FAIL_STRIKES
    try:
        driver.get(url_api)
    except WebDriverException as e:
        # erros típicos de proxy/RESET contam strike
        if "ERR_CONNECTION_RESET" in str(e):
            PROXY_FAIL_STRIKES += 1
            if USAR_PROXY and PROXY_FAIL_STRIKES >= PROXY_FAIL_LIMIT:
                desativar_proxy_instavel("ERR_CONNECTION_RESET no Selenium")
        raise

    try:
        # 1) tentar <pre>
        try:
            el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
            txt = el.text.strip()
            if txt:
                return json.loads(txt)
        except TimeoutException:
            pass

        # 2) tentar body.innerText
        try:
            txt = driver.find_element(By.TAG_NAME, "body").text.strip()
            if txt.startswith("{") or txt.startswith("["):
                return json.loads(txt)
        except Exception:
            pass

        # 3) tentar page_source
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

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, ValueError, TimeoutException, WebDriverException),
    factor=2, max_time=90
)
def obter_dados_api(url_api: str) -> Dict:
    """Tenta API com requests; se falhar por qualquer motivo, usa Selenium."""
    global PROXY_FAIL_STRIKES
    # sincroniza cookies (ajuda no 403)
    try:
        session.cookies.clear()
        for c in driver.get_cookies():
            session.cookies.set(c["name"], c["value"])
    except Exception:
        pass

    try:
        resp = session.get(url_api, timeout=20)
    except requests.exceptions.RequestException as e:
        # Contabiliza falha de proxy e tenta Selenium imediatamente
        PROXY_FAIL_STRIKES += 1
        if USAR_PROXY and PROXY_FAIL_STRIKES >= PROXY_FAIL_LIMIT:
            desativar_proxy_instavel(f"Falhas consecutivas no requests: {type(e).__name__}")
        return _selenium_api_json(url_api)

    if resp.status_code == 403:
        return _selenium_api_json(url_api)

    try:
        resp.raise_for_status()
        return resp.json()
    except Exception:
        # conteúdo não-JSON, 5xx etc → Selenium
        return _selenium_api_json(url_api)

def safe_get_event(id_jogo: str) -> Dict:
    for tentativa in range(4):
        try:
            data = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}")
            if "event" in data:
                return data["event"]
        except Exception as e:
            log.debug("API falhou (%s) – tentando DOM", e)

        dom_evt = extract_event_from_dom(driver, id_jogo)
        if dom_evt:
            return dom_evt

        log.warning("Tentativa %d – não consegui 'event'", tentativa + 1)
        time.sleep(4 + random.random() * 2)

    # última cartada: Selenium direto na rota
    try:
        return _selenium_api_json(f"https://api.sofascore.com/api/v1/event/{id_jogo}")["event"]
    except Exception:
        pass
    raise RuntimeError(f"Não consegui 'event' para {id_jogo}")

# ───────────────────── FUNÇÕES DE EXTRAÇÃO (Selenium) ──────────────────── #
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

def get_odds(drv: webdriver.Chrome, max_scrolls: int = 6, wait_each: float = 0.7) -> Tuple[str | None, str | None, str | None]:
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

# ──────────────── AUXILIARES BD (substituir + estatísticas) ────────────── #
def substituir_campeonato(n: str) -> str:
    return substituicoes_campeonatos.get(n.strip(), n.strip())

def inserir_estatisticas(id_jogo: str, home: str, away: str,
                         periodo: str, tabela: str, stats: dict):
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

# ──────────────────────── TEMPO DOS GOLS ────────────────────────── #
def _disp(x):
    return x.get("display", 0) if isinstance(x, dict) else (x or 0)

def inserir_tempo_gols(id_jogo: str, campeonato_padronizado: str, home: str, away: str):
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
        log.info("⚠️  Sem gols (provável 0x0) – não inserido em tempo_gols.")
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

# ───────────────────────────── LOOP PRINCIPAL ───────────────────────────── #
for idx, url in enumerate(urls, start=1):
    if not url.strip():
        continue
    if matches_done_with_this_driver >= MAX_MATCHES_PER_DRIVER:
        restart_driver()

    log.info("⚽  (%d/%d) %s", idx, len(urls), url)
    try:
        id_jogo = re.search(r"#id:(\d+)", url).group(1)

        # país + odds (resiliente)
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

        # evento
        ev = safe_get_event(id_jogo)
        home_team, away_team = ev["homeTeam"], ev["awayTeam"]
        home = substituicoes_clubes.substituicoes_por_id.get(home_team["id"], home_team["name"].strip())
        away = substituicoes_clubes.substituicoes_por_id.get(away_team["id"], away_team["name"].strip())
        log.info("Partida: %s x %s | odds %s/%s/%s", home, away, odd_casa, odd_empate, odd_fora)

        # INSERT / UPDATE em 'partidas'
        cursor.execute("SELECT pais, odd_casa, odd_empate, odd_fora, campeonato FROM partidas WHERE id_jogo=%s",
                       (id_jogo,))
        row = cursor.fetchone()
        campeonato_padronizado = None

        if not row:
            dt_ini = datetime.datetime.fromtimestamp(ev["startTimestamp"], tz=datetime.timezone.utc)\
                                       .astimezone(datetime.timezone(datetime.timedelta(hours=-3)))
            data_str, hora_str = dt_ini.strftime("%d/%m/%Y"), dt_ini.strftime("%H:%M")
            campeonato_padronizado = substituir_campeonato((ev["tournament"]["name"] or "").split(",")[0])
            round_info = ev.get("roundInfo") or {}
            rodada = re.search(r"\d+", str(round_info.get("name") or round_info.get("round") or "0"))
            rodada = rodada.group() if rodada else "0"
            gols_casa = ev["homeScore"].get("normaltime", ev["homeScore"].get("current", 0))
            gols_fora = ev["awayScore"].get("normaltime", ev["awayScore"].get("current", 0))

            cursor.execute("""
                INSERT INTO partidas
                (id_jogo,data,hora,pais,campeonato,rodada,casa,fora,
                 gols_casa,gols_fora,temporada,odd_casa,odd_empate,odd_fora,cadastrado_em)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (id_jogo, data_str, hora_str, pais, campeonato_padronizado, rodada, home, away,
                  gols_casa, gols_fora, temporada, odd_casa, odd_empate, odd_fora,
                  datetime.datetime.now()))
            conn.commit()
            log.info("✅  Partida inserida")
        else:
            _, _, _, _, camp_existente = row
            campeonato_padronizado = camp_existente

            sets, vals = [], []
            if pais != "Desconhecido" and not row[0]:
                sets.append("pais=%s");           vals.append(pais)
            if odd_casa and not row[1]:
                sets.append("odd_casa=%s");       vals.append(odd_casa)
            if odd_empate and not row[2]:
                sets.append("odd_empate=%s");     vals.append(odd_empate)
            if odd_fora and not row[3]:
                sets.append("odd_fora=%s");       vals.append(odd_fora)
            if not campeonato_padronizado:
                campeonato_padronizado = substituir_campeonato((ev["tournament"]["name"] or "").split(",")[0])
                sets.append("campeonato=%s");     vals.append(campeonato_padronizado)
            if sets:
                vals.append(id_jogo)
                cursor.execute(f"UPDATE partidas SET {', '.join(sets)} WHERE id_jogo=%s", vals)
                conn.commit()
                log.info("✅  Partida atualizada")

        # estatísticas (blindado)
        try:
            stats = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}/statistics")
            inserir_estatisticas(id_jogo, home, away, "1ST", "1_tempo", stats)
            inserir_estatisticas(id_jogo, home, away, "2ND", "2_tempo", stats)
            inserir_estatisticas(id_jogo, home, away, "ALL", "estatisticas_partidas", stats)
        except Exception as e:
            log.warning("⚠️  Não consegui estatísticas (%s). Seguindo.", e)

        # jogadores (sem 'mando', blindado)
        try:
            cursor.execute("SELECT 1 FROM estatisticas_jogadores WHERE id_jogo=%s LIMIT 1", (id_jogo,))
            if not cursor.fetchone():
                lineups = obter_dados_api(f"https://api.sofascore.com/api/v1/event/{id_jogo}/lineups")
                players = []
                for side, team in [("home", home), ("away", away)]:
                    for p in lineups.get(side, {}).get("players", []):
                        ply, st = p.get("player", {}), p.get("statistics", {})
                        rp = {
                            "id_jogo": id_jogo, "team": team, "name": ply.get("name"),
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
                    log.info("✅  Jogadores inseridos: %d", len(players))
        except Exception as e:
            log.warning("⚠️  Não consegui lineups (%s). Seguindo.", e)

        # >>> TEMPO DOS GOLS
        try:
            inserir_tempo_gols(id_jogo, campeonato_padronizado, home, away)
        except Exception as e:
            log.warning("⚠️  Não consegui tempo_gols (%s). Seguindo.", e)

    except Exception as e:
        if "Timed out receiving message from renderer" in str(e):
            log.warning("🧹 Renderer travou — reiniciando driver para recuperar.")
            restart_driver()
        log.error("❌  Erro em %s: %s", url, e, exc_info=True)

    matches_done_with_this_driver += 1
    time.sleep(random.uniform(4, 8))

# ──────────────────────────── ENCERRAMENTO ─────────────────────────────── #
try:
    driver.quit()
except Exception:
    pass
try:
    cursor.close()
    conn.close()
except Exception:
    pass
log.info("✅✅✅  Processo finalizado.")
