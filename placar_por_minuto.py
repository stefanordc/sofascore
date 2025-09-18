from __future__ import annotations
import concurrent.futures as cf
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

# ───── NOVAS VARIÁVEIS ─────
campeonato = "Serie A"
temporada  = "2020/2021"
# ───────────────────────────

# ─────────── SUAS URLs ───────────
URLS: List[str] = [
    'https://www.sofascore.com/pt/football/match/flamengo-palmeiras/nOsGuc#id:10987608'
]
# ---------------------------------

# ───── CONFIGURAÇÕES ─────
MAX_WORKERS       = 10
WAIT_CLOUDFLARE_S = 3
SAVE_CSV          = False
SAVE_XLSX         = True
DESKTOP           = Path.home() / "Desktop"
CSV_PATH          = DESKTOP / "gols_sofascore.csv"
XLSX_PATH         = DESKTOP / "gols_sofascore.xlsx"
SHOW_IN_TERMINAL  = False
# ─────────────────────────

os.environ["WDM_LOG_LEVEL"] = "0"            # silencia webdriver-manager

ID_RE  = re.compile(r"#id:(\d+)")
_LOCK  = threading.Lock()


def _chrome_major() -> Optional[int]:
    for root in ("HKCU", "HKLM"):
        try:
            out = subprocess.check_output(
                ["reg", "query",
                 fr"{root}\SOFTWARE\Google\Chrome\BLBeacon", "/v", "version"],
                text=True, stderr=subprocess.DEVNULL,
            )
            return int(re.search(r"(\d+)\.", out).group(1))
        except Exception:
            continue
    return None


CHROME_MAJOR = _chrome_major()
uc.Chrome.__del__ = lambda *_: None          # evita WinError 6


def new_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1920,1080")
    with _LOCK:
        if CHROME_MAJOR:
            return uc.Chrome(options=opts, version_main=CHROME_MAJOR)
        return uc.Chrome(options=opts)


def new_session() -> requests.Session:
    drv = new_driver()
    try:
        drv.get("https://www.sofascore.com/")
        time.sleep(WAIT_CLOUDFLARE_S)
        ua = drv.execute_script("return navigator.userAgent;")
        cookies = {c["name"]: c["value"] for c in drv.get_cookies()}
    finally:
        drv.quit()

    ses = requests.Session()
    ses.headers.update(
        {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.sofascore.com/",
            "Origin": "https://www.sofascore.com",
            "x-fsign": cookies.get("fsign"),
        }
    )
    for k, v in cookies.items():
        ses.cookies.set(k, v, domain=".sofascore.com")
    return ses


SESSION = new_session()


def disp(x):
    return x.get("display", 0) if isinstance(x, dict) else (x or 0)


def fetch_json(url: str) -> Dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 403:
        return renew_fsign(url)
    r.raise_for_status()
    return r.json()


def renew_fsign(url: str) -> Dict:
    drv = new_driver()
    try:
        drv.get(url)
        time.sleep(WAIT_CLOUDFLARE_S)
        data = json.loads(drv.find_element(By.TAG_NAME, "pre").text)
        fsign = next((c["value"] for c in drv.get_cookies() if c["name"] == "fsign"),
                     None)
        if fsign:
            with _LOCK:
                SESSION.headers["x-fsign"] = fsign
                SESSION.cookies.set("fsign", fsign, domain=".sofascore.com")
        return data
    finally:
        drv.quit()


def parse_match(link: str) -> pd.DataFrame:
    gid = int(ID_RE.search(link).group(1))
    evt   = fetch_json(f"https://api.sofascore.com/api/v1/event/{gid}")["event"]
    teams = {"home": evt["homeTeam"]["name"], "away": evt["awayTeam"]["name"]}

    incs = fetch_json(f"https://api.sofascore.com/api/v1/event/{gid}/incidents")["incidents"]

    rows = []
    for inc in incs:
        if inc.get("incidentType") != "goal":
            continue

        home_s, away_s = disp(inc["homeScore"]), disp(inc["awayScore"])
        for ac in inc.get("footballPassingNetworkAction") or [inc]:
            if ac.get("eventType", inc["incidentType"]) == "goal":
                rows.append(
                    {
                        "id_jogo":   gid,
                        "temporada": temporada,
                        "campeonato": campeonato,
                        "clube_gol": teams["home"] if ac.get("isHome") else teams["away"],
                        "minutos":   ac.get("time"),
                        "forma_gol": ac.get("bodyPart"),
                        "placar":    f"{home_s}-{away_s}",
                    }
                )
                break

    if rows:
        print(f"✔︎ {gid} ok")
        return pd.DataFrame(rows)
    else:
        print(f"⚠︎ {gid} 0×0 (ignorado)")
        return pd.DataFrame()


def main(links: List[str]) -> None:
    if not links:
        print("Nenhuma URL fornecida.")
        return

    print(f"⏳ Processando {len(links)} partidas…")

    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        dfs = list(pool.map(parse_match, links))

    df = pd.concat(dfs, ignore_index=True)
    if df.empty:
        print("Nenhum gol encontrado!")
        return

    # garante a ordem de colunas exigida
    colunas = ["id_jogo", "temporada", "campeonato",
               "clube_gol", "minutos", "forma_gol", "placar"]
    df = df[colunas].sort_values(["id_jogo", "minutos"], ignore_index=True)

    if SHOW_IN_TERMINAL:
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", None)
        print("\n=============== RESULTADO ===============")
        print(f"Total de linhas: {len(df)}\n")
        print(df.to_string(index=False, max_rows=None))

    if SAVE_CSV:
        df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"CSV salvo em: {CSV_PATH}")

    if SAVE_XLSX:
        df.to_excel(XLSX_PATH, index=False, engine="openpyxl")
        print(f"Excel salvo em: {XLSX_PATH}")


if __name__ == "__main__":
    main(URLS)
