import os
import time
import argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE = "https://www.ogol.com.br"
DEFAULT_URL = (
    "https://www.ogol.com.br/edicao/campeonato-nacional-de-clubes-1973/2482/"
    "calendario?equipa=0&estado=&filtro=&op=calendario&page=1"
)


def build_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": BASE,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def fetch_html(session: requests.Session, url: str, retries: int = 3, backoff: float = 1.0) -> str:
    last_exc = None
    for i in range(retries):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"Falha ao baixar {url}: {last_exc}")


def parse_rows(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, str]] = []

    for tr in soup.select("tbody tr.parent"):
        date_td = tr.select_one("td.date")
        home_td = tr.select_one("td.text.home")
        result_td = tr.select_one("td.result")
        away_td = tr.select_one("td.text.away")

        date_str = date_td.get_text(strip=True) if date_td else ""
        home = home_td.get_text(strip=True) if home_td else ""
        score = result_td.get_text(strip=True) if result_td else ""
        away = away_td.get_text(strip=True) if away_td else ""

        if date_str and home and score and away:
            rows.append(
                {
                    "data": date_str,
                    "mandante": home,
                    "placar": score,
                    "visitante": away,
                }
            )

    return rows


def get_max_pages_from_numbers(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "lxml")
    numbers = soup.select_one("div.zz-pagination div.numbers")
    if not numbers:
        return None

    pages = []
    for el in numbers.select("a.link, span.link"):
        txt = el.get_text(strip=True)
        if txt.isdigit():
            pages.append(int(txt))
    return max(pages) if pages else None


def set_url_page(url: str, page: int) -> str:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q["page"] = [str(page)]
    normalized = {k: (v[0] if isinstance(v, list) and len(v) == 1 else v) for k, v in q.items()}
    new_query = urlencode(normalized, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def crawl_all_pages(start_url: str, delay: float = 0.8) -> List[Dict[str, str]]:
    with requests.Session() as s:
        s.headers.update(build_headers())

        html1 = fetch_html(s, start_url)
        data = parse_rows(html1)

        max_pages = get_max_pages_from_numbers(html1) or 1

        for p in range(2, max_pages + 1):
            url_p = set_url_page(start_url, p)
            time.sleep(delay)
            html = fetch_html(s, url_p)
            data.extend(parse_rows(html))

    return data


def salvar_excel_desktop(df: pd.DataFrame, nome_arquivo: str = "rascunho_jogos.xlsx") -> str:
    """
    Salva no Desktop do usuário (Windows/macOS/Linux) e retorna o caminho salvo.
    """
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    # Fallback se Desktop não existir (ex.: PT-BR 'Área de Trabalho')
    if not os.path.isdir(desktop):
        # Alguns Windows em PT-BR usam 'Área de Trabalho'
        alternativa = os.path.join(os.path.expanduser("~"), "Área de Trabalho")
        desktop = alternativa if os.path.isdir(alternativa) else os.path.expanduser("~")

    out_path = os.path.join(desktop, nome_arquivo)

    # Usa openpyxl como engine
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="jogos", index=False)

        # Ajuste simples de largura de coluna
        ws = writer.sheets["jogos"]
        for col_idx, col_name in enumerate(df.columns, start=1):
            # tamanho base pelo nome da coluna + conteúdo máximo
            max_len = max([len(str(col_name))] + [len(str(v)) for v in df[col_name].astype(str).values])
            ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 2, 50)

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Scraper oGol -> DataFrame e Excel (Desktop/rascunho_jogos.xlsx).")
    parser.add_argument("--url", default=DEFAULT_URL, help="URL inicial (página 1) da edição.")
    parser.add_argument("--out", default="", help="(Opcional) Caminho do CSV para salvar.")
    parser.add_argument("--delay", type=float, default=0.8, help="Delay entre páginas (segundos).")
    parser.add_argument("--show-all", action="store_true", help="Exibir todos os registros no terminal.")
    args = parser.parse_args()

    print(f"[1/4] Coletando dados a partir de: {args.url}")
    rows = crawl_all_pages(args.url, delay=args.delay)

    print(f"[2/4] Total de partidas coletadas: {len(rows)}")

    df = pd.DataFrame(rows, columns=["data", "mandante", "placar", "visitante"])

    # Exibir no terminal
    if args.show_all:
        with pd.option_context(
            "display.max_rows", None,
            "display.max_columns", None,
            "display.width", 0,
            "display.max_colwidth", None,
        ):
            print(df.to_string(index=False))
    else:
        print(df.head(15).to_string(index=False))
        if len(df) > 15:
            print(f"... ({len(df)-15} linhas ocultas; use --show-all para exibir tudo)")
    print(f"Shape do DataFrame: {df.shape}")

    # (Opcional) CSV
    if args.out:
        df.to_csv(args.out, index=False, encoding="utf-8-sig")
        print(f"[3/4] CSV salvo em: {args.out}")

    # Excel no Desktop com nome solicitado
    xlsx_path = salvar_excel_desktop(df, "rascunho_jogos.xlsx")
    print(f"[4/4] Excel salvo em: {xlsx_path}")


if __name__ == "__main__":
    main()
