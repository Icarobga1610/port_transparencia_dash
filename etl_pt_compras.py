#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, argparse, time, random
from datetime import date, timedelta
from pathlib import Path
import pandas as pd, requests
from dotenv import load_dotenv

load_dotenv()
DATA = Path("data"); DATA.mkdir(exist_ok=True)

def get_pages(url, params=None, page_size=300, start_page=1, max_retries=6,
              backoff_base=1.6, pause_between=0.5, timeout=60, headers=None, verbose=True):
    s = requests.Session()
    base_hdr = {"Accept": "application/json", "User-Agent": "curl/8"}
    if headers: base_hdr.update(headers)
    rows, page = [], start_page
    while True:
        q = dict(params or {}); q["pagina"] = page; q["tamanhoPagina"] = page_size
        attempt = 0
        while True:
            r = s.get(url, params=q, headers=base_hdr, timeout=timeout)
            if r.status_code in (429,500,502,503,504):
                attempt += 1
                if attempt > max_retries: r.raise_for_status()
                ra = r.headers.get("Retry-After")
                try: delay = float(ra) if ra is not None else backoff_base**attempt
                except ValueError: delay = backoff_base**attempt
                delay += random.uniform(0,0.25)
                if verbose: print(f"[retry {attempt}] http={r.status_code} delay={delay:.2f}s", flush=True)
                time.sleep(delay); continue
            r.raise_for_status(); break
        try: data = r.json()
        except ValueError: data = []
        if not data:
            if verbose: print(f"[done] page={page} (vazio)", flush=True)
            break
        if verbose: print(f"[page {page}] rows={len(data)}", flush=True)
        rows.extend(data if isinstance(data, list) else [data])
        page += 1; time.sleep(pause_between)
    return pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()

def compras_contratos(codigoOrgao, vmin=None, vmax=None, ultimos_dias=0, itens=False, page_size=300, verbose=True):
    if ultimos_dias:
        vmax = date.today().strftime("%Y-%m-%d")
        vmin = (date.today()-timedelta(days=ultimos_dias)).strftime("%Y-%m-%d")
    base = "https://dadosabertos.compras.gov.br/modulo-contratos"
    if verbose: print(f"-> contratos: orgao={codigoOrgao} vmin={vmin} vmax={vmax}", flush=True)
    dfc = get_pages(f"{base}/1_consultarContratos",
                    params={"codigoOrgao":codigoOrgao,"dataVigenciaInicialMin":vmin,"dataVigenciaInicialMax":vmax},
                    page_size=page_size, verbose=verbose)
    (DATA/"compras_contratos.csv").write_text("") if dfc.empty else dfc.to_csv(DATA/"compras_contratos.csv", index=False)
    try: dfc.to_parquet(DATA/"compras_contratos.parquet", index=False)
    except Exception: pass
    print(f"compras_contratos: {len(dfc)}", flush=True)
    if itens and not dfc.empty and "numeroContrato" in dfc.columns:
        print("-> itens por contrato…", flush=True)
        dfs=[]
        for num in dfc["numeroContrato"].dropna().astype(str).unique():
            dfi = get_pages(f"{base}/2_consultarContratosItem",
                            params={"numeroContrato":num}, page_size=page_size, verbose=verbose)
            if not dfi.empty: dfi["numeroContrato"]=num; dfs.append(dfi)
        dfi_all = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        (DATA/"compras_itens.csv").write_text("") if dfi_all.empty else dfi_all.to_csv(DATA/"compras_itens.csv", index=False)
        try: dfi_all.to_parquet(DATA/"compras_itens.parquet", index=False)
        except Exception: pass
        print(f"compras_itens: {len(dfi_all)}", flush=True)

def pt_despesas(mesAno, codigoIbge=None, page_size=100, verbose=True):
    key = os.getenv("PT_API_KEY"); 
    if not key: raise SystemExit("PT_API_KEY ausente no .env")
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/despesas"
    hdr = {"chave-api-dados": key}
    if verbose: print(f"-> despesas PT: mesAno={mesAno} codigoIbge={codigoIbge}", flush=True)
    df = get_pages(url, params={"mesAno":mesAno,"codigoIbge":codigoIbge}, page_size=page_size, headers=hdr, verbose=verbose)
    (DATA/"pt_despesas.csv").write_text("") if df.empty else df.to_csv(DATA/"pt_despesas.csv", index=False)
    try: df.to_parquet(DATA/"pt_despesas.parquet", index=False)
    except Exception: pass
    print(f"pt_despesas: {len(df)}", flush=True)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Compras.gov.br v3 + Portal da Transparência")
    ap.add_argument("cmd", choices=["compras-contratos","pt-despesas"])
    ap.add_argument("--codigoOrgao"); ap.add_argument("--vigencia-min", dest="vmin"); ap.add_argument("--vigencia-max", dest="vmax")
    ap.add_argument("--ultimos-dias", type=int, default=0); ap.add_argument("--with-itens", action="store_true")
    ap.add_argument("--page-size", type=int, default=300); ap.add_argument("--mesAno"); ap.add_argument("--codigoIbge")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    if args.cmd=="compras-contratos":
        if not args.codigoOrgao: raise SystemExit("--codigoOrgao é obrigatório")
        compras_contratos(args.codigoOrgao, args.vmin, args.vmax, args.ultimos_dias, args.with_itens, args.page_size, verbose=not args.quiet)
    else:
        if not args.mesAno: raise SystemExit("--mesAno é obrigatório (AAAAMM)")
        pt_despesas(args.mesAno, args.codigoIbge, page_size=100, verbose=not args.quiet)
