#!/usr/bin/env python3
import argparse, requests, pandas as pd
from pathlib import Path
from datetime import date, timedelta

DATA = Path("data"); DATA.mkdir(exist_ok=True)

def get_pages(url, params=None, page_size=500):
    out, page = [], 1
    while True:
        q = dict(params or {}); q["pagina"] = page; q["tamanhoPagina"] = page_size
        r = requests.get(url, params=q, timeout=60); r.raise_for_status()
        js = r.json()
        if not js: break
        out.extend(js if isinstance(js, list) else [js]); page += 1
    return pd.json_normalize(out) if out else pd.DataFrame()

def compras_contratos(codigoOrgao, vmin=None, vmax=None, ultimos_dias=0, itens=False):
    if ultimos_dias:
        vmax = date.today().strftime("%Y-%m-%d")
        vmin = (date.today()-timedelta(days=ultimos_dias)).strftime("%Y-%m-%d")
    base = "https://dadosabertos.compras.gov.br/modulo-contratos"
    dfc = get_pages(f"{base}/1_consultarContratos",
                    {"codigoOrgao": codigoOrgao,
                     "dataVigenciaInicialMin": vmin,
                     "dataVigenciaInicialMax": vmax})
    dfc.to_csv(DATA/"compras_contratos.csv", index=False)
    try: dfc.to_parquet(DATA/"compras_contratos.parquet", index=False)
    except Exception: pass
    print(f"compras_contratos: {len(dfc)}")
    if itens and not dfc.empty and "numeroContrato" in dfc.columns:
        dfs=[]
        for num in dfc["numeroContrato"].dropna().astype(str).unique():
            dfi = get_pages(f"{base}/2_consultarContratosItem", {"numeroContrato": num})
            if not dfi.empty: dfi["numeroContrato"]=num; dfs.append(dfi)
        all_itens = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        all_itens.to_csv(DATA/"compras_itens.csv", index=False)
        try: all_itens.to_parquet(DATA/"compras_itens.parquet", index=False)
        except Exception: pass
        print(f"compras_itens: {len(all_itens)}")

if __name__=="__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["compras-contratos"])
    ap.add_argument("--codigoOrgao", required=True)
    ap.add_argument("--vigencia-min"); ap.add_argument("--vigencia-max")
    ap.add_argument("--ultimos-dias", type=int, default=0)
    ap.add_argument("--with-itens", action="store_true")
    a = ap.parse_args()
    compras_contratos(a.codigoOrgao, a.vigencia_min, a.vigencia_max, a.ultimos_dias, a.with_itens)
PY
chmod +x etl_pt_compras.py