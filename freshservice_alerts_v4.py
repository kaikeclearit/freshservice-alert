import time
import requests
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('freshservice_alerts.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# ==========================================
# CONFIGURAÇÕES
# ==========================================
API_KEY = os.getenv("FRESHSERVICE_API_KEY")
DOMAIN = os.getenv("FRESHSERVICE_DOMAIN")
BASE_URL = f"https://{DOMAIN}/api/v2"
AUTH = (API_KEY, "X")

# Configurações de alerta
DAYS_WARN = int(os.getenv("DAYS_TO_WARN", 120))
DAYS_CRITICAL = int(os.getenv("DAYS_TO_CRITICAL", 90))
MAKE_URL = os.getenv("MAKE_WEBHOOK_URL")
EMAIL_TO = os.getenv("EMAIL_TO")
MAX_ASSETS = int(os.getenv("MAX_ASSETS", 0)) or None

# Assets excluídos
EXCLUDED_ASSETS = {
    "ASSET-96", "ASSET-97", "ASSET-952", "ASSET-953", "ASSET-954", 
    "ASSET-955", "ASSET-956", "ASSET-957", "ASSET-958", "ASSET-959", 
    "ASSET-960", "ASSET-961", "ASSET-962", "ASSET-963", "ASSET-964", 
    "ASSET-965", "ASSET-966", "ASSET-967", "ASSET-968", "ASSET-969", 
    "ASSET-970", "ASSET-971", "ASSET-972", "ASSET-973", "ASSET-974", 
    "ASSET-975", "ASSET-976", "ASSET-977", "ASSET-978", "ASSET-979", 
    "ASSET-981", "ASSET-683", "ASSET-682", "ASSET-681", "ASSET-680", 
    "ASSET-679", "ASSET-678", "ASSET-677", "ASSET-676", "ASSET-675",
    "ASSET-674", "ASSET-673", "ASSET-672", "ASSET-671", "ASSET-651",
    "ASSET-650", "ASSET-649", "ASSET-648", "ASSET-647", "ASSET-646",
    "ASSET-645", "ASSET-644", "ASSET-643", "ASSET-642", "ASSET-641",
    "ASSET-621", "ASSET-620", "ASSET-618", "ASSET-615", "ASSET-598",
    "ASSET-597", "ASSET-596", "ASSET-595", "ASSET-594", "ASSET-593",
    "ASSET-592", "ASSET-591", "ASSET-590", "ASSET-589", "ASSET-588",
    "ASSET-587", "ASSET-586", "ASSET-585", "ASSET-584", "ASSET-583",
    "ASSET-582", "ASSET-581", "ASSET-580", "ASSET-579", "ASSET-578",
    "ASSET-577", "ASSET-576", "ASSET-575", "ASSET-574", 
}

# ==========================================
# FUNÇÕES AUXILIARES
# ==========================================

def get_paged_results(endpoint: str, params: Optional[Dict] = None, desc: str = "Baixando dados") -> List[Dict]:
    if params is None: params = {}
    page, results = 1, []
    params["per_page"] = 100
    while True:
        params["page"] = page
        try:
            resp = requests.get(endpoint, auth=AUTH, params=params, timeout=20)
            if resp.status_code == 429:
                retry = int(resp.headers.get("Retry-After", 60))
                logger.warning(f"Rate Limit. Aguardando {retry}s...")
                time.sleep(retry)
                continue
            resp.raise_for_status()
            data = resp.json()
            key = next((k for k in data.keys() if isinstance(data[k], list)), None)
            if not key: break
            batch = data[key]
            if not batch: break
            results.extend(batch)
            if page % 5 == 0: logger.info(f"{desc}: Página {page} ({len(results)} itens)")
            if len(batch) < 100: break
            page += 1
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"Erro na página {page}: {e}")
            break
    return results

def get_asset_details(display_id: str) -> Dict:
    try:
        resp = requests.get(f"{BASE_URL}/assets/{display_id}", auth=AUTH, params={"include": "type_fields"}, timeout=15)
        return resp.json().get("asset", {}) if resp.status_code == 200 else {}
    except: return {}

def extract_fields_smart(type_fields: Dict) -> tuple:
    serial, expiry = None, None
    normalized = {k.lower(): v for k, v in type_fields.items() if v and str(v).lower() not in ['none', 'n/a', '']}
    
    serial_keywords = ["serial", "service_tag", "srie", "nmero", "imei", "asset_tag"]
    for kw in serial_keywords:
        found = next((k for k in normalized if kw in k), None)
        if found: 
            serial = normalized[found]
            break
            
    date_keywords = ["warranty_expiry", "expiry_date", "final_de_suporte", "support_end", "validade", "vencimento"]
    for kw in date_keywords:
        found = next((k for k in normalized if kw in k), None)
        if found:
            val = str(normalized[found])
            if len(val) > 8: 
                expiry = val
                break
    return serial, expiry

def parse_date(date_string: str) -> Optional[datetime]:
    if not date_string: return None
    try:
        return datetime.strptime(str(date_string)[:10], "%Y-%m-%d")
    except: return None

def categorize_alert(days: int) -> str:
    if days <= DAYS_CRITICAL: return "critical"
    if days <= 30: return "warning"
    return "info"

def clean(val):
    """Remove quebras de linha e espaços extras para não quebrar o Make/HTML"""
    if val is None: return "N/A"
    return str(val).strip().replace('\n', ' ').replace('\r', '')

def send_to_make(asset_alerts: List[Dict], contract_alerts: List[Dict]) -> bool:
    if not MAKE_URL: return False
    
    # Prepara listas limpas
    clean_assets = [{
        "asset_name": clean(a["Asset"]),
        "asset_tag": clean(a["Tag"]),
        "serial_number": clean(a["Serial"]),
        "contract_name": clean(a["Contrato"]) if a["Contrato"] else "Sem contrato",
        "expiry_date": a["Vencimento Real"],
        "days_remaining": a["Dias"],
        "alert_level": a["Nivel"]
    } for a in asset_alerts]

    clean_contracts = [{
        "contract_name": clean(c["contract_name"]),
        "contract_id": c["contract_id"],
        "vendor": clean(c["vendor"]),
        "end_date": c["end_date"],
        "days_remaining": c["days_remaining"],
        "alert_level": c["alert_level"]
    } for c in contract_alerts]

    # Sumário consolidado
    all_alerts = asset_alerts + contract_alerts
    payload = {
        "asset_alerts": clean_assets,
        "contract_alerts": clean_contracts,
        "summary": {
            "total_count": len(all_alerts),
            "critical_count": len([x for x in all_alerts if x.get("Nivel") == "critical" or x.get("alert_level") == "critical"]),
            "warning_count": len([x for x in all_alerts if x.get("Nivel") == "warning" or x.get("alert_level") == "warning"]),
            "info_count": len([x for x in all_alerts if x.get("Nivel") == "info" or x.get("alert_level") == "info"])
        },
        "recipient_email": EMAIL_TO,
        "generated_at": datetime.now().isoformat(),
        "config": {
            "days_warning_threshold": DAYS_WARN,
            "days_critical_threshold": DAYS_CRITICAL
        }
    }

    try:
        logger.info(f"🚀 Enviando {len(clean_assets)} ativos e {len(clean_contracts)} contratos para o Make...")
        r = requests.post(MAKE_URL, json=payload, timeout=60)
        r.raise_for_status()
        logger.info(f"✅ Sucesso! Status: {r.status_code}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao enviar para o Make: {e}")
        return False

# ==========================================
# FLUXO PRINCIPAL
# ==========================================

def main():
    logger.info("INICIANDO V4 - ATIVOS E CONTRATOS")
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    warning_limit = today + timedelta(days=DAYS_WARN)

    # 1. Dados iniciais
    assets_raw = get_paged_results(f"{BASE_URL}/assets", desc="Assets")
    contracts_raw = get_paged_results(f"{BASE_URL}/contracts", desc="Contratos")

    # 2. Analisar Contratos (Independente)
    contract_alerts = []
    asset_contract_map = {}
    
    for c in tqdm(contracts_raw, desc="Analisando Contratos"):
        c_id, name = c.get("id"), c.get("name")
        end_dt = parse_date(c.get("end_date"))
        
        # Alerta de contrato
        if end_dt and today <= end_dt <= warning_limit:
            contract_alerts.append({
                "contract_name": name,
                "contract_id": c_id,
                "vendor": c.get("vendor_name", "N/A"),
                "end_date": end_dt.strftime("%d/%m/%Y"),
                "days_remaining": (end_dt - today).days,
                "alert_level": categorize_alert((end_dt - today).days)
            })
        
        # Mapeamento para os assets
        assoc = get_paged_results(f"{BASE_URL}/contracts/{c_id}/associated-assets", desc=f"Assoc {name[:20]}")
        for a in assoc:
            asset_contract_map[a.get("id")] = {"name": name, "end": c.get("end_date")}

    # 3. Analisar Assets
    asset_alerts = []
    if MAX_ASSETS: assets_raw = assets_raw[:MAX_ASSETS]

    for asset in tqdm(assets_raw, desc="Analisando Assets"):
        if asset.get("asset_tag") in EXCLUDED_ASSETS: continue
        
        det = get_asset_details(asset.get("display_id"))
        serial, expiry = extract_fields_smart(det.get("type_fields", {}))
        c_info = asset_contract_map.get(det.get("id"), {})
        
        # Prioriza data de garantia, depois contrato
        check_date = expiry if expiry else c_info.get("end")
        dt = parse_date(check_date)
        
        if dt and today <= dt <= warning_limit:
            days = (dt - today).days
            asset_alerts.append({
                "Asset": det.get("name"),
                "Tag": det.get("asset_tag"),
                "Serial": serial,
                "Contrato": c_info.get("name"),
                "Vencimento Real": dt.strftime("%d/%m/%Y"),
                "Dias": days,
                "Nivel": categorize_alert(days)
            })
        time.sleep(0.05)

    # 4. Finalização
    if asset_alerts or contract_alerts:
        send_to_make(asset_alerts, contract_alerts)
    else:
        logger.info("Nenhum alerta hoje.")

if __name__ == "__main__":
    main()