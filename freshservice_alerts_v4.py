import time
import requests
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from tqdm import tqdm
from dotenv import load_dotenv

# Criação da planilha para envio
import pandas as pd
import io
import base64
import jinja2

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

# Configurações de alerta (garantindo 365 dias limite de busca)
DAYS_WARN = int(os.getenv("DAYS_TO_WARN", 365))
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

def get_paged_results(endpoint: str, params: Optional[Dict] = None, desc: str = "Baixando") -> List[Dict]:
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
            if not key or not data[key]: break
            batch = data[key]
            results.extend(batch)
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
    
    for kw in ["serial", "service_tag", "srie", "nmero", "imei", "asset_tag"]:
        found = next((k for k in normalized if kw in k), None)
        if found: 
            serial = normalized[found]
            break
            
    for kw in ["warranty_expiry", "expiry_date", "final_de_suporte", "support_end", "validade", "vencimento"]:
        found = next((k for k in normalized if kw in k), None)
        if found:
            val = str(normalized[found])
            if len(val) > 8: 
                expiry = val
                break
    return serial, expiry

def parse_date(date_string: str) -> Optional[datetime]:
    if not date_string: return None
    try: return datetime.strptime(str(date_string)[:10], "%Y-%m-%d")
    except: return None

# ==========================================
# A MÁGICA ACONTECE AQUI: Estilos prontos para o Make
# ==========================================
# ==========================================
# ESTILOS E TEXTOS LIMPOS (Sem HTML)
# ==========================================
# ==========================================
# ESTILOS COM EMOJIS NATIVOS PARA EXCEL
# ==========================================
def get_style(days: int) -> str:
    # O Excel lê esses emojis perfeitamente como texto
    if days < 0: return "⚫ Vencido"
    if days <= 90: return "🔴 Crítico"
    if days <= 120: return "🟡 Atenção"
    return "🔵 Info"

def clean(val):
    if val is None: return "N/A"
    return str(val).strip().replace('\n', ' ').replace('\r', '')

# ==========================================
# FUNÇÃO DE ENVIO ATUALIZADA (SEM JINJA2)
# ==========================================
def send_to_make(asset_alerts: List[Dict], contract_alerts: List[Dict]) -> bool:
    if not MAKE_URL: return False
    
    clean_assets = []
    for a in asset_alerts:
        clean_assets.append({
            "Status": get_style(a["Dias"]), 
            "Asset": clean(a["Asset"]),
            "Tag": clean(a["Tag"]), 
            "Serial": clean(a["Serial"]),
            "Contrato": clean(a["Contrato"]) if a["Contrato"] else "Sem contrato",
            "Vencimento": a["Vencimento Real"], 
            "Dias Restantes": a["Dias"]
        })

    clean_contracts = []
    for c in contract_alerts:
        clean_contracts.append({
            "Status": get_style(c["days_remaining"]), 
            "Contrato": clean(c["contract_name"]), 
            "ID": c["contract_id"], 
            "Vencimento": c["end_date"], 
            "Dias Restantes": c["days_remaining"]
        })

    all_alerts = clean_assets + clean_contracts

    # Geração do Excel simples e direto (sem a função .style)
    df_assets = pd.DataFrame(clean_assets)
    df_contracts = pd.DataFrame(clean_contracts)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        if not df_assets.empty:
            # Salvando direto, sem tentar pintar a linha
            df_assets.to_excel(writer, sheet_name='Ativos', index=False)
        if not df_contracts.empty:
            df_contracts.to_excel(writer, sheet_name='Contratos', index=False)
    
    excel_base64 = base64.b64encode(output.getvalue()).decode('utf-8')
    filename_date = datetime.now().strftime("%d_%m_%Y")

    payload = {
        "summary": {
            "total_count": len(all_alerts),
            "vencido_count": len([x for x in all_alerts if x.get("Dias Restantes", 0) < 0]),
            "critical_count": len([x for x in all_alerts if 0 <= x.get("Dias Restantes", 0) <= 90]),
            "warning_count": len([x for x in all_alerts if 91 <= x.get("Dias Restantes", 0) <= 120]),
            "info_count": len([x for x in all_alerts if 121 <= x.get("Dias Restantes", 0) <= 365])
        },
        "recipient_email": EMAIL_TO,
        "generated_at": datetime.now().isoformat(),
        "file_name": f"Relatorio_Vencimentos_{filename_date}.xlsx",
        "file_data": excel_base64
    }

    try:
        logger.info(f"Enviando Excel para o Make: {payload['summary']['total_count']} alertas no total.")
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
    logger.info("INICIANDO VERIFICAÇÃO DE VENCIMENTOS (INCLUINDO VENCIDOS E AVISOS ATÉ 365 DIAS)")
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    warning_limit = today + timedelta(days=DAYS_WARN)

    assets_raw = get_paged_results(f"{BASE_URL}/assets", desc="Assets")
    contracts_raw = get_paged_results(f"{BASE_URL}/contracts", desc="Contratos")

    contract_alerts = []
    asset_contract_map = {}
    
    for c in tqdm(contracts_raw, desc="Analisando Contratos"):
        c_id, name = c.get("id"), c.get("name")
        end_dt = parse_date(c.get("end_date"))
        
        # Filtro: Pega o passado (vencidos) e o futuro até 365 dias
        if end_dt and end_dt <= warning_limit:
            days = (end_dt - today).days
            contract_alerts.append({
                "contract_name": name,
                "contract_id": c_id,
                "vendor": c.get("vendor_name", "N/A"),
                "end_date": end_dt.strftime("%d/%m/%Y"),
                "days_remaining": days
            })
        
        assoc = get_paged_results(f"{BASE_URL}/contracts/{c_id}/associated-assets", desc=f"Assoc {name[:15]}")
        for a in assoc:
            asset_contract_map[a.get("id")] = {"name": name, "end": c.get("end_date")}

    asset_alerts = []
    if MAX_ASSETS: assets_raw = assets_raw[:MAX_ASSETS]

    for asset in tqdm(assets_raw, desc="Analisando Assets"):
        if asset.get("asset_tag") in EXCLUDED_ASSETS: continue
        
        det = get_asset_details(asset.get("display_id"))
        serial, expiry = extract_fields_smart(det.get("type_fields", {}))
        c_info = asset_contract_map.get(det.get("id"), {})
        
        check_date = expiry if expiry else c_info.get("end")
        dt = parse_date(check_date)
        
        # Filtro: Pega o passado (vencidos) e o futuro até 365 dias
        if dt and dt <= warning_limit:
            days = (dt - today).days
            asset_alerts.append({
                "Asset": det.get("name"),
                "Tag": det.get("asset_tag"),
                "Serial": serial,
                "Contrato": c_info.get("name"),
                "Vencimento Real": dt.strftime("%d/%m/%Y"),
                "Dias": days
            })
        time.sleep(0.05)

    if asset_alerts or contract_alerts:
        send_to_make(asset_alerts, contract_alerts)
    else:
        logger.info("Nenhum alerta hoje.")

if __name__ == "__main__":
    main()