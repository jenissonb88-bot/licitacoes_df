import json
import os
import gzip
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://pncp.gov.br/api/pncp/v1"
ARQ_DADOS = 'pregacoes_pharma_limpos.json.gz'
MAX_WORKERS = 10

def criar_sessao():
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_resultado_item(id_lic, num_item, session):
    """id_lic formato: CNPJ+ANO+SEQ"""
    cnpj = id_lic[:14]
    ano = id_lic[14:18]
    seq = id_lic[18:]
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data[0] if isinstance(data, list) and len(data) > 0 else None
    except: pass
    return None

def atualizar_licitacao(lic, session):
    id_lic = lic.get('id')
    teve_mudanca = False
    
    # Só atualiza itens que ainda estão "EM ANDAMENTO"
    for item in lic.get('itens', []):
        if item.get('sit') == "EM ANDAMENTO":
            res = buscar_resultado_item(id_lic, item.get('n'), session)
            if res:
                fornecedor = res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')
                ni = res.get('niFornecedor', '')
                if fornecedor:
                    item['res_forn'] = f"{fornecedor} ({ni})" if ni else fornecedor
                    item['res_val'] = float(res.get('valorUnitarioHomologado') or 0)
                    item['sit'] = "HOMOLOGADO"
                    teve_mudanca = True
    return teve_mudanca

def main():
    if not os.path.exists(ARQ_DADOS): return

    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        banco = json.load(f)

    session = criar_sessao()
    # Filtra apenas licitações que ainda têm itens para homologar
    pendentes = [l for l in banco if any(i.get('sit') == "EM ANDAMENTO" for i in l.get('itens', []))]
    
    if not pendentes:
        logger.info("✅ Tudo atualizado no banco de dados.")
        return

    logger.info(f"⏳ Verificando resultados para {len(pendentes)} licitações...")
    mudancas = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(atualizar_licitacao, l, session): l for l in pendentes}
        for f in as_completed(futuros):
            if f.result(): mudancas += 1

    if mudancas > 0:
        with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
            json.dump(banco, f, ensure_ascii=False)
        logger.info(f"💾 Sucesso: {mudancas} licitações atualizadas com vencedores.")
    else:
        logger.info("ℹ️ Nenhuma nova homologação encontrada hoje.")

if __name__ == "__main__":
    main()
