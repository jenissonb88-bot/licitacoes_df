import json
import os
import sys
import time
import gzip
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

API_BASE = "https://pncp.gov.br/api/pncp/v1"
ARQ_DADOS = 'pregacoes_pharma_limpos.json.gz'
MAX_WORKERS = 10

MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/23.0 Updater', 'Accept-Encoding': 'gzip, deflate, br'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def extrair_id_licitacao(identificador):
    try:
        identificador = str(identificador).strip()
        if len(identificador) >= 18 and '/' not in identificador:
            return identificador[:14], identificador[14:18], identificador[18:]
    except: pass
    return None, None, None

def buscar_resultado_item(cnpj, ano, seq, num_item, session):
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0: return data[0]
    except: pass
    return None

def buscar_situacao_licitacao(cnpj, ano, seq, session):
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            return MAPA_SITUACAO_GLOBAL.get(r.json().get('situacaoCompraId') or 1, "DIVULGADA")
    except: pass
    return None

def processar_atualizacao(licitacao, session):
    id_lic = licitacao.get('id')
    cnpj, ano, seq = extrair_id_licitacao(id_lic)
    if not all([cnpj, ano, seq]): return False
        
    teve_atualizacao = False
    nova_sit_global = buscar_situacao_licitacao(cnpj, ano, seq, session)
    if nova_sit_global and nova_sit_global != licitacao.get('sit_global'):
        licitacao['sit_global'] = nova_sit_global
        teve_atualizacao = True
        
    if nova_sit_global in ["REVOGADA", "ANULADA", "SUSPENSA"]: return teve_atualizacao

    for item in licitacao.get('itens', []):
        if item.get('sit') == "EM ANDAMENTO" and not item.get('res_forn'):
            resultado = buscar_resultado_item(cnpj, ano, seq, item.get('n'), session)
            if resultado:
                fornecedor = resultado.get('nomeRazaoSocialFornecedor') or resultado.get('razaoSocial')
                ni = resultado.get('niFornecedor', '')
                if fornecedor:
                    item['res_forn'] = f"{fornecedor} (CNPJ: {ni})" if ni else fornecedor
                    item['res_val'] = float(resultado.get('valorUnitarioHomologado', 0) or 0)
                    item['sit'] = "HOMOLOGADO"
                    teve_atualizacao = True
    return teve_atualizacao

def main():
    logger.info("🚀 Atualizador de Vencedores (Sniper Pharma PNCP)")
    if not os.path.exists(ARQ_DADOS): return

    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        todas_licitacoes = json.load(f)
        
    licitacoes_para_atualizar = [lic for lic in todas_licitacoes if lic.get('sit_global') not in ["REVOGADA", "ANULADA"] and any(it.get('sit') == "EM ANDAMENTO" for it in lic.get('itens', []))]
                
    MAX_POR_TURNO = 1500
    licitacoes_para_atualizar = licitacoes_para_atualizar[:MAX_POR_TURNO]
    
    if not licitacoes_para_atualizar:
        logger.info("✅ Tudo atualizado! Nada a fazer.")
        return

    session = criar_sessao()
    contador_sucesso = 0
    processados_agora = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(processar_atualizacao, lic, session): lic for lic in licitacoes_para_atualizar}
        for i, future in enumerate(as_completed(futuros)):
            try:
                if future.result(): contador_sucesso += 1
                processados_agora += 1
                if processados_agora % 50 == 0: logger.info(f"   🔄 Processados: {processados_agora}/{len(licitacoes_para_atualizar)}")
                
                # Checkpoint para evitar perda por TimeOut do Servidor
                if processados_agora % 200 == 0 and contador_sucesso > 0:
                    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f: json.dump(todas_licitacoes, f, ensure_ascii=False)
            except: pass

    if contador_sucesso > 0:
        logger.info(f"💾 Salvamento final: {contador_sucesso} licitações com novos vencedores.")
        with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f: json.dump(todas_licitacoes, f, ensure_ascii=False)
    else:
        logger.info("ℹ️ Nenhuma homologação nova encontrada.")

if __name__ == "__main__":
    main()
