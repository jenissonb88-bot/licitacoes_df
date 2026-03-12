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

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ✅ CORREÇÃO 1: Usando a API Real-Time do PNCP (Mesma do app.py)
API_BASE = "https://pncp.gov.br/api/pncp/v1"

# Arquivo Oficial consumido pelo Front-End
ARQ_DADOS = 'pregacoes_pharma_limpos.json.gz'
MAX_WORKERS = 10

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def criar_sessao():
    s = requests.Session()
    s.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'Sniper Pharma/23.0 Updater',
        'Accept-Encoding': 'gzip, deflate, br'
    })
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def extrair_id_licitacao(identificador):
    """Extrai CNPJ, Ano e Sequencial da string de ID."""
    try:
        identificador = str(identificador).strip()
        if len(identificador) >= 18 and '/' not in identificador:
            return identificador[:14], identificador[14:18], identificador[18:]
    except Exception as e:
        logger.error(f"Erro ao extrair ID {identificador}: {e}")
    return None, None, None

def buscar_resultado_item(cnpj, ano, seq, num_item, session):
    """Busca o vencedor de um item específico na API oficial do PNCP."""
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            # A API retorna uma lista de resultados. Pegamos o primeiro (o vencedor).
            if isinstance(data, list) and len(data) > 0:
                return data[0]
    except Exception as e:
        pass
    return None

def buscar_situacao_licitacao(cnpj, ano, seq, session):
    """Busca a situação atualizada da licitação como um todo."""
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            sit_id = data.get('situacaoCompraId') or 1
            return MAPA_SITUACAO_GLOBAL.get(sit_id, "DIVULGADA")
    except:
        pass
    return None

def processar_atualizacao(licitacao, session):
    """Injeta os dados do vencedor e novos status diretamente no dicionário da licitação."""
    id_lic = licitacao.get('id')
    cnpj, ano, seq = extrair_id_licitacao(id_lic)
    
    if not all([cnpj, ano, seq]):
        return False # ID inválido, ignora
        
    teve_atualizacao = False
    
    # 1. Atualiza o Status Global da Licitação
    nova_sit_global = buscar_situacao_licitacao(cnpj, ano, seq, session)
    if nova_sit_global and nova_sit_global != licitacao.get('sit_global'):
        licitacao['sit_global'] = nova_sit_global
        teve_atualizacao = True
        
    # Se a licitação foi anulada/revogada, não precisamos checar itens
    if nova_sit_global in ["REVOGADA", "ANULADA", "SUSPENSA"]:
        return teve_atualizacao

    # 2. Atualiza os Itens (Busca de Vencedores)
    itens = licitacao.get('itens', [])
    for item in itens:
        # Só busca resultado se o item estava 'EM ANDAMENTO' e ainda não tem vencedor
        if item.get('sit') == "EM ANDAMENTO" and not item.get('res_forn'):
            num_item = item.get('n')
            
            resultado = buscar_resultado_item(cnpj, ano, seq, num_item, session)
            
            if resultado:
                # Extrai os dados do vencedor da API do PNCP
                fornecedor = resultado.get('nomeRazaoSocialFornecedor') or resultado.get('razaoSocial')
                ni = resultado.get('niFornecedor', '')
                valor_final = float(resultado.get('valorUnitarioHomologado', 0) or 0)
                
                if fornecedor:
                    item['res_forn'] = f"{fornecedor} (CNPJ: {ni})" if ni else fornecedor
                    item['res_val'] = valor_final
                    item['sit'] = "HOMOLOGADO"
                    teve_atualizacao = True
                    
            # Opcional: Aqui poderíamos checar também se o item deu "DESERTO", 
            # mas o endpoint principal do PNCP geralmente informa isso.
            
    return teve_atualizacao

def main():
    logger.info("🚀 Atualizador de Vencedores (Sniper Pharma PNCP)")
    
    if not os.path.exists(ARQ_DADOS):
        logger.error(f"❌ Banco de dados {ARQ_DADOS} não encontrado.")
        return

    # 1. Carrega o banco de dados oficial que o Front-End lê
    logger.info("📦 Carregando banco de dados para atualização em memória...")
    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        todas_licitacoes = json.load(f)
        
    licitacoes_para_atualizar = []
    
    # 2. Seleciona apenas as licitações que têm itens "EM ANDAMENTO"
    for lic in todas_licitacoes:
        if lic.get('sit_global') not in ["REVOGADA", "ANULADA"]:
            tem_pendencia = any(it.get('sit') == "EM ANDAMENTO" for it in lic.get('itens', []))
            if tem_pendencia:
                licitacoes_para_atualizar.append(lic)
                
    logger.info(f"⏳ {len(licitacoes_para_atualizar)} licitações possuem itens aguardando vencedor.")
    
    if not licitacoes_para_atualizar:
        logger.info("✅ Tudo atualizado! Nada a fazer.")
        return

    session = criar_sessao()
    contador_sucesso = 0

    # 3. Dispara as requisições em paralelo para descobrir os vencedores
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(processar_atualizacao, lic, session): lic for lic in licitacoes_para_atualizar}
        
        for i, future in enumerate(as_completed(futuros)):
            try:
                houve_mudanca = future.result()
                if houve_mudanca:
                    contador_sucesso += 1
                if (i + 1) % 50 == 0:
                    logger.info(f"   🔄 Processados: {i+1}/{len(licitacoes_para_atualizar)}")
            except Exception as e:
                pass

    # ✅ CORREÇÃO 2: Salva as alterações de volta no arquivo OFICIAL!
    if contador_sucesso > 0:
        logger.info(f"💾 Salvando {contador_sucesso} licitações com novos vencedores no banco principal...")
        with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
            json.dump(todas_licitacoes, f, ensure_ascii=False)
        logger.info(f"✅ Painel (index.html) atualizado com sucesso!")
    else:
        logger.info("ℹ️ Nenhuma homologação nova encontrada pelo governo hoje.")

if __name__ == "__main__":
    main()
