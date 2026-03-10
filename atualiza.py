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
import urllib3
from urllib.parse import urlencode

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ✅ NOVA API DE DADOS ABERTOS (2026)
API_BASE = "https://dadosabertos.compras.gov.br/modulo-contratacoes"
ENDPOINT_CONTRATACOES = f"{API_BASE}/1_consultarContratacoes_PNCP_14133"
ENDPOINT_ITENS = f"{API_BASE}/2_consultarItensContratacoes_PNCP_14133"
ENDPOINT_RESULTADOS = f"{API_BASE}/3_consultarResultadoItensContratacoes_PNCP_14133"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive'
}
MAX_WORKERS = 5
MAX_RETRIES = 3
BACKOFF_FACTOR = 2
CHECKPOINT_FILE = "checkpoint_atualizacao.json"

# Mapas de situação
MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}


def extrair_id_licitacao(identificador):
    """Extrai CNPJ, Ano e Sequencial."""
    try:
        identificador = str(identificador).strip()
        
        if '/' in identificador:
            partes = identificador.split('/')
            if len(partes) == 3:
                return partes[0].strip(), partes[1].strip(), partes[2].strip()
        
        if len(identificador) >= 18:
            cnpj = identificador[:14]
            ano = identificador[14:18]
            sequencial = identificador[18:]
            return cnpj, ano, sequencial
            
        logger.warning(f"Formato de ID não reconhecido: {identificador}")
        return None, None, None
        
    except Exception as e:
        logger.error(f"Erro ao extrair ID {identificador}: {e}")
        return None, None, None


def fazer_requisicao(url, params=None, retries=0):
    """Faz requisição com retry e backoff."""
    try:
        full_url = f"{url}?{urlencode(params)}" if params else url
        response = requests.get(full_url, headers=HEADERS, timeout=30)
        
        if response.status_code == 429 and retries < MAX_RETRIES:
            wait_time = BACKOFF_FACTOR ** retries
            logger.warning(f"Rate limit. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            return fazer_requisicao(url, params, retries + 1)
        
        response.raise_for_status()
        return response

    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            wait_time = BACKOFF_FACTOR ** retries
            logger.warning(f"Erro: {e}. Retry {retries+1}/{MAX_RETRIES}...")
            time.sleep(wait_time)
            return fazer_requisicao(url, params, retries + 1)
        else:
            logger.error(f"Falha após {MAX_RETRIES} tentativas: {e}")
            return None


def buscar_dados_licitacao(cnpj, ano, sequencial):
    """Busca dados da licitação via API de dados abertos."""
    params = {
        'orgaoEntidadeCnpj': cnpj,
        'anoCompraPncp': ano,
        'sequencialCompraPncp': sequencial,
        'pagina': 1,
        'tamanhoPagina': 10
    }
    
    response = fazer_requisicao(ENDPOINT_CONTRATACOES, params)
    
    if response and response.status_code == 200:
        try:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            elif isinstance(data, dict) and 'data' in data:
                return data['data'][0] if data['data'] else None
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Erro JSON: {e}")
            return None
    return None


def buscar_itens(cnpj, ano, sequencial):
    """Busca itens da licitação."""
    params = {
        'orgaoEntidadeCnpj': cnpj,
        'anoCompraPncp': ano,
        'sequencialCompraPncp': sequencial,
        'pagina': 1,
        'tamanhoPagina': 500
    }
    
    itens = []
    pagina = 1
    
    while True:
        params['pagina'] = pagina
        response = fazer_requisicao(ENDPOINT_ITENS, params)
        
        if not response or response.status_code != 200:
            break
            
        try:
            data = response.json()
            if isinstance(data, list):
                page_items = data
            elif isinstance(data, dict):
                page_items = data.get('data', [])
            else:
                break
            
            if not page_items:
                break
                
            itens.extend(page_items)
            
            if len(page_items) < 500:
                break
                
            pagina += 1
            
        except json.JSONDecodeError:
            break
    
    return itens


def buscar_resultado_item(cnpj, ano, seq, num_item):
    """Busca resultado de um item específico."""
    params = {
        'orgaoEntidadeCnpj': cnpj,
        'anoCompraPncp': ano,
        'sequencialCompraPncp': seq,
        'numeroItem': num_item,
        'pagina': 1,
        'tamanhoPagina': 10
    }
    
    response = fazer_requisicao(ENDPOINT_RESULTADOS, params)
    
    if response and response.status_code == 200:
        try:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return None
        except:
            return None
    return None


def processar_licitacao(licitacao):
    """Processa uma licitação completa."""
    id_lic = licitacao['id']
    cnpj, ano, sequencial = extrair_id_licitacao(id_lic)
    
    if not all([cnpj, ano, sequencial]):
        return {'id': id_lic, 'status': 'id_invalido', 'dados': None}
    
    # Buscar dados gerais
    dados = buscar_dados_licitacao(cnpj, ano, sequencial)
    if not dados:
        return {'id': id_lic, 'status': 'falho', 'dados': None}
    
    # Buscar itens
    itens_api = buscar_itens(cnpj, ano, sequencial)
    
    # Para cada item em andamento, buscar resultado
    itens_atualizados = []
    for item in itens_api:
        num_item = item.get('numeroItem')
        sit_id = item.get('situacaoCompraItem', 1)
        
        item_formatado = {
            'n': num_item,
            'd': item.get('descricao', ''),
            'q': float(item.get('quantidade', 0) or 0),
            'u': item.get('unidadeMedida', 'UN'),
            'v_est': float(item.get('valorUnitarioEstimado', 0) or 0),
            'sit': MAPA_SITUACAO_ITEM.get(sit_id, "EM ANDAMENTO"),
            'res_forn': None,
            'res_val': 0.0
        }
        
        # Se estiver em andamento ou homologado, buscar resultado
        if sit_id in [1, 2]:  # EM ANDAMENTO ou HOMOLOGADO
            resultado = buscar_resultado_item(cnpj, ano, sequencial, num_item)
            if resultado:
                fornecedor = resultado.get('nomeRazaoSocialFornecedor') or resultado.get('razaoSocial')
                if fornecedor:
                    ni = resultado.get('niFornecedor', '')
                    item_formatado['res_forn'] = f"{fornecedor} (CNPJ: {ni})" if ni else fornecedor
                    item_formatado['sit'] = "HOMOLOGADO"
                    item_formatado['res_val'] = float(resultado.get('valorUnitarioHomologado', 0) or 0)
        
        itens_atualizados.append(item_formatado)
    
    return {
        'id': id_lic,
        'status': 'atualizado',
        'dados': {
            'sit_global': MAPA_SITUACAO_GLOBAL.get(dados.get('situacaoCompraId', 1), "DIVULGADA"),
            'val_tot': float(dados.get('valorTotalEstimado', 0) or 0),
            'itens': itens_atualizados,
            'total_itens': len(itens_atualizados)
        }
    }


def carregar_licitacoes_pendentes():
    """Carrega licitações do arquivo de dados."""
    licitacoes = []
    padroes = ['dadosoportunidades.json.gz', 'pregacoes_pharma_limpos.json.gz']
    
    for padrao in padroes:
        arquivos = list(Path('.').glob(padrao))
        for arquivo in arquivos:
            try:
                if str(arquivo).endswith('.gz'):
                    with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
                        dados = json.load(f)
                else:
                    with open(arquivo, 'r', encoding='utf-8') as f:
                        dados = json.load(f)
                
                if isinstance(dados, list):
                    for item in dados:
                        if isinstance(item, dict) and item.get('id'):
                            licitacoes.append({
                                'id': item['id'],
                                'dados_originais': item
                            })
                break  # Usa o primeiro arquivo encontrado
                
            except Exception as e:
                logger.error(f"Erro ao carregar {arquivo}: {e}")
                continue
    
    # Remover duplicatas
    vistos = set()
    unicas = []
    for lic in licitacoes:
        if lic['id'] not in vistos:
            vistos.add(lic['id'])
            unicas.append(lic)
    
    logger.info(f"🔍 {len(unicas)} licitações carregadas")
    return unicas


def carregar_checkpoint():
    """Carrega progresso anterior."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {'processados': [], 'falhos': []}


def salvar_checkpoint(processados, falhos):
    """Salva progresso atual."""
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'processados': processados,
                'falhos': falhos,
                'timestamp': datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Erro ao salvar checkpoint: {e}")


def main():
    logger.info("🚀 Atualização PNCP v4 - API Dados Abertos 2026")
    
    checkpoint = carregar_checkpoint()
    ja_processados = set(checkpoint.get('processados', []))
    
    licitacoes = carregar_licitacoes_pendentes()
    pendentes = [l for l in licitacoes if l['id'] not in ja_processados]
    
    logger.info(f"⏳ {len(pendentes)} licitações pendentes")
    
    if not pendentes:
        logger.info("✅ Nenhuma pendência.")
        return

    resultados_sucesso = []
    resultados_falhos = []
    processados_atual = list(ja_processados)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_lic = {
            executor.submit(processar_licitacao, lic): lic 
            for lic in pendentes
        }

        for i, future in enumerate(as_completed(future_to_lic)):
            lic = future_to_lic[future]
            try:
                resultado = future.result()

                if resultado['status'] == 'atualizado':
                    resultados_sucesso.append(resultado)
                    logger.info(f"✅ [{i+1}/{len(pendentes)}] {resultado['id']} - {resultado['dados']['total_itens']} itens")
                else:
                    resultados_falhos.append(resultado)
                    logger.warning(f"❌ [{i+1}/{len(pendentes)}] {resultado['id']}")

                processados_atual.append(resultado['id'])

                if (i + 1) % 100 == 0:
                    salvar_checkpoint(processados_atual, resultados_falhos)

            except Exception as e:
                logger.error(f"💥 Erro: {e}")
                resultados_falhos.append({'id': lic['id'], 'status': 'erro', 'erro': str(e)})

    # Salvar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_saida = f"licitacoes_atualizadas_{timestamp}.json.gz"

    todos_resultados = {
        'metadata': {
            'data_atualizacao': datetime.now().isoformat(),
            'total_processados': len(processados_atual),
            'total_sucesso': len(resultados_sucesso),
            'total_falhos': len(resultados_falhos)
        },
        'sucessos': resultados_sucesso,
        'falhos': resultados_falhos
    }

    with gzip.open(arquivo_saida, 'wt', encoding='utf-8') as f:
        json.dump(todos_resultados, f, ensure_ascii=False, indent=2)
    
    salvar_checkpoint(processados_atual, resultados_falhos)

    logger.info("=" * 60)
    logger.info(f"✅ Sucessos: {len(resultados_sucesso)}")
    logger.info(f"❌ Falhos: {len(resultados_falhos)}")
    logger.info(f"📁 Arquivo: {arquivo_saida}")


if __name__ == "__main__":
    main()
