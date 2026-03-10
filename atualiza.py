import json
import os
import sys
import time
import gzip
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
import urllib3
from urllib.parse import urlencode

# Desabilitar warnings de SSL não verificado (se necessário)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ✅ NOVAS CONSTANTES - API Dados Abertos (2026)
API_BASE_URL = "https://dadosabertos.compras.gov.br/modulo-contratacoes"
ENDPOINT_CONTRATACOES = f"{API_BASE_URL}/1_consultarContratacoes_PNCP_14133"
ENDPOINT_ITENS = f"{API_BASE_URL}/2_consultarItensContratacoes_PNCP_14133"
ENDPOINT_RESULTADOS = f"{API_BASE_URL}/3_consultarResultadoItensContratacoes_PNCP_14133"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://pncp.gov.br/'
}
MAX_WORKERS = 5
MAX_RETRIES = 3
BACKOFF_FACTOR = 2
CHECKPOINT_FILE = "checkpoint_atualizacao.json"


def extrair_id_licitacao(identificador):
    """
    Extrai CNPJ, Ano e Sequencial de um identificador.
    Aceita formatos: "cnpj/ano/sequencial" ou "cnpjanosequencial"
    """
    try:
        identificador = str(identificador).strip()
        
        # Formato com barras: cnpj/ano/sequencial
        if '/' in identificador:
            partes = identificador.split('/')
            if len(partes) == 3:
                return partes[0].strip(), partes[1].strip(), partes[2].strip()
        
        # Formato concatenado: CNPJ(14) + ANO(4) + SEQUENCIAL(N)
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
    """
    Faz requisição HTTP com retry automático e backoff exponencial.
    """
    try:
        full_url = f"{url}?{urlencode(params)}" if params else url
        response = requests.get(
            full_url,
            headers=HEADERS,
            timeout=30,
            allow_redirects=True,
            verify=True
        )

        # Rate limit - retry com backoff
        if response.status_code == 429 and retries < MAX_RETRIES:
            wait_time = BACKOFF_FACTOR ** retries
            logger.warning(f"Rate limit atingido. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            return fazer_requisicao(url, params, retries + 1)

        response.raise_for_status()
        return response

    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            wait_time = BACKOFF_FACTOR ** retries
            logger.warning(f"Erro na requisição: {e}. Retry {retries+1}/{MAX_RETRIES} em {wait_time}s...")
            time.sleep(wait_time)
            return fazer_requisicao(url, params, retries + 1)
        else:
            logger.error(f"Falha após {MAX_RETRIES} tentativas: {e}")
            return None


def buscar_dados_licitacao(cnpj, ano, sequencial):
    """
    ✅ NOVO: Busca dados da licitação via API de Dados Abertos com query params
    """
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
            # A API retorna uma lista de contratações
            if isinstance(data, list) and len(data) > 0:
                return data[0]  # Retorna primeira (e única) contratação
            elif isinstance(data, dict) and 'data' in data:
                return data['data'][0] if data['data'] else None
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON: {e}")
            return None
    else:
        status = response.status_code if response else "Sem resposta"
        logger.warning(f"HTTP {status} ao buscar licitação {cnpj}/{ano}/{sequencial}")
        return None


def buscar_itens_licitacao(cnpj, ano, sequencial):
    """
    ✅ NOVO: Busca itens da licitação via API de Dados Abertos
    """
    params = {
        'orgaoEntidadeCnpj': cnpj,
        'anoCompraPncp': ano,
        'sequencialCompraPncp': sequencial,
        'pagina': 1,
        'tamanhoPagina': 500  # Máximo permitido
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
            page_items = []
            
            if isinstance(data, list):
                page_items = data
            elif isinstance(data, dict):
                page_items = data.get('data', []) or data.get('itens', [])
            
            if not page_items:
                break
                
            itens.extend(page_items)
            
            # Se retornou menos que o máximo, acabou
            if len(page_items) < 500:
                break
                
            pagina += 1
            
        except json.JSONDecodeError:
            break
    
    return itens


def processar_licitacao(licitacao):
    """
    Processa uma licitação completa: dados gerais + itens
    """
    id_lic = licitacao['id']
    cnpj, ano, sequencial = extrair_id_licitacao(id_lic)
    
    if not all([cnpj, ano, sequencial]):
        return {'id': id_lic, 'status': 'id_invalido', 'dados': None}
    
    # Buscar dados gerais
    dados = buscar_dados_licitacao(cnpj, ano, sequencial)
    if not dados:
        return {'id': id_lic, 'status': 'falho', 'dados': None}
    
    # Buscar itens
    itens = buscar_itens_licitacao(cnpj, ano, sequencial)
    
    return {
        'id': id_lic,
        'status': 'atualizado',
        'dados': {
            'contratacao': dados,
            'itens': itens,
            'total_itens': len(itens)
        }
    }


# ... resto do código (carregar_checkpoint, salvar_checkpoint, main) permanece igual
# mas usando a nova estrutura de dados da API

def main():
    """Função principal de atualização."""
    logger.info("🚀 Iniciando atualização de licitações PNCP v4 (API Dados Abertos 2026)")
    logger.info(f"📡 API Base: {API_BASE_URL}")
    logger.info(f"⚡ Workers: {MAX_WORKERS} | Max Retries: {MAX_RETRIES}")

    # Carregar checkpoint anterior
    checkpoint = carregar_checkpoint()
    ja_processados = set(checkpoint.get('processados', []))
    falhos_anteriores = checkpoint.get('falhos', [])

    logger.info(f"📝 Checkpoint: {len(ja_processados)} já processados")

    # Carregar licitações pendentes
    licitacoes = carregar_licitacoes_pendentes()
    pendentes = [l for l in licitacoes if l['id'] not in ja_processados]
    
    logger.info(f"⏳ {len(pendentes)} licitações pendentes")

    if not pendentes:
        logger.info("✅ Nenhuma licitação pendente.")
        return

    # Processamento paralelo
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
                    logger.warning(f"❌ [{i+1}/{len(pendentes)}] {resultado['id']} - {resultado['status']}")

                processados_atual.append(resultado['id'])

                if (i + 1) % 100 == 0:
                    salvar_checkpoint(processados_atual, resultados_falhos)

            except Exception as e:
                logger.error(f"💥 Erro inesperado: {e}")
                resultados_falhos.append({'id': lic['id'], 'status': 'erro', 'erro': str(e)})

    # Salvar resultado final
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_saida = f"licitacoes_atualizadas_{timestamp}.json.gz"

    todos_resultados = {
        'metadata': {
            'data_atualizacao': datetime.now().isoformat(),
            'total_processados': len(processados_atual),
            'total_sucesso': len(resultados_sucesso),
            'total_falhos': len(resultados_falhos),
            'api_url': API_BASE_URL,
            'versao_api': 'dados_abertos_2026'
        },
        'sucessos': resultados_sucesso,
        'falhos': resultados_falhos
    }

    salvar_resultados(todos_resultados, arquivo_saida)
    salvar_checkpoint(processados_atual, resultados_falhos)

    logger.info("=" * 60)
    logger.info("📊 RESUMO DA ATUALIZAÇÃO")
    logger.info("=" * 60)
    logger.info(f"✅ Sucessos: {len(resultados_sucesso)}")
    logger.info(f"❌ Falhos: {len(resultados_falhos)}")
    logger.info(f"📁 Arquivo: {arquivo_saida}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
