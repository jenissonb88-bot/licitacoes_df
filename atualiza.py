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

# Desabilitar warnings de SSL não verificado (se necessário)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constantes
API_BASE_URL = "https://pncp.gov.br/api/pncp/v1"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive'
}
MAX_WORKERS = 5  # Paralelização controlada
MAX_RETRIES = 3
BACKOFF_FACTOR = 2
CHECKPOINT_FILE = "checkpoint_atualizacao.json"


def extrair_id_licitacao(identificador):
    """
    Extrai CNPJ, Ano e Sequencial de um identificador.
    Aceita formatos: "cnpj/ano/sequencial" ou "cnpjanosequencial"

    Retorna: (cnpj, ano, sequencial) ou (None, None, None) se inválido
    """
    try:
        # Remover espaços e normalizar
        identificador = str(identificador).strip()

        # Se já tem barras, separar diretamente
        if '/' in identificador:
            partes = identificador.split('/')
            if len(partes) == 3:
                cnpj, ano, sequencial = partes
                return cnpj.strip(), ano.strip(), sequencial.strip()

        # Se não tem barras, tentar extrair por posição
        # CNPJ = 14 dígitos, Ano = 4 dígitos, Resto = sequencial
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


def construir_url_api(cnpj, ano, sequencial):
    """
    Constrói a URL correta da API PNCP v1.
    Formato: /orgaos/{cnpj}/compras/{ano}/{sequencial}
    """
    return f"{API_BASE_URL}/orgaos/{cnpj}/compras/{ano}/{sequencial}"


def fazer_requisicao(url, retries=0):
    """
    Faz requisição HTTP com retry automático e backoff exponencial.
    Resolve problemas de HTTP 301/302 seguindo redirects.
    """
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=30,
            allow_redirects=True,  # Segue redirects 301/302 automaticamente
            verify=True
        )

        # Se for 429 (Too Many Requests), esperar e retry
        if response.status_code == 429 and retries < MAX_RETRIES:
            wait_time = BACKOFF_FACTOR ** retries
            logger.warning(f"Rate limit atingido. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            return fazer_requisicao(url, retries + 1)

        response.raise_for_status()
        return response

    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            wait_time = BACKOFF_FACTOR ** retries
            logger.warning(f"Erro na requisição: {e}. Retry {retries+1}/{MAX_RETRIES} em {wait_time}s...")
            time.sleep(wait_time)
            return fazer_requisicao(url, retries + 1)
        else:
            logger.error(f"Falha após {MAX_RETRIES} tentativas: {e}")
            return None


def buscar_dados_licitacao(identificador):
    """
    Busca dados atualizados de uma licitação na API PNCP.
    Retorna dict com dados ou None se erro.
    """
    cnpj, ano, sequencial = extrair_id_licitacao(identificador)

    if not all([cnpj, ano, sequencial]):
        logger.error(f"ID inválido: {identificador}")
        return None

    url = construir_url_api(cnpj, ano, sequencial)
    logger.debug(f"Buscando: {url}")

    response = fazer_requisicao(url)

    if response and response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON para {identificador}: {e}")
            return None
    else:
        status = response.status_code if response else "Sem resposta"
        logger.warning(f"HTTP {status} ao buscar dados da licitação {identificador}")
        return None


def carregar_licitacoes_pendentes():
    """
    Carrega lista de licitações que precisam de atualização.
    Busca em arquivos JSON comprimidos do app.py.
    """
    licitacoes = []

    # Buscar arquivos de licitações
    padroes = ['licitacoes_*.json.gz', 'licitacoes_*.json', 'dados_licitacoes/*.json*']

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

                # Extrair identificadores
                if isinstance(dados, list):
                    for item in dados:
                        if isinstance(item, dict):
                            # Tentar diferentes campos de ID
                            id_lic = item.get('id') or item.get('identificador') or item.get('numeroControlePNCP')
                            if id_lic:
                                licitacoes.append({
                                    'id': id_lic,
                                    'dados_originais': item
                                })

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

    logger.info(f"🔍 {len(unicas)} licitações selecionadas para atualização")
    return unicas


def carregar_checkpoint():
    """Carrega progresso anterior se existir."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
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


def processar_licitacao(licitacao):
    """
    Processa uma única licitação: busca dados atualizados e retorna resultado.
    """
    id_lic = licitacao['id']

    # Buscar dados atualizados
    dados_novos = buscar_dados_licitacao(id_lic)

    if dados_novos:
        return {
            'id': id_lic,
            'status': 'atualizado',
            'dados': dados_novos
        }
    else:
        return {
            'id': id_lic,
            'status': 'falho',
            'dados': None
        }


def salvar_resultados(resultados, nome_arquivo='licitacoes_atualizadas.json.gz'):
    """Salva resultados em arquivo comprimido."""
    try:
        with gzip.open(nome_arquivo, 'wt', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Resultados salvos em {nome_arquivo}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar resultados: {e}")
        return False


def main():
    """Função principal de atualização."""
    logger.info("🚀 Iniciando atualização de licitações PNCP v3")
    logger.info(f"📡 API Base: {API_BASE_URL}")
    logger.info(f"⚡ Workers: {MAX_WORKERS} | Max Retries: {MAX_RETRIES}")

    # Carregar checkpoint anterior
    checkpoint = carregar_checkpoint()
    ja_processados = set(checkpoint.get('processados', []))
    falhos_anteriores = checkpoint.get('falhos', [])

    logger.info(f"📝 Checkpoint: {len(ja_processados)} já processados, {len(falhos_anteriores)} falhos anteriores")

    # Carregar licitações pendentes
    licitacoes = carregar_licitacoes_pendentes()

    # Filtrar já processados
    pendentes = [l for l in licitacoes if l['id'] not in ja_processados]
    logger.info(f"⏳ {len(pendentes)} licitações pendentes de processamento")

    if not pendentes:
        logger.info("✅ Nenhuma licitação pendente. Processo concluído.")
        return

    # Processamento paralelo
    resultados_sucesso = []
    resultados_falhos = []
    processados_atual = list(ja_processados)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submeter todas as tarefas
        future_to_lic = {
            executor.submit(processar_licitacao, lic): lic 
            for lic in pendentes
        }

        # Processar resultados conforme completam
        for i, future in enumerate(as_completed(future_to_lic)):
            lic = future_to_lic[future]
            try:
                resultado = future.result()

                if resultado['status'] == 'atualizado':
                    resultados_sucesso.append(resultado)
                    logger.info(f"✅ [{i+1}/{len(pendentes)}] {resultado['id']} - Atualizado")
                else:
                    resultados_falhos.append(resultado)
                    logger.warning(f"❌ [{i+1}/{len(pendentes)}] {resultado['id']} - Falha")

                processados_atual.append(resultado['id'])

                # Salvar checkpoint a cada 100 itens
                if (i + 1) % 100 == 0:
                    salvar_checkpoint(processados_atual, resultados_falhos)
                    logger.info(f"💾 Checkpoint salvo: {i+1} processados")

            except Exception as e:
                logger.error(f"💥 Erro inesperado processando {lic['id']}: {e}")
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
            'api_url': API_BASE_URL
        },
        'sucessos': resultados_sucesso,
        'falhos': resultados_falhos
    }

    salvar_resultados(todos_resultados, arquivo_saida)
    salvar_checkpoint(processados_atual, resultados_falhos)

    # Resumo final
    logger.info("=" * 60)
    logger.info("📊 RESUMO DA ATUALIZAÇÃO")
    logger.info("=" * 60)
    logger.info(f"✅ Sucessos: {len(resultados_sucesso)}")
    logger.info(f"❌ Falhos: {len(resultados_falhos)}")
    logger.info(f"📁 Arquivo: {arquivo_saida}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
