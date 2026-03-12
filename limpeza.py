import json
import gzip
from pathlib import Path
import logging
import os

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

ARQ_ENTRADA = 'dadosoportunidades.json.gz'
ARQ_SAIDA = 'pregacoes_pharma_limpos.json.gz'

def carregar_licitacoes():
    licitacoes = []
    arquivo = Path(ARQ_ENTRADA)
    if not arquivo.exists():
        logger.error(f"❌ Arquivo não encontrado: {ARQ_ENTRADA}")
        return []
    try:
        with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
            dados = json.load(f)
        if isinstance(dados, list):
            licitacoes.extend(dados)
            logger.info(f"✅ {arquivo}: {len(dados)} registros carregados.")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar {arquivo}: {e}")
    return licitacoes

def deduplicar(licitacoes):
    por_id = {}
    for lic in licitacoes:
        lic_id = str(lic.get('id', ''))
        if not lic_id: continue
        por_id[lic_id] = lic
    resultado = list(por_id.values())
    logger.info(f"✅ Após deduplicação: {len(resultado)} licitações únicas.")
    return resultado

def otimizar_peso_json(licitacao):
    chaves_para_remover = ['_metadata', '_raw', 'api_fonte']
    for campo in chaves_para_remover:
        licitacao.pop(campo, None)
    return licitacao

def salvar_resultado(licitacoes):
    try:
        with gzip.open(ARQ_SAIDA, 'wt', encoding='utf-8') as f:
            json.dump(licitacoes, f, ensure_ascii=False)
        tamanho_kb = os.path.getsize(ARQ_SAIDA) / 1024
        logger.info(f"💾 Salvo com sucesso: {ARQ_SAIDA} ({tamanho_kb:.1f} KB)")
        return True
    except Exception as e:
        logger.error(f"❌ Erro crítico ao salvar json final: {e}")
        raise

def main():
    logger.info("🚀 Iniciando QA e Compressão (limpeza.py)")
    licitacoes = carregar_licitacoes()
    if not licitacoes: return
    licitacoes = deduplicar(licitacoes)
    licitacoes = [otimizar_peso_json(lic) for lic in licitacoes]
    if licitacoes:
        salvar_resultado(licitacoes)
        logger.info("✅ Limpeza concluída perfeitamente!")

if __name__ == "__main__":
    main()
