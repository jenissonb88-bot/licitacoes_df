import json
import gzip
from pathlib import Path
import logging
import os

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ✅ NOMES FIXOS DO FLUXO
ARQ_ENTRADA = 'dadosoportunidades.json.gz'      # Veio do app.py (O Sniper)
ARQ_SAIDA = 'pregacoes_pharma_limpos.json.gz'   # Vai para o avalia_portfolio.py

def carregar_licitacoes():
    """Carrega licitações do arquivo dadosoportunidades.json.gz."""
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
            logger.info(f"✅ {arquivo}: {len(dados)} registros carregados do app.py")
        else:
            logger.warning(f"⚠️ Formato inesperado: {type(dados)}")

    except Exception as e:
        logger.error(f"❌ Erro ao carregar {arquivo}: {e}")
        return []

    return licitacoes

def deduplicar(licitacoes):
    """Remove duplicatas absolutas mantendo o banco de dados enxuto."""
    logger.info(f"🔍 Verificando duplicatas em {len(licitacoes)} licitações...")
    
    por_id = {}
    for lic in licitacoes:
        # Usa a chave 'id' gerada de forma única pelo app.py (cnpj+ano+seq)
        lic_id = str(lic.get('id', ''))
        
        if not lic_id:
            continue
            
        # Mantém sempre a versão mais atual/completa processada
        por_id[lic_id] = lic
    
    resultado = list(por_id.values())
    logger.info(f"✅ Após deduplicação (Safety Net): {len(resultado)} licitações únicas prontas.")
    return resultado

def otimizar_peso_json(licitacao):
    """
    Limpa chaves internas de debug que não são usadas pelo Painel,
    deixando o arquivo .gz super leve para carregamento web.
    """
    chaves_para_remover = ['_metadata', '_raw', 'api_fonte']
    for campo in chaves_para_remover:
        licitacao.pop(campo, None)
    
    # Confia 100% no val_tot e na data gerados pelo app.py (não formata novamente)
    return licitacao

def salvar_resultado(licitacoes):
    """Salva o resultado final comprimido e pronto para o Front-End e Portfólio."""
    try:
        with gzip.open(ARQ_SAIDA, 'wt', encoding='utf-8') as f:
            # indent=None deixa o arquivo minificado (menor tamanho em disco)
            json.dump(licitacoes, f, ensure_ascii=False)
        
        tamanho = os.path.getsize(ARQ_SAIDA)
        tamanho_kb = tamanho / 1024
        logger.info(f"💾 Salvo com sucesso: {ARQ_SAIDA} ({len(licitacoes)} registros, {tamanho_kb:.1f} KB)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro crítico ao salvar json final: {e}")
        raise

def main():
    logger.info("🚀 Iniciando QA e Compressão (limpeza.py)")
    
    # 1. Carrega os dados perfeitos do app.py
    licitacoes = carregar_licitacoes()
    if not licitacoes:
        logger.warning("⚠️ O app.py não gerou licitações ou o arquivo está vazio.")
        return
    
    # 2. Garante que não há repetições
    licitacoes = deduplicar(licitacoes)
    
    # 3. Minifica e retira lixo de memória
    licitacoes = [otimizar_peso_json(lic) for lic in licitacoes]
    
    # 4. Salva (Atenção: Não existe mais filtro de palavras aqui. O app.py já fez!)
    if licitacoes:
        salvar_resultado(licitacoes)
        logger.info("✅ Limpeza e Otimização concluídas perfeitamente! Passando bastão para Avaliação.")

if __name__ == "__main__":
    main()
