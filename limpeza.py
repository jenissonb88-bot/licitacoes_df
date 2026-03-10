import json
import gzip
import re
from datetime import datetime
from pathlib import Path
import logging
import os

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ✅ NOMES FIXOS DO FLUXO
ARQ_ENTRADA = 'dadosoportunidades.json.gz'      # Do app.py
ARQ_SAIDA = 'pregacoes_pharma_limpos.json.gz'   # Para avalia_portfolio.py

def carregar_licitacoes():
    """Carrega licitações do arquivo dadosoportunidades.json.gz."""
    licitacoes = []
    
    # ✅ Procura arquivo específico do app.py
    arquivo = Path(ARQ_ENTRADA)
    
    if not arquivo.exists():
        logger.error(f"❌ Arquivo não encontrado: {ARQ_ENTRADA}")
        logger.info(f"   📁 Diretório atual: {os.getcwd()}")
        logger.info(f"   📁 Arquivos disponíveis: {[f for f in os.listdir('.') if '.json' in f]}")
        return []
    
    try:
        with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
            dados = json.load(f)

        if isinstance(dados, list):
            licitacoes.extend(dados)
            logger.info(f"✅ {arquivo}: {len(dados)} registros carregados")
        else:
            logger.warning(f"⚠️ Formato inesperado: {type(dados)}")

    except Exception as e:
        logger.error(f"❌ Erro ao carregar {arquivo}: {e}")
        return []

    logger.info(f"📊 Total para processar: {len(licitacoes)} licitações")
    return licitacoes

def normalizar_id(licitacao):
    """Extrai ID normalizado da licitação."""
    for campo in ['id', 'numeroControlePNCP', 'numeroCompra']:
        if campo in licitacao and licitacao[campo]:
            return str(licitacao[campo]).strip()
    
    # Fallback
    orgao = licitacao.get('orgao', '') or licitacao.get('org', '')
    edital = licitacao.get('edital', '') or licitacao.get('edit', '')
    if orgao and edital:
        return f"{orgao}_{edital}"
    
    return None

def deduplicar(licitacoes):
    """Remove duplicatas mantendo a versão mais completa."""
    logger.info(f"🔍 Deduplicando {len(licitacoes)} licitações")
    
    por_id = {}
    for lic in licitacoes:
        lic_id = normalizar_id(lic)
        if not lic_id:
            continue
            
        if lic_id not in por_id:
            por_id[lic_id] = []
        por_id[lic_id].append(lic)
    
    resultado = []
    for lic_id, versoes in por_id.items():
        if len(versoes) == 1:
            resultado.append(versoes[0])
        else:
            # Escolher versão com mais campos preenchidos
            melhor = max(versoes, key=lambda x: len([v for v in x.values() if v]))
            resultado.append(melhor)
            logger.debug(f"🔄 {lic_id}: {len(versoes)} versões → 1")
    
    logger.info(f"✅ Após deduplicação: {len(resultado)} licitações")
    return resultado

def limpar_dados(licitacao):
    """Limpa e normaliza campos."""
    # Remover campos internos
    for campo in ['_metadata', '_raw', 'api_fonte']:
        licitacao.pop(campo, None)
    
    # Normalizar datas
    for campo in ['dt_enc', 'dataEncerramento']:
        if campo in licitacao and licitacao[campo]:
            try:
                data = datetime.fromisoformat(str(licitacao[campo]).replace('Z', '+00:00'))
                licitacao[campo] = data.isoformat()
            except:
                pass
    
    # Normalizar valores
    for campo in ['val_tot', 'valorTotal']:
        if campo in licitacao and licitacao[campo]:
            try:
                valor = float(str(licitacao[campo]).replace('R$', '').replace('.', '').replace(',', '.'))
                licitacao[campo] = valor
            except:
                pass
    
    return licitacao

def filtrar_relevantes(licitacoes):
    """Filtra licitações relevantes (medicamentos/material médico)."""
    logger.info(f"🔍 Filtrando {len(licitacoes)} licitações")
    
    palavras_chave = [
        'MEDICAMENT', 'FARMAC', 'VACINA', 'IMUNIZANTE',
        'MATERIAL MEDIC', 'INSUMO HOSPITALAR', 'MMH',
        'GAZE', 'LUVA', 'SERINGA', 'AGULHA', 'SONDA', 'CATETER',
        'FRALDA', 'ALGODAO', 'ANTISEPTICO', 'ANALGESICO',
        'ANTIBIOTICO', 'HOSPITALAR', 'NUTRICAO ENTERAL'
    ]
    
    relevantes = []
    for lic in licitacoes:
        texto = ' '.join([
            str(lic.get('obj', '')),
            str(lic.get('objeto', '')),
            str(lic.get('d', ''))  # descrição de itens
        ]).upper()
        
        if any(p in texto for p in palavras_chave):
            relevantes.append(lic)
    
    logger.info(f"✅ {len(relevantes)}/{len(licitacoes)} licitações relevantes")
    return relevantes

def salvar_resultado(licitacoes):
    """Salva resultado em pregacoes_pharma_limpos.json.gz."""
    try:
        with gzip.open(ARQ_SAIDA, 'wt', encoding='utf-8') as f:
            json.dump(licitacoes, f, ensure_ascii=False, indent=2)
        
        tamanho = os.path.getsize(ARQ_SAIDA)
        logger.info(f"💾 Salvo: {ARQ_SAIDA} ({len(licitacoes)} regs, {tamanho} bytes)")
        return ARQ_SAIDA
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar: {e}")
        raise

def main():
    logger.info("🚀 Iniciando limpeza")
    
    # 1. Carregar
    licitacoes = carregar_licitacoes()
    if not licitacoes:
        logger.warning("⚠️ Nada para processar")
        return
    
    # 2. Deduplicar
    licitacoes = deduplicar(licitacoes)
    
    # 3. Limpar
    licitacoes = [limpar_dados(lic) for lic in licitacoes]
    
    # 4. Filtrar
    licitacoes = filtrar_relevantes(licitacoes)
    
    # 5. Salvar
    if licitacoes:
        salvar_resultado(licitacoes)
        logger.info(f"✅ Concluído: {len(licitacoes)} licitações limpas")
    else:
        logger.warning("⚠️ Nenhuma licitação relevante após filtro")

if __name__ == "__main__":
    main()
