import json
import gzip
import re
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def carregar_licitacoes():
    """Carrega todas as licitações dos arquivos JSON."""
    licitacoes = []
    arquivos = list(Path('.').glob('licitacoes_*.json*'))

    logger.info(f"📂 Encontrados {len(arquivos)} arquivos de licitações")

    for arquivo in arquivos:
        try:
            if str(arquivo).endswith('.gz'):
                with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
                    dados = json.load(f)
            else:
                with open(arquivo, 'r', encoding='utf-8') as f:
                    dados = json.load(f)

            if isinstance(dados, list):
                licitacoes.extend(dados)
            else:
                licitacoes.append(dados)

            logger.info(f"✅ {arquivo}: {len(dados) if isinstance(dados, list) else 1} registros")

        except Exception as e:
            logger.error(f"❌ Erro ao carregar {arquivo}: {e}")
            continue

    return licitacoes

def normalizar_id(licitacao):
    """Extrai ID normalizado da licitação."""
    # Tentar vários campos possíveis
    for campo in ['id', 'numeroControlePNCP', 'numeroCompra', 'codigo']:
        if campo in licitacao and licitacao[campo]:
            return str(licitacao[campo]).strip()

    # Fallback: criar ID a partir de orgao + edital
    orgao = licitacao.get('orgao', '') or licitacao.get('org', '')
    edital = licitacao.get('edital', '') or licitacao.get('numeroEdital', '')
    if orgao and edital:
        return f"{orgao}_{edital}"

    return None

def deduplicar(licitacoes):
    """Remove duplicatas mantendo a versão mais completa."""
    logger.info(f"🔍 Iniciando deduplicação de {len(licitacoes)} licitações")

    # Agrupar por ID
    por_id = {}
    for lic in licitacoes:
        lic_id = normalizar_id(lic)
        if not lic_id:
            logger.warning(f"⚠️ Licitação sem ID identificável: {lic}")
            continue

        if lic_id not in por_id:
            por_id[lic_id] = []
        por_id[lic_id].append(lic)

    logger.info(f"📊 {len(por_id)} IDs únicos encontrados")

    # Para cada ID, escolher a melhor versão
    resultado = []
    for lic_id, versoes in por_id.items():
        if len(versoes) == 1:
            resultado.append(versoes[0])
        else:
            # Escolher versão com mais dados (mais campos preenchidos)
            melhor = max(versoes, key=lambda x: len([v for v in x.values() if v]))
            resultado.append(melhor)
            logger.debug(f"🔄 {lic_id}: {len(versoes)} versões, mantida a mais completa")

    logger.info(f"✅ Após deduplicação: {len(resultado)} licitações")
    return resultado

def limpar_dados(licitacao):
    """Limpa e normaliza campos de uma licitação."""
    # Remover campos vazios ou nulos desnecessários
    campos_remover = ['_metadata', '_raw', 'versao']
    for campo in campos_remover:
        licitacao.pop(campo, None)

    # Normalizar datas
    for campo in ['dt_enc', 'dataEncerramento', 'dataPublicacao']:
        if campo in licitacao and licitacao[campo]:
            try:
                # Tentar padronizar formato ISO
                data = datetime.fromisoformat(str(licitacao[campo]).replace('Z', '+00:00'))
                licitacao[campo] = data.isoformat()
            except:
                pass

    # Normalizar valores monetários
    for campo in ['val_tot', 'valorTotal', 'valorEstimado']:
        if campo in licitacao and licitacao[campo]:
            try:
                valor = float(str(licitacao[campo]).replace('R$', '').replace('.', '').replace(',', '.'))
                licitacao[campo] = valor
            except:
                pass

    return licitacao

def filtrar_relevantes(licitacoes):
    """Filtra apenas licitações relevantes (medicamentos/material médico)."""
    logger.info(f"🔍 Filtrando {len(licitacoes)} licitações por relevância")

    palavras_chave = [
        'MEDICAMENTO', 'FARMACO', 'MEDICAMENTO', 'VACINA', 'IMUNIZANTE',
        'MATERIAL MEDICO', 'INSUMO HOSPITALAR', 'GAZE', 'LUVA', 'SERINGA',
        'AGULHA', 'SONDA', 'CATETER', 'EQUIPO', 'FRALDA', 'ALGODAO',
        'ANTISEPTICO', 'ANALGESICO', 'ANTIBIOTICO', 'HOSPITALAR'
    ]

    relevantes = []
    for lic in licitacoes:
        texto = ' '.join([
            str(lic.get('obj', '')),
            str(lic.get('objeto', '')),
            str(lic.get('descricao', ''))
        ]).upper()

        if any(palavra in texto for palavra in palavras_chave):
            relevantes.append(lic)

    logger.info(f"✅ {len(relevantes)} licitações relevantes encontradas")
    return relevantes

def salvar_resultado(licitacoes):
    """Salva resultado limpo em arquivo JSON comprimido."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_saida = f"pregacoes_pharma_limpos_{timestamp}.json.gz"

    try:
        with gzip.open(arquivo_saida, 'wt', encoding='utf-8') as f:
            json.dump(licitacoes, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Resultado salvo: {arquivo_saida}")

        # Também salvar como arquivo não comprimido para fácil acesso
        arquivo_json = f"pregacoes_pharma_limpos_{timestamp}.json"
        with open(arquivo_json, 'w', encoding='utf-8') as f:
            json.dump(licitacoes, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Também salvo como: {arquivo_json}")

        return arquivo_saida
    except Exception as e:
        logger.error(f"❌ Erro ao salvar: {e}")
        raise

def main():
    """Fluxo principal de limpeza."""
    logger.info("🚀 Iniciando limpeza de licitações")

    # 1. Carregar
    licitacoes = carregar_licitacoes()
    if not licitacoes:
        logger.warning("⚠️ Nenhuma licitação encontrada para limpar")
        return

    # 2. Deduplicar
    licitacoes = deduplicar(licitacoes)

    # 3. Limpar dados individuais
    licitacoes = [limpar_dados(lic) for lic in licitacoes]

    # 4. Filtrar relevantes
    licitacoes = filtrar_relevantes(licitacoes)

    # 5. Salvar
    if licitacoes:
        salvar_resultado(licitacoes)
        logger.info(f"✅ Limpeza concluída: {len(licitacoes)} licitações processadas")
    else:
        logger.warning("⚠️ Nenhuma licitação relevante após filtragem")

if __name__ == "__main__":
    main()
