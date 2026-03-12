import json
import gzip
import csv
import gc
import logging
import os
import re
from math import ceil

# Configuração de Logs para o GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ✅ CONFIGURAÇÃO DE ARQUIVOS
ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
ARQUIVO_DICIONARIO = 'dicionario_ouro.json'
ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
TAMANHO_LOTE = 500 

def normalizar_texto(texto):
    """Remove acentos e padroniza para busca limpa."""
    if not texto: return ""
    from unicodedata import normalize
    # Remove acentos e converte para maiúsculas
    texto_limpo = "".join(c for c in normalize('NFD', str(texto).upper()) if not (ord(c) >= 768 and ord(c) <= 879))
    return texto_limpo.strip()

def carregar_dicionario_ouro():
    """Carrega os termos de alta precisão (Princípios Ativos + N-Grams)."""
    if not os.path.exists(ARQUIVO_DICIONARIO):
        logger.error(f"❌ Erro: {ARQUIVO_DICIONARIO} não encontrado. Rode o gerador_dicionario.py primeiro.")
        return []
    
    with open(ARQUIVO_DICIONARIO, 'r', encoding='utf-8') as f:
        termos = json.load(f)
        # Retorna uma lista de termos normalizados
        return [normalizar_texto(t) for t in termos if t]

def processar_lote(lote_licitacoes, arquivo_saida_csv, termos_ouro):
    """Cruza os itens do lote com o Dicionário de Ouro."""
    resultados_lote = []
    
    for licitacao in lote_licitacoes:
        itens = licitacao.get('itens', [])
        for item in itens:
            # Pega a descrição do item (chave 'd' vinda do app.py)
            desc_original = item.get('d', '')
            desc_norm = normalizar_texto(desc_original)
            
            if not desc_norm or desc_norm == 'NONE':
                continue
            
            matches = []
            
            # ✅ BUSCA DE ALTA PRECISÃO
            for termo in termos_ouro:
                # Usamos Regex para garantir que o termo seja uma palavra inteira (boundary \b)
                # Isso evita que "SORO" dê match em "TESOURA", por exemplo.
                padrao = r'\b' + re.escape(termo) + r'\b'
                if re.search(padrao, desc_norm):
                    matches.append(termo)
            
            if matches:
                resultados_lote.append({
                    'id_licitacao': licitacao.get('id', ''),
                    'orgao': licitacao.get('org', ''),
                    'item_num': item.get('n', ''),
                    'descricao_item': desc_original,
                    'termo_encontrado': " | ".join(matches)
                })
                
    if resultados_lote:
        # Modo 'a' (append) para escrever os resultados do lote
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
            
    return len(resultados_lote)

def main():
    logger.info("🚀 Iniciando Sniper de Portfólio (Dicionário de Ouro)")
    
    # 1. Carrega a inteligência
    termos_ouro = carregar_dicionario_ouro()
    if not termos_ouro:
        logger.error("🛑 Abortando: Dicionário vazio ou não encontrado.")
        return
    
    logger.info(f"🧠 Inteligência carregada: {len(termos_ouro)} termos de busca.")

    # 2. Prepara o arquivo de saída (Zera o CSV anterior)
    with open(ARQUIVO_SAIDA, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
        writer.writeheader()

    # 3. Carrega o banco de dados das pregações
    if not os.path.exists(ARQUIVO_ENTRADA):
        logger.error(f"❌ Arquivo {ARQUIVO_ENTRADA} não encontrado.")
        return

    try:
        with gzip.open(ARQUIVO_ENTRADA, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)
    except Exception as e:
        logger.error(f"❌ Erro ao ler banco de dados: {e}")
        return
    
    total_licitacoes = len(licitacoes)
    logger.info(f"📊 Analisando {total_licitacoes} licitações...")

    total_matches = 0

    # 4. Processamento em Lotes (Economia de RAM)
    for i in range(0, total_licitacoes, TAMANHO_LOTE):
        lote = licitacoes[i:i + TAMANHO_LOTE]
        encontrados = processar_lote(lote, ARQUIVO_SAIDA, termos_ouro)
        total_matches += encontrados
        
        # Limpeza agressiva de memória
        del lote
        gc.collect() 
        
        progresso = min(100, (i + TAMANHO_LOTE) / total_licitacoes * 100)
        if (i // TAMANHO_LOTE) % 5 == 0:
            logger.info(f"   ⏳ Progresso: {progresso:.1f}% | Encontrados: {total_matches}")

    logger.info(f"✅ Concluído! O relatório '{ARQUIVO_SAIDA}' foi gerado com {total_matches} itens compatíveis.")

if __name__ == '__main__':
    main()
