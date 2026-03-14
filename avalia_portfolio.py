import json
import gzip
import csv
import gc
import logging
import os
import re
from math import ceil

def destacar_item_inteligente(descricao_original, termos_encontrados):
    """
    Aplica Destaque Baseado em Tokens com Regra de Hierarquia (Chave Mestra) 
    e Expansão de Fronteira para concentrações.
    """
    if not termos_encontrados:
        return descricao_original

    STOPWORDS = {"DE", "DO", "DA", "COM", "SEM", "PARA", "EM", "E", "OU", "A", "O"}
    
    # 1. Quebra os termos encontrados em palavras soltas (Tokens)
    tokens = set()
    for termo in termos_encontrados:
        for palavra in termo.split():
            if palavra not in STOPWORDS and len(palavra) > 1:
                tokens.add(palavra)
                
    if not tokens:
        return descricao_original

    # 2. Separar Fármacos/Formas (letras) de Dosagens (números)
    farmacos_formas = []
    dosagens = []
    for t in tokens:
        if any(char.isdigit() for char in t):
            dosagens.append(t)
        else:
            farmacos_formas.append(t)

    # 3. REGRA 2: A Chave Mestra (Obriga a ter pelo menos uma palavra não-numérica no edital)
    chave_mestra_ativa = False
    regex_farmacos = []
    
    for f in farmacos_formas:
        padrao = re.escape(f)
        # Regras flexíveis de sufixo (INO/INA, PAM/PAN, ONA/ONE)
        padrao = re.sub(r'PAM\\b', r'PA[MN]', padrao)
        padrao = re.sub(r'PAN\\b', r'PA[MN]', padrao)
        padrao = re.sub(r'INO\\b', r'IN[OA]', padrao)
        padrao = re.sub(r'INA\\b', r'IN[OA]', padrao)
        padrao = re.sub(r'ONA\\b', r'ON[AE]', padrao)
        padrao = re.sub(r'ONE\\b', r'ON[AE]', padrao)
        regex_farmacos.append(padrao)
        
        # Testa se essa palavra-chave (Fármaco/Forma) existe no texto original
        if not chave_mestra_ativa and re.search(rf'\b{padrao}\b', descricao_original, re.IGNORECASE):
            chave_mestra_ativa = True

    # Se há fármacos no match, mas nenhum foi encontrado no texto (falso positivo), aborta o destaque.
    if len(farmacos_formas) > 0 and not chave_mestra_ativa:
        return descricao_original

    # 4. REGRA 1: Construir os padrões das Dosagens com Expansão de Fronteira
    regex_dosagens = []
    for d in dosagens:
        # Captura a dosagem exata (ex: 20MG) e opcionalmente estica se houver barra (ex: /G, /ML, / 2ML)
        # O \s* permite que ele pegue mesmo se o pregoeiro digitar com espaços "20MG / G"
        padrao_dos = rf"{re.escape(d)}(?:\s*/\s*[A-Z0-9]+)?"
        regex_dosagens.append(padrao_dos)

    # 5. Juntar tudo num único Super Regex
    todos_padroes = regex_farmacos + regex_dosagens
    if not todos_padroes:
        return descricao_original
        
    # O "OR" (|) garante que ele procure todas as palavras simultaneamente sem sobrepor tags HTML
    super_regex = r'\b(' + '|'.join(todos_padroes) + r')\b'
    
    # 6. Substituição Única Adicionando o Destaque (<mark> e <b> para negrito)
    texto_destacado = re.sub(super_regex, r'<mark><b>\1</b></mark>', descricao_original, flags=re.IGNORECASE)
    
    return texto_destacado

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
                # 🚀 AQUI ACONTECE A MÁGICA: Aplica o destaque na descrição antes de salvar!
                descricao_destacada = destacar_item_inteligente(desc_original, matches)

                resultados_lote.append({
                    'id_licitacao': licitacao.get('id', ''),
                    'orgao': licitacao.get('org', ''),
                    'item_num': item.get('n', ''),
                    'descricao_item': descricao_destacada, # <- Salva o texto já com o HTML
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
            logger.info(f"    ⏳ Progresso: {progresso:.1f}% | Encontrados: {total_matches}")

    logger.info(f"✅ Concluído! O relatório '{ARQUIVO_SAIDA}' foi gerado com {total_matches} itens compatíveis.")

if __name__ == '__main__':
    main()
