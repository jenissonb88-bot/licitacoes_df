import json
import gzip
import csv
import gc
import logging
import os
import re

def destacar_item_inteligente(descricao_original, termos_encontrados):
    """
    Aplica Destaque Baseado em Tokens com Regra de Hierarquia (Chave Mestra),
    Expansão de Fronteira, Flexibilidade de Espaços e Tolerância a Acentos.
    """
    if not termos_encontrados:
        return descricao_original

    STOPWORDS = {"DE", "DO", "DA", "COM", "SEM", "PARA", "EM", "E", "OU", "A", "O"}
    
    # 1. Quebra os termos encontrados em palavras soltas (Tokens)
    tokens = set()
    for termo in termos_encontrados:
        unidades = r'(MG|ML|G|KG|UI|MCG|AMP|CPR|CX|CAPS|L)'
        termo_ajustado = re.sub(rf'(\d)\s+{unidades}\b', r'\1\2', termo, flags=re.IGNORECASE)
        
        for palavra in termo_ajustado.split():
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

    # 3. REGRA 2: A Chave Mestra e Flexibilidade de Acentos
    def flexibilizar_palavra(f):
        # Primeiro trata os sufixos (PAM/PAN, INO/INA)
        SUFIXOS = {
            'PAM': ('PA', '[MN]'), 'PAN': ('PA', '[MN]'),
            'INO': ('IN', '[OÓÒÔÕÖAÁÀÂÃÄ]'), 'INA': ('IN', '[OÓÒÔÕÖAÁÀÂÃÄ]'),
            'ONA': ('ON', '[EÉÈÊËAÁÀÂÃÄ]'), 'ONE': ('ON', '[EÉÈÊËAÁÀÂÃÄ]')
        }
        base = f
        final_regex = ""
        for suf, (prefixo, regex_char) in SUFIXOS.items():
            if f.endswith(suf):
                base = f[:-3] + prefixo
                final_regex = regex_char
                break
        
        # Agora injeta a tolerância a acentos em todas as vogais e no C
        mapa = {
            'A': '[AÁÀÂÃÄ]', 'E': '[EÉÈÊË]', 'I': '[IÍÌÎÏ]',
            'O': '[OÓÒÔÕÖ]', 'U': '[UÚÙÛÜ]', 'C': '[CÇ]'
        }
        res = ""
        for char in base:
            res += mapa.get(char.upper(), re.escape(char))
        return res + final_regex

    chave_mestra_ativa = False
    regex_farmacos = []
    
    for f in farmacos_formas:
        padrao = flexibilizar_palavra(f)
        regex_farmacos.append(padrao)
        
        if not chave_mestra_ativa and re.search(rf'\b{padrao}\b', descricao_original, re.IGNORECASE):
            chave_mestra_ativa = True

    if len(farmacos_formas) > 0 and not chave_mestra_ativa:
        return descricao_original

    # 4. REGRA 1: Construir os padrões das Dosagens
    regex_dosagens = []
    for d in dosagens:
        padrao_dos = re.escape(d)
        padrao_dos = re.sub(r'(\d)(?:\\ )*([A-Za-z]+)', r'\1\\s*\2', padrao_dos)
        # Permite caracteres com acento no final da dosagem, se houver
        padrao_dos = rf"{padrao_dos}(?:\s*/\s*[A-Za-z0-9À-ÿ]+)?"
        regex_dosagens.append(padrao_dos)

    # 5. Juntar tudo num único Super Regex
    todos_padroes = regex_farmacos + regex_dosagens
    if not todos_padroes:
        return descricao_original
        
    super_regex = r'\b(' + '|'.join(todos_padroes) + r')\b'
    
    # 6. Substituição Adicionando o Destaque
    texto_destacado = re.sub(super_regex, r'<mark><b>\1</b></mark>', descricao_original, flags=re.IGNORECASE)
    
    return texto_destacado

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# CONFIGURAÇÃO DE ARQUIVOS
ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
ARQUIVO_DICIONARIO = 'dicionario_ouro.json'
ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
TAMANHO_LOTE = 500 

def normalizar_texto(texto):
    if not texto: return ""
    from unicodedata import normalize
    texto_limpo = "".join(c for c in normalize('NFD', str(texto).upper()) if not (ord(c) >= 768 and ord(c) <= 879))
    return texto_limpo.strip()

def carregar_dicionario_ouro():
    if not os.path.exists(ARQUIVO_DICIONARIO):
        logger.error(f"❌ Erro: {ARQUIVO_DICIONARIO} não encontrado.")
        return []
    
    with open(ARQUIVO_DICIONARIO, 'r', encoding='utf-8') as f:
        termos = json.load(f)
        return [normalizar_texto(t) for t in termos if t]

def processar_lote(lote_licitacoes, arquivo_saida_csv, termos_ouro):
    resultados_lote = []
    
    for licitacao in lote_licitacoes:
        itens = licitacao.get('itens', [])
        for item in itens:
            desc_original = item.get('d', '')
            desc_norm = normalizar_texto(desc_original)
            
            if not desc_norm or desc_norm == 'NONE':
                continue
            
            matches = []
            
            for termo in termos_ouro:
                termo_esc = re.escape(termo)
                termo_esc = re.sub(r'(\d)(?:\\ )*([A-Za-z]+)', r'\1\\s*\2', termo_esc)
                padrao = r'\b' + termo_esc + r'\b'
                
                if re.search(padrao, desc_norm):
                    matches.append(termo)
            
            if matches:
                # 🚀 A mágica do destaque acontece aqui
                descricao_destacada = destacar_item_inteligente(desc_original, matches)

                resultados_lote.append({
                    'id_licitacao': licitacao.get('id', ''),
                    'orgao': licitacao.get('org', ''),
                    'item_num': item.get('n', ''),
                    'descricao_item': descricao_destacada,
                    'termo_encontrado': " | ".join(matches)
                })
                
    if resultados_lote:
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
            
    return len(resultados_lote)

def main():
    logger.info("🚀 Iniciando Sniper de Portfólio (Dicionário de Ouro)")
    
    termos_ouro = carregar_dicionario_ouro()
    if not termos_ouro:
        logger.error("🛑 Abortando: Dicionário vazio.")
        return
    
    logger.info(f"🧠 Inteligência carregada: {len(termos_ouro)} termos de busca.")

    with open(ARQUIVO_SAIDA, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
        writer.writeheader()

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

    for i in range(0, total_licitacoes, TAMANHO_LOTE):
        lote = licitacoes[i:i + TAMANHO_LOTE]
        encontrados = processar_lote(lote, ARQUIVO_SAIDA, termos_ouro)
        total_matches += encontrados
        
        del lote
        gc.collect() 
        
        progresso = min(100, (i + TAMANHO_LOTE) / total_licitacoes * 100)
        if (i // TAMANHO_LOTE) % 5 == 0:
            logger.info(f"    ⏳ Progresso: {progresso:.1f}% | Encontrados: {total_matches}")

    logger.info(f"✅ Concluído! O relatório '{ARQUIVO_SAIDA}' foi gerado com {total_matches} itens compatíveis.")

if __name__ == '__main__':
    main()
