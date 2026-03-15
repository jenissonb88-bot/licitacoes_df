import json
import gzip
import csv
import os
import re
import unicodedata
import logging

# --- CONFIGURAÇÕES ---
ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
ARQUIVO_DICIONARIO = 'dicionario_ouro.json'
ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# --- MURALHA DE BLOQUEIO (FALSOS POSITIVOS) ---
# Adicione ou remova termos conforme a sua necessidade diária
BLACKLIST = {
    # Alimentação
    "BISCOITO", "FARINHA", "ACUCAR", "DOCE", "BOLO", "MISTURA", "RACAO", "PAO", "ARROZ", 
    "MACARRAO", "LEITE", "SUCO", "MERENDA", "ALIMENTO", "CESTA", "HORTIFRUTI", "CARNE", 
    "FRANGO", "PEIXE", "FEIJAO", "CAFE", "ACHOCOLATADO", "SOPA", "POLPA", "IOGURTE", "BEBIDA",
    
    # Frota, Materiais e Administrativo
    "AMBULANCIA", "PNEU", "PAPEL", "CANETA", "TONER", "COMPUTADOR", "VEICULO", "CARTUCHO", 
    "IMPRESSORA", "MOTO", "TRATOR", "ESCOLA", "LIMPEZA", "DETERGENTE", "SABAO", "OBRA", 
    "CIMENTO", "ASFALTO", "TINTA", "LIXO", "FUNERARI", "URNA", "COPO", "DESCARTAVEL", 
    "OFFICE", "CADEIRA", "MESA", "AR CONDICIONADO", "GRAMPEADOR", "LIVRO", "MOCHILA", "UNIFORME"
}

def normalizar(texto):
    """Remove acentos e converte para maiúsculas para facilitar a busca."""
    if not texto: return ""
    t = ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if not unicodedata.combining(c))
    return t.upper()

def criar_regex_token(token):
    """Cria expressões regulares flexíveis para capturar variações do pregoeiro."""
    if token.isalpha():
        # Se for só letras (Nome do remédio): Tolerância a A/O e S/Z
        t = token.upper()
        t = t.replace('S', '[SZ]').replace('Z', '[SZ]')
        t = t.replace('I', '[IY]').replace('Y', '[IY]')
        if t.endswith('A') or t.endswith('O'):
            t = t[:-1] + '[AO]'
        return t + r'S?' # Permite que esteja no plural (ex: AMPOLAS)
    else:
        # Se for dosagem ou medida (ex: 10MG/ML ou 0,9%)
        t = re.escape(token.upper())
        # Permite espaços entre números e letras (ex: 10MG vira 10 MG)
        t = re.sub(r'(\d)(\w)', r'\1\\s*\2', t)
        # Permite espaços ao redor de barras e vírgulas
        t = t.replace(r'\/', r'\s*/\s*').replace(r'\,', r'\s*,\s*')
        return t

def preparar_dicionario(termos_brutos):
    """Transforma a sua lista do dicionário numa máquina de tokens."""
    dicionario_inteligente = []
    for termo in termos_brutos:
        termo = normalizar(termo).strip()
        if not termo: continue
        
        tokens = termo.split()
        dicionario_inteligente.append({
            'original': termo,
            # Core = Palavras-chave (O nome do Fármaco)
            'core': [t for t in tokens if t.isalpha()],
            # Atributos = Dosagens, percentuais e unidades (10MG, 0,9%, etc)
            'attrs': [t for t in tokens if not t.isalpha()]
        })
    return dicionario_inteligente

def processar_item(desc_original, dicionario_inteligente):
    """Aplica as barreiras e o marca-texto num item específico."""
    desc_norm = normalizar(desc_original)

    # 🛑 BARREIRA 1: Blacklist (O Item morre aqui se for pão ou pneu)
    if any(b in desc_norm for b in BLACKLIST):
        return None, None

    # 🎯 BARREIRA 2: Busca da Chave Mestra
    for termo in dicionario_inteligente:
        core_matches = []
        
        # Procura os nomes dos fármacos (Core)
        for ct in termo['core']:
            # (?<!\w) e (?!\w) funcionam como \b, mas suportam símbolos como % melhor
            regex_ct = r'(?<!\w)' + criar_regex_token(ct) + r'(?!\w)'
            if re.search(regex_ct, desc_norm):
                core_matches.append(ct)

        # Se achou TODAS as palavras principais do fármaco...
        if core_matches and len(core_matches) == len(termo['core']):
            
            # Prepara a tinta amarela para os nomes do remédio...
            tokens_para_pintar = [criar_regex_token(ct) for ct in termo['core']]

            # ...e também procura as dosagens perdidas na frase (CATMAT)
            for at in termo['attrs']:
                regex_at = r'(?<!\w)' + criar_regex_token(at) + r'(?!\w)'
                if re.search(regex_at, desc_norm):
                    tokens_para_pintar.append(criar_regex_token(at))

            # 🖌️ APLICAÇÃO DO MARCA-TEXTO
            # Junta todos os tokens que achou numa super regex
            super_regex = r'(?<!\w)(' + '|'.join(tokens_para_pintar) + r')(?!\w)'
            
            # Pinta o texto ORIGINAL (mantendo a formatação e as minúsculas/maiúsculas que o pregoeiro usou)
            desc_pintada = re.sub(super_regex, r'<mark><b>\g<1></b></mark>', desc_original, flags=re.IGNORECASE)

            return desc_pintada, termo['original']

    # Se não achou nenhum medicamento, ignora
    return None, None

def main():
    if not os.path.exists(ARQUIVO_DICIONARIO):
        logging.error("Dicionário não encontrado.")
        return

    with open(ARQUIVO_DICIONARIO, 'r', encoding='utf-8') as f:
        termos_brutos = json.load(f)
    
    dicionario = preparar_dicionario(termos_brutos)
    logging.info(f"Dicionário carregado com {len(dicionario)} estratégias de busca ativa.")

    if not os.path.exists(ARQUIVO_ENTRADA):
        logging.error("Banco de dados JSON não encontrado.")
        return

    with gzip.open(ARQUIVO_ENTRADA, 'rt', encoding='utf-8') as f:
        licitacoes = json.load(f)

    resultados_finais = []
    
    logging.info("A auditar itens e a aplicar filtros de destaque inteligente...")

    for licitacao in licitacoes:
        for item in licitacao.get('itens', []):
            desc_original = item.get('d', '')
            
            # Passa o item pela nova inteligência
            desc_pintada, termo_encontrado = processar_item(desc_original, dicionario)
            
            if desc_pintada:
                resultados_finais.append({
                    'id_licitacao': licitacao.get('id'),
                    'orgao': licitacao.get('org'),
                    'item_num': item.get('n'),
                    'descricao_item': desc_pintada,
                    'termo_encontrado': termo_encontrado
                })

    # 💾 GRAVAÇÃO (MODO SOBREPOR TOTAL)
    # Como abrimos com 'w', ele destrói os dados velhos e reescreve com a versão mais atualizada
    with open(ARQUIVO_SAIDA, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
        writer.writeheader()
        writer.writerows(resultados_finais)

    logging.info(f"✅ Concluído! Relatório CSV sobreposto e atualizado com {len(resultados_finais)} itens de ouro.")

if __name__ == '__main__':
    main()
