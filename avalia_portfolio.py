import json, gzip, csv, gc, logging, os, re
from datetime import datetime

# --- CONFIGURAÇÃO DE MODO ---
# True: Só analisa o que ainda não está no CSV (Velocidade máxima)
# False: Reanalisa todo o banco de dados (Gera tudo de novo)
MODO_INCREMENTAL = True 

# CONFIGURAÇÃO DE ARQUIVOS
ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
ARQUIVO_DICIONARIO = 'dicionario_ouro.json'
ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
TAMANHO_LOTE = 500 

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def destacar_item_inteligente(descricao_original, termos_encontrados):
    if not termos_encontrados: return descricao_original
    STOPWORDS = {"DE", "DO", "DA", "COM", "SEM", "PARA", "EM", "E", "OU", "A", "O"}
    
    tokens = set()
    for termo in termos_encontrados:
        unidades = r'(MG|ML|G|KG|UI|MCG|AMP|CPR|CX|CAPS|L)'
        termo_ajustado = re.sub(rf'(\d)\s+{unidades}\b', r'\1\2', termo, flags=re.IGNORECASE)
        for palavra in termo_ajustado.split():
            if palavra not in STOPWORDS and len(palavra) > 1: tokens.add(palavra)
                
    if not tokens: return descricao_original

    farmacos_formas, dosagens = [], []
    for t in tokens:
        if any(char.isdigit() for char in t): dosagens.append(t)
        else: farmacos_formas.append(t)

    def flexibilizar_palavra(f):
        mapa = {'A': '[AÁÀÂÃÄ]', 'E': '[EÉÈÊË]', 'I': '[IÍÌÎÏ]', 'O': '[OÓÒÔÕÖ]', 'U': '[UÚÙÛÜ]', 'C': '[CÇ]'}
        res = ""
        for char in f: res += mapa.get(char.upper(), re.escape(char))
        return res

    chave_mestra_ativa = False
    regex_farmacos = []
    for f in farmacos_formas:
        padrao = flexibilizar_palavra(f)
        regex_farmacos.append(padrao)
        if not chave_mestra_ativa and re.search(rf'\b{padrao}\b', descricao_original, re.IGNORECASE):
            chave_mestra_ativa = True

    if farmacos_formas and not chave_mestra_ativa: return descricao_original

    regex_dosagens = []
    for d in dosagens:
        padrao_dos = re.escape(d)
        padrao_dos = re.sub(r'(\d)(?:\\ )*([A-Za-z]+)', r'\1\\s*\2', padrao_dos)
        padrao_dos = rf"{padrao_dos}(?:\s*/\s*[A-Za-z0-9À-ÿ]+)?"
        regex_dosagens.append(padrao_dos)

    todos_padroes = regex_farmacos + regex_dosagens
    if not todos_padroes: return descricao_original
    super_regex = r'\b(' + '|'.join(todos_padroes) + r')\b'
    return re.sub(super_regex, r'<mark><b>\1</b></mark>', descricao_original, flags=re.IGNORECASE)

def normalizar_texto(texto):
    if not texto: return ""
    from unicodedata import normalize
    return "".join(c for c in normalize('NFD', str(texto).upper()) if not (ord(c) >= 768 and ord(c) <= 879)).strip()

def carregar_ids_processados(arquivo_csv):
    ids = set()
    if MODO_INCREMENTAL and os.path.exists(arquivo_csv):
        try:
            with open(arquivo_csv, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader: ids.add(row['id_licitacao'])
        except: pass
    return ids

def processar_lote(lote_licitacoes, arquivo_saida_csv, termos_ouro, ids_ja_feitos):
    resultados_lote = []
    for licitacao in lote_licitacoes:
        lic_id = licitacao.get('id')
        if MODO_INCREMENTAL and lic_id in ids_ja_feitos: continue

        for item in licitacao.get('itens', []):
            desc_orig = item.get('d', '')
            desc_norm = normalizar_texto(desc_orig)
            if not desc_norm or desc_norm == 'NONE': continue
            
            matches = [t for t in termos_ouro if re.search(r'\b' + re.sub(r'(\d)\s*([A-Z])', r'\1\\s*\2', re.escape(t)) + r'\b', desc_norm)]
            
            if matches:
                desc_formatada = destacar_item_inteligente(desc_orig, matches)
                resultados_lote.append({
                    'id_licitacao': lic_id,
                    'orgao': licitacao.get('org', ''),
                    'item_num': item.get('n', ''),
                    'descricao_item': desc_formatada,
                    'termo_encontrado': " | ".join(matches)
                })
                
    if resultados_lote:
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
    return len(resultados_lote)

def main():
    if not os.path.exists(ARQUIVO_DICIONARIO): return
    with open(ARQUIVO_DICIONARIO, 'r', encoding='utf-8') as f:
        termos_ouro = [normalizar_texto(t) for t in json.load(f) if t]

    if not MODO_INCREMENTAL or not os.path.exists(ARQUIVO_SAIDA):
        with open(ARQUIVO_SAIDA, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writeheader()
        ids_ja_feitos = set()
    else:
        ids_ja_feitos = carregar_ids_processados(ARQUIVO_SAIDA)

    if not os.path.exists(ARQUIVO_ENTRADA): return
    with gzip.open(ARQUIVO_ENTRADA, 'rt', encoding='utf-8') as f:
        licitacoes = json.load(f)
    
    total_matches = 0
    for i in range(0, len(licitacoes), TAMANHO_LOTE):
        lote = licitacoes[i:i + TAMANHO_LOTE]
        total_matches += processar_lote(lote, ARQUIVO_SAIDA, termos_ouro, ids_ja_feitos)
        gc.collect() 
        if (i // TAMANHO_LOTE) % 5 == 0: logger.info(f"⏳ Progresso: {(i/len(licitacoes)*100):.1f}%")

    logger.info(f"✅ Concluído! Relatório gerado com {total_matches} novos itens.")

if __name__ == '__main__':
    main()
