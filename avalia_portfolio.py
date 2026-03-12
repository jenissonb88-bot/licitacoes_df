import json
import gzip
import csv
import gc
import logging
import os
import re
from math import ceil

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

SINONIMOS_FARMACOS = {
    "GLICOSE": {"GLICOSE INJETAVEL", "GLICOSE INJETÁVEL", "DEXTROSE"},
    "AMINOACIDO": {"AMINOÁCIDO", "AMINOACIDOS", "AMINOÁCIDOS"},
    "HEMODIALISE": {"HEMODIÁLISE", "DIALISE", "DIÁLISE", "TERAPIA RENAL"},
    "FOSFORO": {"FOSFORICO", "FOSFÓRICO", "FO"},
    "VITAMINA": {"VITAMINA C", "ACIDO ASCORBICO", "ÁCIDO ASCÓRBICO", "VITAMINA"},
    "AAS": {"AAS", "ACIDO ACETILSALICILICO", "ÁCIDO ACETILSALICÍLICO"},
    "MATERIAL_HOSPITALAR": {"MATERIAL MÉDICO-HOSPITALAR", "MÉDICO-HOSPITALAR"}
}

TERMOS_GENERICOS = {
    "FRASCO", "FRASCOS", "AMPOLA", "AMPOLAS", "BOLSA", "BOLSAS", "CAIXA", "CX", 
    "COMPRIMIDO", "COMPRIMIDOS", "CPR", "CAPSULA", "CAPSULAS", "INJETAVEL", 
    "INJ", "GOTAS", "XAROPE", "SOLUCAO", "MG", "ML", "KG", "LITRO", "LITROS", 
    "UNIDADE", "UN", "KIT", "KITS", "TIPO", "PARA", "COM", "SEM", "MEDICAMENTO"
}

TERMOS_BUSCA = set(SINONIMOS_FARMACOS.keys())
for sinonimos in SINONIMOS_FARMACOS.values():
    TERMOS_BUSCA.update(sinonimos)

def carregar_portfolio():
    portfolio = []
    arquivo_csv = 'Exportar Dados.csv'
    if not os.path.exists(arquivo_csv): return portfolio

    encodings = ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']
    for enc in encodings:
        try:
            with open(arquivo_csv, mode='r', encoding=enc) as f:
                amostra = f.read(1024)
                f.seek(0)
                delimitador = ';' if ';' in amostra else ','
                reader = csv.DictReader(f, delimiter=delimitador)
                headers = {h.strip().upper(): h for h in reader.fieldnames if h}
                col_desc = next((h_real for h_upper, h_real in headers.items() if 'DESCRI' in h_upper), None)
                if not col_desc: continue
                
                for row in reader:
                    desc_completa = str(row.get(col_desc, '')).strip().upper()
                    if desc_completa:
                        portfolio.append(desc_completa)
                        palavras_cruas = desc_completa.replace(',', ' ').replace('.', ' ').split()
                        for palavra in palavras_cruas:
                            palavra_limpa = re.sub(r'[^A-Z]', '', palavra)
                            if len(palavra_limpa) > 2 and palavra_limpa not in TERMOS_GENERICOS:
                                TERMOS_BUSCA.add(palavra_limpa)
                                break 
            if portfolio: break
        except Exception: continue
    return portfolio

def extrair_componentes(descricao):
    if not descricao: return []
    descricao_limpa = str(descricao).replace(',', ' ').replace('(', ' ').replace(')', ' ').replace('.', ' ')
    return [palavra.strip().upper() for palavra in descricao_limpa.split() if palavra.strip()]

def processar_lote(lote_licitacoes, arquivo_saida_csv):
    resultados_lote = []
    for licitacao in lote_licitacoes:
        for item in licitacao.get('itens', []):
            descricao = str(item.get('d', '')).upper()
            if not descricao or descricao == 'NONE': continue
                
            componentes = extrair_componentes(descricao)
            componentes_set = set(componentes)
            match = componentes_set.intersection(TERMOS_BUSCA)
            
            if not match:
                for termo in TERMOS_BUSCA:
                    if " " in termo and termo in descricao: match.add(termo)
            
            if match:
                resultados_lote.append({
                    'id_licitacao': licitacao.get('id', ''), 'orgao': licitacao.get('org', ''),
                    'item_num': item.get('n', ''), 'descricao_item': item.get('d', ''),
                    'termo_encontrado': " | ".join(list(match))
                })
                
    if resultados_lote:
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
    return len(resultados_lote)

def main():
    ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
    ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
    TAMANHO_LOTE = 500 

    logging.info("🚀 A iniciar a Avaliação de Portfólio (Escalonada)...")
    portfolio = carregar_portfolio()
    
    with open(ARQUIVO_SAIDA, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
        writer.writeheader()

    if not os.path.exists(ARQUIVO_ENTRADA): return
    with gzip.open(ARQUIVO_ENTRADA, 'rt', encoding='utf-8') as f: licitacoes = json.load(f)
    
    total_licitacoes = len(licitacoes)
    total_encontrados = 0

    for i in range(0, total_licitacoes, TAMANHO_LOTE):
        lote = licitacoes[i:i + TAMANHO_LOTE]
        total_encontrados += processar_lote(lote, ARQUIVO_SAIDA)
        del lote
        gc.collect() 
        
    logging.info(f"✅ Avaliação concluída! Compatíveis: {total_encontrados}.")

if __name__ == '__main__':
    main()
