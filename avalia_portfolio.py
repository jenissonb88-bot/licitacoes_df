import json
import gzip
import csv
import gc
import logging
import os
from math import ceil

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 1. Dicionário de Sinónimos Base
SINONIMOS_FARMACOS = {
    "GLICOSE": {"GLICOSE INJETAVEL", "GLICOSE INJETÁVEL", "DEXTROSE"},
    "AMINOACIDO": {"AMINOÁCIDO", "AMINOACIDOS", "AMINOÁCIDOS"},
    "HEMODIALISE": {"HEMODIÁLISE", "DIALISE", "DIÁLISE", "TERAPIA RENAL"},
    "FOSFORO": {"FOSFORICO", "FOSFÓRICO", "FO"},
    "MATERIAL_HOSPITALAR": {"MATERIAL MÉDICO-HOSPITALAR", "MÉDICO-HOSPITALAR"},
    "VITAMINA_C": {"VITAMINA C", "ACIDO ASCORBICO", "ÁCIDO ASCÓRBICO"},
    "AAS": {"AAS", "ACIDO ACETILSALICILICO", "ÁCIDO ACETILSALICÍLICO"}
}

# Motor de pesquisa central
TERMOS_BUSCA = set(SINONIMOS_FARMACOS.keys())
for sinonimos in SINONIMOS_FARMACOS.values():
    TERMOS_BUSCA.update(sinonimos)

def carregar_portfolio():
    """
    Carrega o portfólio de forma blindada contra formatações do Excel 
    e injeta os princípios ativos no motor de pesquisa.
    """
    portfolio = []
    arquivo_csv = 'Exportar Dados.csv'
    
    if not os.path.exists(arquivo_csv):
        logging.warning(f"⚠️ Ficheiro {arquivo_csv} não encontrado. A usar apenas os termos estáticos.")
        return portfolio

    # Tenta vários padrões de codificação comuns no Windows/Excel
    encodings = ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']
    
    for enc in encodings:
        try:
            with open(arquivo_csv, mode='r', encoding=enc) as f:
                # Amostra para descobrir se o Excel usou ',' ou ';'
                amostra = f.read(1024)
                f.seek(0)
                delimitador = ';' if ';' in amostra else ','
                
                reader = csv.DictReader(f, delimiter=delimitador)
                
                # Normaliza cabeçalhos para ignorar espaços ocultos
                headers = {h.strip().upper(): h for h in reader.fieldnames if h}
                
                # Encontra automaticamente a coluna de descrição
                col_desc = next((h_real for h_upper, h_real in headers.items() if 'DESCRI' in h_upper), None)
                
                if not col_desc:
                    continue # Falhou ao achar a coluna, tenta o próximo encoding
                
                for row in reader:
                    desc_completa = str(row.get(col_desc, '')).strip().upper()
                    if desc_completa:
                        portfolio.append(desc_completa)
                        
                        # EXTRAÇÃO INTELIGENTE: Pega a primeira palavra (geralmente o Fármaco)
                        # Ex: "AAS 100 MG INF" vira "AAS" e vai para o motor de busca
                        palavra_chave = desc_completa.split()[0].replace(',', '').replace('.', '')
                        
                        # Filtra sujeira (números soltos ou siglas muito curtas)
                        if len(palavra_chave) > 2 and not palavra_chave.isdigit():
                            TERMOS_BUSCA.add(palavra_chave)
                            
            # Se chegou aqui e carregou dados, quebra o loop de tentativas
            if portfolio:
                break
        except Exception as e:
            continue
            
    return portfolio

def extrair_componentes(descricao):
    if not descricao:
        return []
    descricao_limpa = str(descricao).replace(',', ' ').replace('(', ' ').replace(')', ' ').replace('.', ' ')
    return [palavra.strip().upper() for palavra in descricao_limpa.split() if palavra.strip()]

def processar_lote(lote_licitacoes, arquivo_saida_csv):
    resultados_lote = []
    
    for licitacao in lote_licitacoes:
        itens = licitacao.get('itens', [])
        for item in itens:
            descricao = str(item.get('descricao', '')).upper()
            componentes = extrair_componentes(descricao)
            
            # Cruzamento exato de palavras (O(1))
            componentes_set = set(componentes)
            match = componentes_set.intersection(TERMOS_BUSCA)
            
            # Cruzamento abrangente (para termos com 2+ palavras como 'TERAPIA RENAL')
            if not match:
                for termo in TERMOS_BUSCA:
                    if " " in termo and termo in descricao:
                        match.add(termo)
            
            if match:
                resultado = {
                    'id_licitacao': licitacao.get('id', ''),
                    'orgao': licitacao.get('orgao', ''),
                    'item_num': item.get('numero', ''),
                    'descricao_item': item.get('descricao', ''),
                    'termo_encontrado': " | ".join(list(match))
                }
                resultados_lote.append(resultado)
                
    if resultados_lote:
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
            
    return len(resultados_lote)

def main():
    ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
    ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
    TAMANHO_LOTE = 500 

    logging.info("🚀 A iniciar a Avaliação de Portfólio...")
    
    # 1. Carrega o Portfólio e alimenta o Motor de Pesquisa ANTES de tudo
    portfolio = carregar_portfolio()
    logging.info(f"📦 Portfólio base carregado com {len(portfolio)} itens.")
    logging.info(f"🧠 Motor de pesquisa configurado com {len(TERMOS_BUSCA)} palavras-chave.")

    arquivo_existe = os.path.isfile(ARQUIVO_SAIDA)
    with open(ARQUIVO_SAIDA, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not arquivo_existe:
            writer.writerow(['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])

    if not os.path.exists(ARQUIVO_ENTRADA):
        logging.error(f"❌ Ficheiro {ARQUIVO_ENTRADA} não encontrado.")
        return

    try:
        with gzip.open(ARQUIVO_ENTRADA, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)
    except Exception as e:
        logging.error(f"❌ Erro ao abrir json: {e}")
        return
    
    total_licitacoes = len(licitacoes)
    total_lotes = ceil(total_licitacoes / TAMANHO_LOTE)
    
    logging.info(f"📊 Total de licitações: {total_licitacoes} | Lotes: {total_lotes}.")

    total_encontrados = 0

    for i in range(0, total_licitacoes, TAMANHO_LOTE):
        lote = licitacoes[i:i + TAMANHO_LOTE]
        num_lote = (i // TAMANHO_LOTE) + 1
        
        encontrados = processar_lote(lote, ARQUIVO_SAIDA)
        total_encontrados += encontrados
        
        del lote
        gc.collect() 
        
    logging.info(f"✅ Avaliação concluída! Total de itens compatíveis encontrados: {total_encontrados}.")

if __name__ == '__main__':
    main()
