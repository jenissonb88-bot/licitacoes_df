import json
import gzip
import csv
import gc
import logging
import os
import re
from math import ceil

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 1. Dicionário de Sinônimos Base (✅ Sincronizado com app.py)
SINONIMOS_FARMACOS = {
    "GLICOSE": {"GLICOSE INJETAVEL", "GLICOSE INJETÁVEL", "DEXTROSE"},
    "AMINOACIDO": {"AMINOÁCIDO", "AMINOACIDOS", "AMINOÁCIDOS"},
    "HEMODIALISE": {"HEMODIÁLISE", "DIALISE", "DIÁLISE", "TERAPIA RENAL"},
    "FOSFORO": {"FOSFORICO", "FOSFÓRICO", "FO"},
    "VITAMINA": {"VITAMINA C", "ACIDO ASCORBICO", "ÁCIDO ASCÓRBICO", "VITAMINA"},
    "AAS": {"AAS", "ACIDO ACETILSALICILICO", "ÁCIDO ACETILSALICÍLICO"},
    "MATERIAL_HOSPITALAR": {"MATERIAL MÉDICO-HOSPITALAR", "MÉDICO-HOSPITALAR"}
}

# 2. Escudo de Exclusão (Stopwords Farmacêuticos)
# Essas palavras NUNCA serão consideradas como termo de busca principal
TERMOS_GENERICOS = {
    "FRASCO", "FRASCOS", "AMPOLA", "AMPOLAS", "BOLSA", "BOLSAS", "CAIXA", "CX", 
    "COMPRIMIDO", "COMPRIMIDOS", "CPR", "CAPSULA", "CAPSULAS", "INJETAVEL", 
    "INJ", "GOTAS", "XAROPE", "SOLUCAO", "MG", "ML", "KG", "LITRO", "LITROS", 
    "UNIDADE", "UN", "KIT", "KITS", "TIPO", "PARA", "COM", "SEM", "MEDICAMENTO"
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
                        
                        # ✅ CORREÇÃO: Escalonamento Inteligente (Pula os Stopwords)
                        palavras_cruas = desc_completa.replace(',', ' ').replace('.', ' ').split()
                        
                        for palavra in palavras_cruas:
                            # Limpa caracteres especiais (deixa só letras)
                            palavra_limpa = re.sub(r'[^A-Z]', '', palavra)
                            
                            # Se a palavra for válida e NÃO for uma embalagem/medida genérica
                            if len(palavra_limpa) > 2 and palavra_limpa not in TERMOS_GENERICOS:
                                TERMOS_BUSCA.add(palavra_limpa)
                                break # Achou o Princípio Ativo, pode pular pro próximo item do Excel
                                
            if portfolio: break
        except Exception as e:
            continue
            
    return portfolio

def extrair_componentes(descricao):
    if not descricao: return []
    descricao_limpa = str(descricao).replace(',', ' ').replace('(', ' ').replace(')', ' ').replace('.', ' ')
    return [palavra.strip().upper() for palavra in descricao_limpa.split() if palavra.strip()]

def processar_lote(lote_licitacoes, arquivo_saida_csv):
    resultados_lote = []
    
    for licitacao in lote_licitacoes:
        itens = licitacao.get('itens', [])
        for item in itens:
            descricao = str(item.get('d', '')).upper()
            
            if not descricao or descricao == 'NONE': continue
                
            componentes = extrair_componentes(descricao)
            componentes_set = set(componentes)
            
            # Cruzamento exato de palavras (O(1))
            match = componentes_set.intersection(TERMOS_BUSCA)
            
            # Cruzamento abrangente (para termos compostos)
            if not match:
                for termo in TERMOS_BUSCA:
                    if " " in termo and termo in descricao:
                        match.add(termo)
            
            if match:
                resultado = {
                    'id_licitacao': licitacao.get('id', ''),
                    'orgao': licitacao.get('org', ''),
                    'item_num': item.get('n', ''),
                    'descricao_item': item.get('d', ''),
                    'termo_encontrado': " | ".join(list(match))
                }
                resultados_lote.append(resultado)
                
    if resultados_lote:
        # Aqui pode continuar sendo 'a' (append) porque estamos escrevendo lote por lote
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
            
    return len(resultados_lote)

def main():
    ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
    ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
    TAMANHO_LOTE = 500 

    logging.info("🚀 A iniciar a Avaliação de Portfólio (Modo Inteligente Escalonado)...")
    
    portfolio = carregar_portfolio()
    logging.info(f"📦 Portfólio base carregado com {len(portfolio)} itens.")
    logging.info(f"🧠 Motor de pesquisa configurado com {len(TERMOS_BUSCA)} palavras-chave ativas.")

    # ✅ CORREÇÃO 1: Prepara o arquivo recriando-o do ZERO (mode='w') a cada execução global
    with open(ARQUIVO_SAIDA, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
        writer.writeheader()

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
        encontrados = processar_lote(lote, ARQUIVO_SAIDA)
        total_encontrados += encontrados
        
        del lote
        gc.collect() 
        
    logging.info(f"✅ Avaliação concluída! Total de itens estritamente compatíveis encontrados: {total_encontrados}.")

if __name__ == '__main__':
    main()
