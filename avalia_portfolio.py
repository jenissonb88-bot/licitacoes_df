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

# 1. Dicionário de Sinónimos Otimizado (Usando SETS para pesquisa O(1))
SINONIMOS_FARMACOS = {
    "GLICOSE": {"GLICOSE INJETAVEL", "GLICOSE INJETÁVEL", "DEXTROSE"},
    "AMINOACIDO": {"AMINOÁCIDO", "AMINOACIDOS", "AMINOÁCIDOS"},
    "HEMODIALISE": {"HEMODIÁLISE", "DIALISE", "DIÁLISE", "TERAPIA RENAL"},
    "FOSFORO": {"FOSFORICO", "FOSFÓRICO", "FO"}, # Adicionado para tratar a falha crítica do log
    "MATERIAL_HOSPITALAR": {"MATERIAL MÉDICO-HOSPITALAR", "MÉDICO-HOSPITALAR"},
    "VITAMINA_C": {"VITAMINA C", "ACIDO ASCORBICO", "ÁCIDO ASCÓRBICO"},
    "AAS": {"AAS", "ACIDO ACETILSALICILICO", "ÁCIDO ACETILSALICÍLICO"}
    # Pode expandir este dicionário com mais itens do seu portfólio
}

# 2. Achar todos os termos de pesquisa num único Set para verificação super rápida
TERMOS_BUSCA = set(SINONIMOS_FARMACOS.keys())
for sinonimos in SINONIMOS_FARMACOS.values():
    TERMOS_BUSCA.update(sinonimos)

def carregar_portfolio():
    """
    Carrega o portfólio base a partir do ficheiro CSV 'Exportar Dados.csv'.
    """
    portfolio = []
    arquivo_csv = 'Exportar Dados.csv'
    
    if not os.path.exists(arquivo_csv):
        logging.warning(f"⚠️ Ficheiro {arquivo_csv} não encontrado. A usar apenas os termos estáticos.")
        return portfolio

    try:
        with open(arquivo_csv, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Adapte o nome da coluna caso seja diferente no seu CSV ('Descrição' ou 'Descricao')
                descricao = row.get('Descrição', row.get('Descricao', ''))
                if descricao:
                    portfolio.append(descricao.strip())
    except Exception as e:
        logging.error(f"Erro ao ler o portfólio: {e}")
        
    return portfolio

def extrair_componentes(descricao):
    """Divide a descrição em palavras/componentes normalizados."""
    if not descricao:
        return []
    # Substitui caracteres especiais comuns e divide
    descricao_limpa = str(descricao).replace(',', ' ').replace('(', ' ').replace(')', ' ').replace('.', ' ')
    return [palavra.strip().upper() for palavra in descricao_limpa.split() if palavra.strip()]

def processar_lote(lote_licitacoes, arquivo_saida_csv):
    """Processa um lote específico e anexa os resultados no ficheiro CSV."""
    resultados_lote = []
    
    for licitacao in lote_licitacoes:
        itens = licitacao.get('itens', [])
        
        for item in itens:
            descricao = str(item.get('descricao', '')).upper()
            componentes = extrair_componentes(descricao)
            
            # Interseção matemática eficiente (Sets)
            componentes_set = set(componentes)
            match = componentes_set.intersection(TERMOS_BUSCA)
            
            # Verificação adicional para descrições completas (ex: "TERAPIA RENAL")
            if not match:
                for termo in TERMOS_BUSCA:
                    if termo in descricao:
                        match.add(termo)
            
            if match:
                # Estruturação do registo encontrado
                resultado = {
                    'id_licitacao': licitacao.get('id', ''),
                    'orgao': licitacao.get('orgao', ''),
                    'item_num': item.get('numero', ''),
                    'descricao_item': item.get('descricao', ''),
                    'termo_encontrado': " | ".join(list(match))
                }
                resultados_lote.append(resultado)
                
    # Guarda o lote no CSV (Modo Append)
    if resultados_lote:
        with open(arquivo_saida_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])
            writer.writerows(resultados_lote)
            
    return len(resultados_lote)

def main():
    ARQUIVO_ENTRADA = 'pregacoes_pharma_limpos.json.gz'
    ARQUIVO_SAIDA = 'relatorio_compatibilidade_consolidado.csv'
    TAMANHO_LOTE = 500 # Previne Memory Exhaustion no GitHub Actions

    logging.info("🚀 A iniciar a Avaliação de Portfólio com Gestão de Memória...")
    
    portfolio = carregar_portfolio()
    logging.info(f"📦 Portfólio base carregado com {len(portfolio)} itens.")

    # Verifica se o ficheiro consolidado já existe
    arquivo_existe = os.path.isfile(ARQUIVO_SAIDA)
    
    # Prepara o ficheiro CSV e escreve o cabeçalho apenas se for novo
    with open(ARQUIVO_SAIDA, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not arquivo_existe:
            writer.writerow(['id_licitacao', 'orgao', 'item_num', 'descricao_item', 'termo_encontrado'])

    if not os.path.exists(ARQUIVO_ENTRADA):
        logging.error(f"❌ Ficheiro {ARQUIVO_ENTRADA} não encontrado. Interrompendo.")
        return

    # Carrega as licitações para a memória a partir do GZIP
    try:
        with gzip.open(ARQUIVO_ENTRADA, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)
    except Exception as e:
        logging.error(f"❌ Erro ao abrir {ARQUIVO_ENTRADA}: {e}")
        return
    
    total_licitacoes = len(licitacoes)
    total_lotes = ceil(total_licitacoes / TAMANHO_LOTE)
    
    logging.info(f"📊 Total de licitações para analisar: {total_licitacoes} | Serão processadas em {total_lotes} lotes.")

    total_encontrados = 0

    # Processamento fracionado (Batch Processing)
    for i in range(0, total_licitacoes, TAMANHO_LOTE):
        lote = licitacoes[i:i + TAMANHO_LOTE]
        num_lote = (i // TAMANHO_LOTE) + 1
        
        logging.info(f"🔄 A processar lote {num_lote}/{total_lotes} (Tamanho do lote: {len(lote)})...")
        
        encontrados = processar_lote(lote, ARQUIVO_SAIDA)
        total_encontrados += encontrados
        
        # Limpeza agressiva da memória RAM
        del lote
        gc.collect() 
        
    logging.info(f"✅ Avaliação concluída com sucesso! Total de itens compatíveis encontrados: {total_encontrados}.")
    logging.info(f"📁 Relatório atualizado e consolidado em: {ARQUIVO_SAIDA}")

if __name__ == '__main__':
    main()
