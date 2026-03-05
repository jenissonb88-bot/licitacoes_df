import csv
import json
import gzip
import re
import os
import sys
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# CONFIGURAÇÕES - THRESHOLDS AJUSTADOS
# ============================================================================

# NOVOS Thresholds de percentual para classificação
THRESHOLD_ALTA = 60.0       # ≥60% = ALTA (Verde)
THRESHOLD_MEDIA = 20.0      # 20-59% = MÉDIA (Amarelo)
THRESHOLD_BAIXA = 5.0       # 5-19% = BAIXA (Vermelho)
# <5% = INCOMPATÍVEL (Transparente)

# Blacklist de termos genéricos que NÃO devem gerar match
TERMOS_GENERICOS = {
    # Unidades e medidas
    'MG', 'ML', 'G', 'KG', 'MCG', 'UI', 'UN', 'UNIDADE', 'UNIDADES',
    # Formas farmacêuticas
    'COMPRIMIDO', 'COMPRIMIDOS', 'CAPSULA', 'CAPSULAS', 'DRAGEA', 'DRAGEAS',
    'SOLUCAO', 'SOLUÇÃO', 'SOLUCOES', 'SOLUÇÕES', 'SUSPENSAO', 'SUSPENSÃO',
    'XAROPE', 'XAROPES', 'INJETAVEL', 'INJETÁVEL', 'INJETAVEIS', 'INJETÁVEIS',
    'AMPOLA', 'AMPOLAS', 'FRASCO', 'FRASCOS', 'BISNAGA', 'BISNAGAS',
    'TUBO', 'TUBOS', 'POTE', 'POTES', 'CAIXA', 'CAIXAS', 'EMBALAGEM', 'EMBALAGENS',
    # Vias de administração
    'ORAL', 'INTRAVENOSO', 'IV', 'IM', 'INTRAMUSCULAR', 'TOPICO', 'TÓPICO',
    'RETAL', 'VAGINAL', 'NASAL', 'OFTALMICO', 'OFTÁLMICO', 'OTICO', 'ÓTICO',
    # Quantidades genéricas
    'UND', 'CP', 'CAP', 'AMP', 'FR', 'CX', 'TB', 'COMPR', 'CAPS',
    # Outros termos genéricos
    'GENERICO', 'GENÉRICO', 'SIMILAR', 'REFERENCIA', 'REFERÊNCIA', 'ETICO', 'ÉTICO',
    'APRESENTACAO', 'APRESENTAÇÃO', 'CONCENTRACAO', 'CONCENTRAÇÃO', 'DOSAGEM',
    'QUANTIDADE', 'QTD', 'TOTAL', 'PARCIAL', 'ITEM', 'ITENS',
    'MEDICAMENTO', 'MEDICAMENTOS', 'FARMACO', 'FÁRMACO', 'PRODUTO', 'PRODUTOS',
    'MATERIAL', 'MATERIAIS', 'INSUMO', 'INSUMOS', 'HOSPITALAR', 'HOSPITALARES'
}

# Peso por especificidade do termo
PESO_MINIMO_CARACTERES = 4  # Termos com menos caracteres são ignorados

# Número de workers para paralelização
MAX_WORKERS = 10

# ============================================================================
# FUNÇÕES UTILITÁRIAS
# ============================================================================

def log(msg, nivel="INFO"):
    """Log com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{nivel}] {msg}")

def normalizar_texto(texto):
    """Normaliza texto para comparação"""
    if not isinstance(texto, str):
        return ""
    # Remove acentos e converte para maiúsculo
    texto = texto.upper()
    texto = re.sub(r'[ÁÀÂÃÄ]', 'A', texto)
    texto = re.sub(r'[ÉÈÊË]', 'E', texto)
    texto = re.sub(r'[ÍÌÎÏ]', 'I', texto)
    texto = re.sub(r'[ÓÒÔÕÖ]', 'O', texto)
    texto = re.sub(r'[ÚÙÛÜ]', 'U', texto)
    texto = re.sub(r'[Ç]', 'C', texto)
    # Remove caracteres especiais, mantém apenas letras, números e espaços
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)
    # Remove espaços múltiplos
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def extrair_tokens_rigorosos(texto):
    """
    Extrai tokens de forma rigorosa, excluindo termos genéricos e curtos.
    Retorna apenas tokens significativos (nomes de medicamentos/produtos).
    """
    if not texto:
        return set()
    
    texto_normalizado = normalizar_texto(texto)
    tokens = set()
    
    # Divide em tokens
    palavras = texto_normalizado.split()
    
    # Adiciona tokens individuais (se não forem genéricos e tiverem tamanho mínimo)
    for palavra in palavras:
        if len(palavra) >= PESO_MINIMO_CARACTERES and palavra not in TERMOS_GENERICOS:
            tokens.add(palavra)
    
    # Adiciona bigramas (2 palavras consecutivas) para maior especificidade
    for i in range(len(palavras) - 1):
        bigrama = f"{palavras[i]} {palavras[i+1]}"
        # Só adiciona se nenhuma das palavras for genérica
        if palavras[i] not in TERMOS_GENERICOS and palavras[i+1] not in TERMOS_GENERICOS:
            if len(bigrama) >= PESO_MINIMO_CARACTERES * 2:
                tokens.add(bigrama)
    
    # Adiciona trigramas (3 palavras) para máxima especificidade
    for i in range(len(palavras) - 2):
        trigrama = f"{palavras[i]} {palavras[i+1]} {palavras[i+2]}"
        if (palavras[i] not in TERMOS_GENERICOS and 
            palavras[i+1] not in TERMOS_GENERICOS and 
            palavras[i+2] not in TERMOS_GENERICOS):
            if len(trigrama) >= PESO_MINIMO_CARACTERES * 3:
                tokens.add(trigrama)
    
    return tokens

def calcular_compatibilidade_percentual(tokens_licitacao, tokens_portfolio):
    """
    Calcula compatibilidade como percentual de itens da licitação presentes no portfólio.
    
    Fórmula: (tokens_licitacao ∩ tokens_portfolio) / tokens_licitacao × 100
    
    Retorna: (percentual, matches_encontrados)
    """
    if not tokens_licitacao:
        return 0.0, []
    
    # Interseção: tokens que existem em ambos
    matches = tokens_licitacao.intersection(tokens_portfolio)
    
    # Cálculo de percentual
    percentual = (len(matches) / len(tokens_licitacao)) * 100
    
    # Ordena matches por tamanho (mais específicos primeiro)
    matches_ordenados = sorted(matches, key=lambda x: len(x), reverse=True)
    
    return percentual, matches_ordenados

def classificar_compatibilidade(percentual):
    """Classifica o nível de compatibilidade baseado nos NOVOS thresholds"""
    if percentual >= THRESHOLD_ALTA:
        return "ALTA"           # ≥60% = Verde
    elif percentual >= THRESHOLD_MEDIA:
        return "MEDIA"          # 20-59% = Amarelo
    elif percentual >= THRESHOLD_BAIXA:
        return "BAIXA"          # 5-19% = Vermelho
    else:
        return "INCOMPATIVEL"   # <5% = Transparente

# ============================================================================
# CARREGAMENTO DE DADOS
# ============================================================================

def carregar_portfolio(csv_path="Exportar Dados.csv"):
    """Carrega portfólio de produtos do CSV usando biblioteca padrão csv"""
    log(f"Carregando portfólio de {csv_path}...")
    
    if not os.path.exists(csv_path):
        log(f"ERRO: Arquivo {csv_path} não encontrado!", "ERRO")
        return None, None
    
    try:
        # Detecta encoding e lê CSV
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        rows = []
        headers = []
        
        for encoding in encodings:
            try:
                with open(csv_path, 'r', encoding=encoding) as f:
                    # Detecta delimitador
                    sample = f.read(2048)
                    f.seek(0)
                    
                    delimiter = ';' if sample.count(';') > sample.count(',') else ','
                    
                    reader = csv.reader(f, delimiter=delimiter)
                    headers = next(reader)
                    rows = list(reader)
                    break
            except Exception:
                continue
        
        if not rows:
            log("ERRO: Não foi possível ler o CSV", "ERRO")
            return None, None
        
        # Detecta coluna de descrição
        col_descricao = None
        for i, col in enumerate(headers):
            col_upper = col.upper()
            if any(term in col_upper for term in ['PRODUTO', 'DESCRICAO', 'DESCRIÇÃO', 'MEDICAMENTO', 'ITEM', 'NOME']):
                col_descricao = i
                break
        
        if col_descricao is None:
            col_descricao = 0
            log(f"Coluna de descrição não identificada, usando primeira coluna: {headers[0]}", "AVISO")
        
        # Extrai tokens de cada produto do portfólio
        tokens_portfolio = set()
        produtos_processados = []
        
        for row in rows:
            if len(row) > col_descricao:
                descricao = row[col_descricao] if row[col_descricao] else ""
                if descricao and descricao.lower() != 'nan':
                    tokens_produto = extrair_tokens_rigorosos(descricao)
                    tokens_portfolio.update(tokens_produto)
                    produtos_processados.append({
                        'descricao_original': descricao,
                        'tokens': tokens_produto
                    })
        
        log(f"✅ Portfólio carregado: {len(rows)} produtos")
        log(f"📊 Tokens únicos no portfólio: {len(tokens_portfolio)}")
        
        # Mostra alguns exemplos de tokens extraídos
        exemplos = sorted(list(tokens_portfolio), key=len, reverse=True)[:10]
        log(f"🔍 Exemplos de tokens: {exemplos}")
        
        return tokens_portfolio, produtos_processados
        
    except Exception as e:
        log(f"ERRO ao carregar portfólio: {e}", "ERRO")
        return None, None

def carregar_licitacoes(json_path="pregacoes_pharma_limpos.json.gz"):
    """Carrega licitações do arquivo JSON comprimido MANTENDO ORDENAÇÃO ORIGINAL"""
    log(f"Carregando licitações de {json_path}...")
    
    if not os.path.exists(json_path):
        log(f"ERRO: Arquivo {json_path} não encontrado!", "ERRO")
        return None
    
    try:
        with gzip.open(json_path, 'rt', encoding='utf-8') as f:
            dados = json.load(f)
        
        # Converte para lista se for dicionário MANTENDO A ORDEM ORIGINAL
        licitacoes = []
        if isinstance(dados, dict):
            for orgao, editais in dados.items():
                for edital, info in editais.items():
                    info['orgao'] = orgao
                    info['edital'] = edital
                    # Preserva índice original para manter ordenação
                    info['_index_original'] = len(licitacoes)
                    licitacoes.append(info)
        else:
            for idx, item in enumerate(dados):
                item['_index_original'] = idx
                licitacoes.append(item)
        
        log(f"✅ {len(licitacoes)} licitações carregadas (ordem original preservada)")
        return licitacoes
        
    except Exception as e:
        log(f"ERRO ao carregar licitações: {e}", "ERRO")
        return None

# ============================================================================
# AVALIAÇÃO DE LICITAÇÕES
# ============================================================================

def avaliar_licitacao(licitacao, tokens_portfolio):
    """Avalia uma única licitação contra o portfólio"""
    try:
        # Extrai texto do objeto da licitação
        objeto = str(licitacao.get('objeto', ''))
        
        # Extrai itens se existirem
        itens_texto = []
        itens = licitacao.get('itens', [])
        if isinstance(itens, list):
            for item in itens:
                if isinstance(item, dict):
                    desc_item = item.get('descricao', '') or item.get('nome', '') or str(item)
                    itens_texto.append(desc_item)
                else:
                    itens_texto.append(str(item))
        
        # Combina objeto + itens
        texto_completo = objeto + " " + " ".join(itens_texto)
        
        # Extrai tokens da licitação
        tokens_licitacao = extrair_tokens_rigorosos(texto_completo)
        
        if not tokens_licitacao:
            return {
                'id': licitacao.get('edital', 'unknown'),
                'percentual': 0.0,
                'confianca': 'INCOMPATIVEL',
                'matches': [],
                'total_tokens_licitacao': 0,
                'total_matches': 0,
                '_index_original': licitacao.get('_index_original', 0)
            }
        
        # Calcula compatibilidade percentual
        percentual, matches = calcular_compatibilidade_percentual(tokens_licitacao, tokens_portfolio)
        
        # Classifica com NOVOS thresholds
        confianca = classificar_compatibilidade(percentual)
        
        return {
            'id': licitacao.get('edital', licitacao.get('id', 'unknown')),
            'orgao': licitacao.get('orgao', 'N/A'),
            'objeto': objeto[:200],
            'percentual': round(percentual, 2),
            'confianca': confianca,
            'matches': matches[:10],
            'total_tokens_licitacao': len(tokens_licitacao),
            'total_matches': len(matches),
            '_index_original': licitacao.get('_index_original', 0)  # PRESERVA ÍNDICE
        }
        
    except Exception as e:
        return {
            'id': licitacao.get('edital', 'unknown'),
            'percentual': 0.0,
            'confianca': 'ERRO',
            'matches': [],
            'erro': str(e),
            '_index_original': licitacao.get('_index_original', 0)
        }

def avaliar_todas_licitacoes(licitacoes, tokens_portfolio):
    """Avalia todas as licitações em paralelo MANTENDO ORDENAÇÃO"""
    log(f"🔍 Avaliando compatibilidade de {len(licitacoes)} licitações...")
    
    resultados = []
    processadas = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(avaliar_licitacao, lic, tokens_portfolio): lic 
            for lic in licitacoes
        }
        
        for future in as_completed(futures):
            resultado = future.result()
            resultados.append(resultado)
            processadas += 1
            
            if processadas % 100 == 0:
                log(f"Processadas {processadas}/{len(licitacoes)}...")
    
    # ORDENA PELO ÍNDICE ORIGINAL (mantém ordem das licitações)
    resultados.sort(key=lambda x: x['_index_original'])
    
    return resultados

# ============================================================================
# GERAÇÃO DE RELATÓRIO
# ============================================================================

def gerar_relatorio(resultados, origem="MANUAL"):
    """Gera relatório CSV com resultados MANTENDO ORDEM ORIGINAL"""
    arquivo_saida = "relatorio_compatibilidade.csv"
    
    # Prepara dados para CSV (mantém ordem original dos pregões)
    with open(arquivo_saida, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        
        # Escreve header
        writer.writerow(['id', 'orgao', 'objeto_licitacao', 'percentual', 'confianca', 
                        'total_tokens', 'total_matches', 'principais_matches'])
        
        # Escreve dados
        for r in resultados:
            matches_str = '|'.join(r.get('matches', []))[:500]
            writer.writerow([
                r['id'],
                r.get('orgao', ''),
                r.get('objeto', ''),
                r['percentual'],
                r['confianca'],
                r.get('total_tokens_licitacao', 0),
                r.get('total_matches', 0),
                matches_str
            ])
    
    # Estatísticas com NOVOS thresholds
    total = len(resultados)
    alta = len([r for r in resultados if r['confianca'] == 'ALTA'])
    media = len([r for r in resultados if r['confianca'] == 'MEDIA'])
    baixa = len([r for r in resultados if r['confianca'] == 'BAIXA'])
    incomp = len([r for r in resultados if r['confianca'] == 'INCOMPATIVEL'])
    
    log("="*60)
    log("📊 RELATÓRIO GERADO (ORDEM ORIGINAL MANTIDA)")
    log("📊 NOVOS THRESHOLDS: ≥60% Verde | 20-59% Amarelo | 5-19% Vermelho | <5% Transparente")
    log("="*60)
    log(f"Arquivo: {arquivo_saida}")
    log(f"Origem: {origem}")
    log(f"Total: {total} | 🟢 ALTA (≥60%): {alta} | 🟡 MÉDIA (20-59%): {media} | 🔴 BAIXA (5-19%): {baixa} | ⚪ INCOMPATÍVEL (<5%): {incomp}")
    
    return arquivo_saida

def salvar_cache(dados, origem):
    """Salva cache para evitar reprocessamento"""
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'origem': origem,
        'total_licitacoes': len(dados)
    }
    with open('cache_avaliacao.json', 'w', encoding='utf-8') as f:
        json.dump(cache_data, f)

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Função principal"""
    # Detecta origem (SYNC ou AUDITOR)
    origem = "MANUAL"
    if len(sys.argv) > 1:
        origem = sys.argv[1].upper()
    
    log("="*60)
    log(f"🔍 AVALIAÇÃO DE PORTFÓLIO v3.1 [SEM DEPENDÊNCIAS EXTERNAS] [{origem}]")
    log(f"📊 REGRAS: ≥60% Verde | 20-59% Amarelo | 5-19% Vermelho | <5% Transparente")
    log("="*60)
    
    # Carrega portfólio
    tokens_portfolio, produtos = carregar_portfolio()
    if not tokens_portfolio:
        log("❌ Falha ao carregar portfólio. Abortando.", "ERRO")
        return 1
    
    # Carrega licitações
    licitacoes = carregar_licitacoes()
    if not licitacoes:
        log("❌ Falha ao carregar licitações. Abortando.", "ERRO")
        return 1
    
    # Avalia
    resultados = avaliar_todas_licitacoes(licitacoes, tokens_portfolio)
    
    # Gera relatório
    gerar_relatorio(resultados, origem)
    
    # Salva cache
    salvar_cache(licitacoes, origem)
    
    log(f"✅ Avaliação [{origem}] concluída! Ordem original dos pregões mantida.")
    return 0

if __name__ == "__main__":
    exit(main())
