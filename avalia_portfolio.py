import json
import gzip
import csv
import os
import unicodedata
import re
from datetime import datetime
import concurrent.futures

# --- CONFIGURAÇÕES ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz'
ARQ_PORTFOLIO = 'Exportar Dados.csv'
ARQ_RELATORIO = 'relatorio_compatibilidade.csv'
ARQ_LOG = 'log_avaliacao.txt'
MAXWORKERS = 10

# Thresholds de confiança
THRESHOLD_ALTA = 70
THRESHOLD_MEDIA = 40
THRESHOLD_BAIXA = 15

def normalize(t):
    """Normaliza texto para comparação: remove acentos, upper case, remove espaços extras"""
    if not t:
        return ""
    # Remove acentos e normaliza
    text = ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() 
                   if unicodedata.category(c) != 'Mn')
    # Remove espaços múltiplos e trim
    text = ' '.join(text.split())
    return text

def extrair_termos_produto(descricao_produto):
    """Extrai termos relevantes de uma descrição de produto do portfólio"""
    desc_norm = normalize(descricao_produto)

    # Remove palavras comuns/genéricas
    palavras_stop = ['COM', 'DE', 'DA', 'DO', 'DAS', 'DOS', 'PARA', 'POR', 'EM', 'NO', 'NA', 
                     'MG', 'ML', 'G', 'KG', 'UNIDADE', 'UND', 'CAPSULA', 'COMPRIMIDO',
                     'INJETAVEL', 'SOLUCAO', 'SUSPENSAO', 'XAROPE', 'CREME', 'POMADA',
                     'GENERICO', 'REFERENCIA', 'SIMILAR', 'APRESENTACAO', 'FRASCO',
                     'CAIXA', 'BLISTER', 'CARTELA', 'CP', 'CAP', 'AMPOLA', 'AMP',
                     'FA', 'CPR', 'COMPR', 'COMP', 'TAB', 'TABLETE']

    # Extrai palavras significativas (tamanho > 3 e não é stop word)
    palavras = [p for p in desc_norm.split() if len(p) > 3 and p not in palavras_stop]

    # Também extrai o nome base do medicamento (primeiras palavras significativas)
    termos_principais = []
    for palavra in palavras[:3]:  # Primeiras 3 palavras significativas
        if len(palavra) > 4:
            termos_principais.append(palavra)

    return {
        'termos_principais': termos_principais,
        'todas_palavras': palavras,
        'descricao_original': descricao_produto,
        'descricao_normalizada': desc_norm
    }

def carregar_portfolio():
    """Carrega e processa o portfólio de produtos"""
    portfolio = {
        'medicamentos': {},  # Por categoria/família
        'materiais': {},
        'todos_termos': set(),
        'total_produtos': 0
    }

    if not os.path.exists(ARQ_PORTFOLIO):
        print(f"⚠️ Arquivo {ARQ_PORTFOLIO} não encontrado!")
        return portfolio

    print(f"📚 Carregando portfólio de {ARQ_PORTFOLIO}...")

    try:
        with open(ARQ_PORTFOLIO, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')

            # Detecta header
            header = next(reader, None)
            if not header:
                # Tenta com outro encoding
                with open(ARQ_PORTFOLIO, 'r', encoding='latin-1') as f2:
                    reader = csv.reader(f2, delimiter=';')
                    header = next(reader, None)

            # Mapeia colunas
            col_descricao = None
            col_categoria = None
            col_codigo = None

            for i, col in enumerate(header):
                col_upper = col.upper()
                if any(x in col_upper for x in ['PRODUTO', 'DESCRI', 'MEDICAMENT', 'ITEM']):
                    col_descricao = i
                if any(x in col_upper for x in ['CATEGORIA', 'FAMILIA', 'GRUPO', 'CLASSE']):
                    col_categoria = i
                if any(x in col_upper for x in ['CODIGO', 'SKU', 'EAN', 'REFERENCIA']):
                    col_codigo = i

            # Se não achou, assume colunas 0 e 1
            if col_descricao is None:
                col_descricao = 0
            if col_categoria is None:
                col_categoria = 1 if len(header) > 1 else 0

            for row in reader:
                if len(row) <= max(col_descricao, col_categoria):
                    continue

                descricao = row[col_descricao].strip()
                categoria = row[col_categoria].strip() if col_categoria < len(row) else "GERAL"
                codigo = row[col_codigo].strip() if col_codigo and col_codigo < len(row) else ""

                if not descricao:
                    continue

                portfolio['total_produtos'] += 1

                # Extrai termos do produto
                termos_produto = extrair_termos_produto(descricao)

                # Adiciona aos termos globais
                portfolio['todos_termos'].update(termos_produto['todas_palavras'])

                # Organiza por categoria
                cat_norm = normalize(categoria)
                if 'MATERIAL' in cat_norm or 'INSUMO' in cat_norm or 'MMH' in cat_norm:
                    categoria_tipo = 'materiais'
                else:
                    categoria_tipo = 'medicamentos'

                if categoria not in portfolio[categoria_tipo]:
                    portfolio[categoria_tipo][categoria] = []

                portfolio[categoria_tipo][categoria].append({
                    'descricao': descricao,
                    'codigo': codigo,
                    'termos': termos_produto
                })

        print(f"✅ Portfólio carregado: {portfolio['total_produtos']} produtos")
        print(f"   💊 Medicamentos: {sum(len(v) for v in portfolio['medicamentos'].values())}")
        print(f"   🏥 Materiais: {sum(len(v) for v in portfolio['materiais'].values())}")
        print(f"   📖 Termos únicos: {len(portfolio['todos_termos'])}")

    except Exception as e:
        print(f"❌ Erro ao carregar portfólio: {e}")

    return portfolio

def calcular_score_compatibilidade(texto_licitacao, itens_licitacao, portfolio):
    """Calcula score de compatibilidade entre licitação e portfólio"""

    texto_norm = normalize(texto_licitacao)

    scores = {
        'objeto': 0,
        'itens': [],
        'categorias_match': set(),
        'produtos_match': [],
        'termos_encontrados': set()
    }

    # --- AVALIAÇÃO DO OBJETO ---
    # Verifica termos do portfólio no objeto da licitação
    matches_objeto = []
    for termo in portfolio['todos_termos']:
        if len(termo) > 4 and termo in texto_norm:
            # Verifica se é palavra completa (evita substring falsa)
            pattern = r'\b' + re.escape(termo) + r'\b'
            if re.search(pattern, texto_norm):
                matches_objeto.append(termo)
                scores['termos_encontrados'].add(termo)

    # Score baseado na quantidade e qualidade dos matches
    if matches_objeto:
        # Deduplica matches similares
        matches_unicos = []
        for match in sorted(matches_objeto, key=len, reverse=True):
            if not any(match in m and match != m for m in matches_unicos):
                matches_unicos.append(match)

        # Score: matches longos valem mais (mais específicos)
        score_obj = sum(min(len(m), 15) for m in matches_unicos[:10])  # Cap nos top 10
        scores['objeto'] = min(score_obj * 2, 100)  # Multiplicador 2x, max 100

    # --- AVALIAÇÃO DOS ITENS ---
    melhor_score_item = 0

    for item in itens_licitacao:
        desc_item = normalize(item.get('d', ''))
        if not desc_item:
            continue

        score_item = 0
        matches_item = []

        for termo in portfolio['todos_termos']:
            if len(termo) > 4 and termo in desc_item:
                pattern = r'\b' + re.escape(termo) + r'\b'
                if re.search(pattern, desc_item):
                    matches_item.append(termo)
                    scores['termos_encontrados'].add(termo)

        if matches_item:
            matches_unicos_item = []
            for match in sorted(matches_item, key=len, reverse=True):
                if not any(match in m and match != m for m in matches_unicos_item):
                    matches_unicos_item.append(match)

            score_item = sum(min(len(m), 12) for m in matches_unicos_item[:5])
            score_item = min(score_item * 2.5, 100)  # Itens têm peso maior

            if score_item > melhor_score_item:
                melhor_score_item = score_item

            scores['itens'].append({
                'numero': item.get('n'),
                'descricao': item.get('d', '')[:60],
                'score': round(score_item, 1),
                'matches': matches_unicos_item[:5]
            })

    # --- SCORE FINAL ---
    # Fórmula: 30% objeto + 70% melhor item (itens são mais específicos)
    score_final = (scores['objeto'] * 0.3) + (melhor_score_item * 0.7)

    # Bônus para múltiplos itens compatíveis (volume de negócio)
    itens_altos = [i for i in scores['itens'] if i['score'] > THRESHOLD_MEDIA]
    if len(itens_altos) > 1:
        bonus = min(len(itens_altos) * 3, 15)  # Até 15 pontos de bônus
        score_final = min(score_final + bonus, 100)

    scores['final'] = round(score_final, 1)

    # Determina confiança
    if scores['final'] >= THRESHOLD_ALTA:
        scores['confianca'] = 'ALTA'
    elif scores['final'] >= THRESHOLD_MEDIA:
        scores['confianca'] = 'MEDIA'
    elif scores['final'] >= THRESHOLD_BAIXA:
        scores['confianca'] = 'BAIXA'
    else:
        scores['confianca'] = 'INCOMPATIVEL'

    return scores

def processar_licitacao_avaliacao(lic, portfolio):
    """Processa uma licitação e retorna avaliação de compatibilidade"""
    try:
        id_lic = lic.get('id', 'N/A')
        edital = lic.get('edit', 'N/A')
        orgao = lic.get('org', 'N/A')
        uf = lic.get('uf', '')
        cidade = lic.get('cid', '')
        objeto = lic.get('obj', '')
        itens = lic.get('itens', [])
        link = lic.get('link', '')
        val_tot = lic.get('val_tot', 0)
        dt_enc = lic.get('dt_enc', '')

        # Calcula score
        scores = calcular_score_compatibilidade(objeto, itens, portfolio)

        # Só retorna se tiver alguma compatibilidade
        if scores['confianca'] == 'INCOMPATIVEL':
            return None

        # Prepara resultado
        resultado = {
            'data_avaliacao': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'id': id_lic,
            'edital': edital,
            'orgao': orgao[:60],
            'uf': uf,
            'cidade': cidade[:30],
            'score_final': scores['final'],
            'confianca': scores['confianca'],
            'score_objeto': scores['objeto'],
            'melhor_score_item': max([i['score'] for i in scores['itens']]) if scores['itens'] else 0,
            'qtd_itens_analisados': len(itens),
            'qtd_itens_compativeis': len([i for i in scores['itens'] if i['score'] > THRESHOLD_MEDIA]),
            'valor_total_estimado': val_tot,
            'data_encerramento': dt_enc[:10] if dt_enc else '',
            'objeto_resumo': objeto[:100],
            'principais_matches': '|'.join(list(scores['termos_encontrados'])[:8]),
            'link': link
        }

        return resultado

    except Exception as e:
        return {'erro': str(e), 'id': lic.get('id', 'N/A')}

def gerar_relatorio(resultados):
    """Gera relatório CSV ordenado por score"""
    if not resultados:
        print("ℹ️ Nenhuma licitação compatível encontrada.")
        return

    # Ordena por score final (maior primeiro)
    resultados_ordenados = sorted(resultados, key=lambda x: x.get('score_final', 0), reverse=True)

    # Salva CSV
    with open(ARQ_RELATORIO, 'w', newline='', encoding='utf-8-sig') as f:
        if resultados_ordenados:
            writer = csv.DictWriter(f, fieldnames=resultados_ordenados[0].keys(), delimiter=';')
            writer.writeheader()
            writer.writerows(resultados_ordenados)

    # Estatísticas
    alta = sum(1 for r in resultados if r.get('confianca') == 'ALTA')
    media = sum(1 for r in resultados if r.get('confianca') == 'MEDIA')
    baixa = sum(1 for r in resultados if r.get('confianca') == 'BAIXA')

    print(f"\n📊 RELATÓRIO GERADO: {ARQ_RELATORIO}")
    print(f"   Total analisado: {len(resultados)} licitações")
    print(f"   🟢 ALTA confiança: {alta}")
    print(f"   🟡 MÉDIA confiança: {media}")
    print(f"   🟠 BAIXA confiança: {baixa}")

    # Log detalhado
    with open(ARQ_LOG, 'a', encoding='utf-8') as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Avaliação em {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        log.write(f"Total: {len(resultados)} | Alta: {alta} | Média: {media} | Baixa: {baixa}\n")
        log.write(f"Top 5 por score:\n")
        for r in resultados_ordenados[:5]:
            log.write(f"  {r['score_final']:5.1f} | {r['confianca']:6} | {r['edital']} | {r['orgao'][:40]}\n")

def main():
    print("="*60)
    print("🔍 AVALIAÇÃO DE COMPATIBILIDADE DE PORTFÓLIO")
    print("="*60)

    # Verifica arquivo de dados
    if not os.path.exists(ARQDADOS):
        print(f"❌ Arquivo {ARQDADOS} não encontrado! Execute limpeza.py primeiro.")
        return

    # Carrega portfólio
    portfolio = carregar_portfolio()
    if portfolio['total_produtos'] == 0:
        print("❌ Portfólio vazio ou não carregado.")
        return

    # Carrega licitações
    print(f"\n📂 Carregando licitações de {ARQDADOS}...")
    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
        licitacoes = json.load(f)
    print(f"✅ {len(licitacoes)} licitações carregadas")

    # Processa em paralelo
    print(f"\n🔄 Avaliando compatibilidade (workers={MAXWORKERS})...")
    resultados = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as executor:
        futures = {executor.submit(processar_licitacao_avaliacao, lic, portfolio): lic 
                   for lic in licitacoes}

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if i % 50 == 0:
                print(f"   Processadas {i}/{len(licitacoes)}...")

            try:
                resultado = future.result()
                if resultado and 'erro' not in resultado:
                    resultados.append(resultado)
            except Exception as e:
                print(f"   ⚠️ Erro em licitação: {e}")

    # Gera relatório
    gerar_relatorio(resultados)

    print("\n✅ Avaliação concluída!")

if __name__ == '__main__':
    main()
