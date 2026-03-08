import csv
import json
import gzip
import re
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
import unicodedata
from collections import defaultdict

# ============================================================================
# CONFIGURACOES
# ============================================================================
THRESHOLD_ALTO = 0.70
THRESHOLD_MEDIO = 0.50
THRESHOLD_BAIXO = 0.30

ARQ_PORTFOLIO = 'Exportar Dados.csv'
ARQ_LICITACOES = 'pregacoes_pharma_limpos.json.gz'
ARQ_SAIDA = 'relatorio_compatibilidade.csv'
ARQ_LOG = 'log_matcher.log'

MAX_WORKERS = 10

# ============================================================================
# DICIONARIO DE SINONIMOS
# ============================================================================
SINONIMOS_FARMACOS = {
    "ESCOPOLAMINA": ["HIOSCINA", "BUTILBROMETO DE ESCOPOLAMINA", "BROMETO DE ESCOPOLAMINA", 
                     "BUTILESCOPOLAMINA", "BUSCOPAN", "BUSCOPAN COMPOSTO"],
    "DIPIRONA": ["METAMIZOL", "DIPIRONA MONOIDRATADA", "DIPIRONA SODICA", "NORAMIDAZOFENINA"],
    "EPINEFRINA": ["ADRENALINA"],
    "FENITOÍNA": ["HIDANTOÍNA", "DIFENILHIDANTOÍNA", "FENITOINA"],
    "FENOBARBITAL": ["FENOBARBITONA", "GARDENAL"],
    "DIAZEPAM": ["VALIUM"],
    "MIDAZOLAM": ["DORMONID"],
    "CLONAZEPAM": ["RIVOTRIL"],
    "HALOPERIDOL": ["HALDOL"],
    "PARACETAMOL": ["ACETAMINOFENO", "ACETAMINOFEN"],
    "CLAVULANATO DE POTÁSSIO": ["CLAV POTASSIO", "CLAVULANATO", "CLAV POT", "ÁCIDO CLAVULÂNICO"],
    "AMOXICILINA": ["AMOXICILINA TRIIDRATADA"],
    "SULFAMETOXAZOL": ["SULFA"],
    "TRIMETOPRIMA": ["TMP"],
}

# ============================================================================
# FUNCOES UTILITARIAS
# ============================================================================
def log(msg, nivel="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] [{nivel}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + os.linesep)

def normalizar(texto):
    if not texto:
        return ""
    texto = str(texto).upper().strip()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) 
                   if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', texto)

def similaridade(s1, s2):
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1, s2).ratio()

# ============================================================================
# PORTFOLIO MATCHER OTIMIZADO
# ============================================================================
class PortfolioMatcherOtimizado:
    """
    Versao otimizada com:
    - Indexacao por componente
    - Cache de matches
    - Pre-computacao de normalizacoes
    """

    def __init__(self, portfolio_rows):
        # Pre-processa todo o portfolio
        self.itens = []
        self.index = defaultdict(list)  # indice por componente

        for idx, row in enumerate(portfolio_rows):
            desc = row.get('Descrição', '')
            farmaco = row.get('Fármaco', '')
            dosagem = row.get('Dosagem', '')
            forma = row.get('Forma Farmacêutica', '')

            # Normaliza
            farmaco_norm = self._norm_farmaco(farmaco)
            componentes = self._extrair_componentes(farmaco_norm)
            dosagem_norm = normalizar(dosagem) if dosagem else ""
            forma_norm = normalizar(forma)

            item = {
                'idx': idx,
                'descricao': desc,
                'farmaco': farmaco_norm,
                'componentes': componentes,
                'dosagem': dosagem_norm,
                'forma': forma_norm,
                'is_combo': len(componentes) > 1
            }

            self.itens.append(item)

            # Indexa por cada componente
            for comp in componentes:
                comp_base = comp.split()[0] if ' ' in comp else comp
                self.index[comp_base[:6]].append(idx)  # indexa por prefixo de 6 chars

        log(f"Portfolio indexado: {len(self.itens)} itens, {len(self.index)} entradas no indice")

    def _norm_farmaco(self, farmaco):
        if not farmaco:
            return ""
        farmaco = str(farmaco).upper().strip()
        subs = {
            'CLAV.': 'CLAVULANATO', 'CLAV ': 'CLAVULANATO ',
            'FOSF.': 'FOSFATO', 'FOSF ': 'FOSFATO ',
            'SOD.': 'SODICO', 'SOD ': 'SODICO ',
            'POT.': 'POTASSIO', 'POT ': 'POTASSIO ',
            'CAP.': 'CAPSULA', 'CAP ': 'CAPSULA ',
            'CPR.': 'COMPRIMIDO', 'CPR ': 'COMPRIMIDO ',
            'COMP.': 'COMPRIMIDO', 'COMP ': 'COMPRIMIDO ',
            'AMP.': 'AMPOLA', 'AMP ': 'AMPOLA ',
            'FR.': 'FRASCO', 'FR ': 'FRASCO ',
            'SOL.': 'SOLUCAO', 'SOL ': 'SOLUCAO ',
        }
        for antigo, novo in subs.items():
            farmaco = farmaco.replace(antigo, novo)
        return farmaco

    def _extrair_componentes(self, farmaco):
        if not farmaco:
            return []
        separadores = ['+', ' E ', '/']
        for sep in separadores:
            if sep in farmaco:
                comps = [c.strip() for c in farmaco.split(sep)]
                return [c for c in comps if len(c) > 2]
        return [farmaco] if len(farmaco) > 2 else []

    def _match_comp(self, c1, c2):
        """Match de componentes com sinônimos"""
        if c1 == c2:
            return 1.0

        # Verifica sinônimos
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            todos = [principal] + sinonimos
            if any(c1 == s for s in todos) and any(c2 == s for s in todos):
                return 1.0

        # Similaridade
        sim = similaridade(c1, c2)
        if c1 in c2 or c2 in c1:
            sim = max(sim, 0.7)
        return sim

    def match(self, desc_edital, conc_edital=None, forma_edital=None):
        """
        Matching otimizado usando índice
        """
        desc_norm = normalizar(desc_edital)

        # Extrai componentes do edital
        comps_edital = []
        if '+' in desc_norm:
            partes = desc_norm.split('+')
        else:
            partes = [desc_norm]

        for parte in partes:
            parte = re.sub(r'\d+[,.]?\d*\s*(MG|ML|G|UI|MCG|%|UN).*', '', parte)
            parte = re.sub(r'(CONCENTRACAO|DOSAGEM|FORMA|VIA|APRESENTACAO).*', '', parte)
            parte = parte.strip()
            if len(parte) > 3:
                comps_edital.append(parte)

        if not comps_edital:
            return []

        # Busca candidatos via índice (otimização principal)
        candidatos = set()
        for comp in comps_edital:
            prefixo = comp[:6]
            candidatos.update(self.index.get(prefixo, []))

        # Se não achou no índice, busca em todos (fallback)
        if not candidatos:
            candidatos = range(len(self.itens))

        # Calcula scores apenas para candidatos
        resultados = []
        for idx in candidatos:
            item = self.itens[idx]
            score = 0.0
            detalhes = {}

            # Match de componentes (60%)
            comps_port = item['componentes']
            if comps_edital and comps_port:
                matches = [max(self._match_comp(ce, cp) for ce in comps_edital) for cp in comps_port]
                score_comp = sum(matches) / len(matches)
                cobertura = sum(1 for m in matches if m > 0.7) / len(matches)

                if item['is_combo'] and cobertura == 1.0:
                    score_comp = min(1.0, score_comp * 1.15)
                    detalhes['tipo'] = 'COMBINACAO_COMPLETA'
                else:
                    detalhes['tipo'] = 'SIMPLES' if not item['is_combo'] else 'PARCIAL'

                score += (score_comp * 0.7 + cobertura * 0.3) * 0.60

            # Match de concentracao (25%)
            if conc_edital and item['dosagem']:
                conc_norm = normalizar(conc_edital)
                if conc_norm == item['dosagem']:
                    score += 0.25
                else:
                    sim = similaridade(conc_norm, item['dosagem'])
                    if conc_norm in item['dosagem'] or item['dosagem'] in conc_norm:
                        sim = max(sim, 0.6)
                    score += sim * 0.25
            else:
                score += 0.15

            # Match de forma (15%)
            if forma_edital and item['forma']:
                forma_norm = normalizar(forma_edital)
                sim = similaridade(forma_norm, item['forma'])
                if forma_norm in item['forma'] or item['forma'] in forma_norm:
                    sim = max(sim, 0.8)
                score += sim * 0.15
            else:
                score += 0.075

            if score >= 0.50:
                resultados.append({
                    'idx': idx,
                    'score': score,
                    'item': item,
                    'detalhes': detalhes
                })

        resultados.sort(key=lambda x: x['score'], reverse=True)
        return resultados

# ============================================================================
# CARREGAMENTO
# ============================================================================
def carregar_portfolio():
    log(f"Carregando portfolio de {ARQ_PORTFOLIO}...")

    if not os.path.exists(ARQ_PORTFOLIO):
        log(f"ERRO: {ARQ_PORTFOLIO} nao encontrado!", "ERRO")
        return None

    rows = []
    encodings = ['utf-8-sig', 'latin1', 'cp1252']

    for enc in encodings:
        try:
            with open(ARQ_PORTFOLIO, 'r', encoding=enc) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                break
        except:
            continue

    if not rows:
        log("ERRO: Nao foi possivel ler CSV", "ERRO")
        return None

    log(f"✅ Portfolio: {len(rows)} itens")
    return rows

def carregar_licitacoes():
    log(f"Carregando licitacoes de {ARQ_LICITACOES}...")

    if not os.path.exists(ARQ_LICITACOES):
        log(f"ERRO: {ARQ_LICITACOES} nao encontrado!", "ERRO")
        return None

    with gzip.open(ARQ_LICITACOES, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    if isinstance(dados, dict):
        licitacoes = []
        for orgao, editais in dados.items():
            for edital, info in editais.items():
                info['orgao'] = orgao
                info['edital'] = edital
                info['_idx'] = len(licitacoes)
                licitacoes.append(info)
    else:
        for idx, item in enumerate(dados):
            item['_idx'] = idx
        licitacoes = dados

    log(f"✅ {len(licitacoes)} licitacoes")
    return licitacoes

# ============================================================================
# AVALIACAO
# ============================================================================
def avaliar_licitacao(lic, matcher):
    try:
        objeto = str(lic.get('obj', '') or lic.get('objeto', ''))
        edital_id = lic.get('edital', lic.get('id', 'unknown'))
        orgao = lic.get('orgao', lic.get('org', 'N/A'))

        itens = lic.get('itens', [])
        if not isinstance(itens, list):
            itens = []

        if not itens:
            itens = [{'d': objeto}]

        melhores = []
        for item in itens:
            if isinstance(item, dict):
                desc = item.get('d', '') or item.get('descricao', '')
                conc = item.get('concentracao')
                forma = item.get('forma') or item.get('forma_farmaceutica')
            else:
                desc = str(item)
                conc = forma = None

            if not desc:
                continue

            matches = matcher.match(desc, conc, forma)
            if matches:
                melhores.append({
                    'score': matches[0]['score'],
                    'desc': matches[0]['item']['descricao'],
                    'tipo': matches[0]['detalhes'].get('tipo', 'SIMPLES')
                })

        if not melhores:
            return {
                'id': edital_id,
                'orgao': orgao,
                'objeto': objeto[:200],
                'percentual': 0.0,
                'confianca': 'INCOMPATIVEL',
                'matches': [],
                'total_itens': len(itens),
                'compativeis': 0,
                '_idx': lic.get('_idx', 0)
            }

        scores = [m['score'] for m in melhores]
        score_medio = sum(scores) / len(scores)
        taxa = len(melhores) / len(itens) if itens else 0

        if taxa >= 0.5:
            score_final = min(1.0, score_medio * (1 + 0.1 * taxa))
        else:
            score_final = score_medio

        percentual = round(score_final * 100, 2)

        if score_final >= THRESHOLD_ALTO:
            conf = 'ALTA'
        elif score_final >= THRESHOLD_MEDIO:
            conf = 'MEDIA'
        elif score_final >= THRESHOLD_BAIXO:
            conf = 'BAIXA'
        else:
            conf = 'INCOMPATIVEL'

        matches_str = [f"{m['desc'][:30]}... ({m['score']:.0%})" for m in melhores[:5]]

        return {
            'id': edital_id,
            'orgao': orgao,
            'objeto': objeto[:200],
            'percentual': percentual,
            'confianca': conf,
            'matches': matches_str,
            'total_itens': len(itens),
            'compativeis': len(melhores),
            '_idx': lic.get('_idx', 0)
        }

    except Exception as e:
        log(f"Erro em {lic.get('id', 'unknown')}: {e}", "ERRO")
        return {
            'id': lic.get('id', 'unknown'),
            'percentual': 0.0,
            'confianca': 'ERRO',
            'erro': str(e),
            '_idx': lic.get('_idx', 0)
        }

def avaliar_todas(licitacoes, matcher):
    log(f"🔍 Avaliando {len(licitacoes)} licitacoes...")

    resultados = []
    processadas = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(avaliar_licitacao, lic, matcher): lic for lic in licitacoes}

        for future in as_completed(futures):
            resultados.append(future.result())
            processadas += 1
            if processadas % 100 == 0:
                log(f"Processadas {processadas}/{len(licitacoes)}...")

    resultados.sort(key=lambda x: x['_idx'])
    return resultados

# ============================================================================
# RELATORIO
# ============================================================================
def gerar_relatorio(resultados, origem="MANUAL"):
    log("="*60)
    log(f"📊 GERANDO RELATORIO [{origem}]")
    log("="*60)

    with open(ARQ_SAIDA, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['id', 'orgao', 'objeto_licitacao', 'percentual', 'confianca',
                        'total_itens', 'itens_compativeis', 'principais_matches'])

        for r in resultados:
            writer.writerow([
                r['id'], r.get('orgao', ''), r.get('objeto', ''),
                r['percentual'], r['confianca'],
                r.get('total_itens', 0), r.get('compativeis', 0),
                '|'.join(r.get('matches', []))[:500]
            ])

    total = len(resultados)
    alta = len([r for r in resultados if r['confianca'] == 'ALTA'])
    media = len([r for r in resultados if r['confianca'] == 'MEDIA'])
    baixa = len([r for r in resultados if r['confianca'] == 'BAIXA'])
    incomp = len([r for r in resultados if r['confianca'] == 'INCOMPATIVEL'])

    log(f"📁 Arquivo: {ARQ_SAIDA}")
    log(f"📊 Total: {total} | 🟢 ALTA: {alta} | 🟡 MEDIA: {media} | 🔴 BAIXA: {baixa} | ⚪ INCOMPATIVEL: {incomp}")

    return ARQ_SAIDA

# ============================================================================
# MAIN
# ============================================================================
def main():
    origem = "MANUAL"
    if len(sys.argv) > 1:
        origem = sys.argv[1].upper()

    if os.path.exists(ARQ_LOG):
        os.remove(ARQ_LOG)

    log("="*70)
    log(f"🤖 MATCHER FARMACEUTICO v2.0 OTIMIZADO [{origem}]")
    log(f"📊 THRESHOLDS: ≥{THRESHOLD_ALTO:.0%} ALTO | ≥{THRESHOLD_MEDIO:.0%} MEDIA | ≥{THRESHOLD_BAIXO:.0%} BAIXA")
    log("="*70)

    portfolio = carregar_portfolio()
    if not portfolio:
        return 1

    log("🧠 Inicializando matcher otimizado...")
    matcher = PortfolioMatcherOtimizado(portfolio)

    licitacoes = carregar_licitacoes()
    if not licitacoes:
        return 1

    resultados = avaliar_todas(licitacoes, matcher)
    gerar_relatorio(resultados, origem)

    log(f"✅ Matcher [{origem}] concluido!")
    return 0

if __name__ == "__main__":
    exit(main())
