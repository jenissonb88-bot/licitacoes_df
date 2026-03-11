import json
import re
import os
import sys
import gzip
import logging
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import numpy as np

# Tentar importar pandas (opcional)
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# Tentar importar rapidfuzz (opcional)  
try:
    from rapidfuzz import fuzz, process
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    import difflib

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constantes - ✅ AJUSTADOS thresholds para serem mais permissivos no debug
THRESHOLD_ALTO = 0.70
THRESHOLD_MEDIO = 0.50
THRESHOLD_BAIXO = 0.30
MAX_WORKERS = 4

# Nome fixo do arquivo de entrada
ARQ_LICITACOES = 'pregacoes_pharma_limpos.json.gz'
ARQ_PORTFOLIO = 'Exportar Dados.csv'

# Dicionário de sinônimos farmacêuticos (expansível)
SINONIMOS_FARMACOS = {
    "ESCOPOLAMINA": ["HIOSCINA", "BUSCOPAN", "BUTILBROMETO DE ESCOPOLAMINA", "ESCOPOLAMINA BUTILBROMETO"],
    "HIOSCINA": ["ESCOPOLAMINA", "BUSCOPAN", "BUTILBROMETO DE ESCOPOLAMINA"],
    "DIPIRONA": ["METAMIZOL", "DIPIRONA SODICA", "DIPIRONA MONOIDRATADA"],
    "METAMIZOL": ["DIPIRONA"],
    "AMOXICILINA": ["AMOXI", "AMOXICILINA TRIHIDRATADA"],
    "CLAVULANATO": ["CLAV", "CLAVULANATO DE POTASSIO", "ÁCIDO CLAVULÂNICO"],
    "SULFAMETOXAZOL": ["SULFA", "SULFAMETOXAZOL"],
    "TRIMETOPRIMA": ["TRI", "TMP"],
    "BETAMETASONA": ["BETAMET", "BETAMETASONA"],
    "FOSFATO DE BETAMETASONA": ["FOSF.BET", "FOSFATO BETAMETASONA"],
    "CLORPROMAZINA": ["CLOPROMAZOL"],
}


class PortfolioIndexado:
    def __init__(self):
        self.indice_componentes = defaultdict(list)
        self.items_completos = {}
        self.sinonimos_expandidos = {}

    def carregar_portfolio(self, csv_path=ARQ_PORTFOLIO):
        logger.info(f"📂 Carregando portfólio: {csv_path}")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

        # Carregar dados
        if HAS_PANDAS:
            try:
                df = pd.read_csv(csv_path, encoding='utf-8', sep=None, engine='python')
                df = df.where(pd.notnull(df), None)
                registros = df.to_dict('records')
            except Exception as e:
                logger.warning(f"Erro ao ler com pandas: {e}, usando fallback CSV")
                registros = self._carregar_csv_nativo(csv_path)
        else:
            registros = self._carregar_csv_nativo(csv_path)

        logger.info(f"📊 {len(registros)} itens carregados")

        # Indexar cada item
        for idx, row in enumerate(registros):
            try:
                item = self._enriquecer_item(row, idx)
                self.items_completos[item['id']] = item

                # Indexar por cada componente
                for comp in item['componentes_normalizados']:
                    self.indice_componentes[comp].append(item['id'])

                    # Indexar também pelos sinônimos
                    for sinonimo in self._expandir_sinonimos(comp):
                        if sinonimo != comp:
                            self.indice_componentes[sinonimo].append(item['id'])
            except Exception as e:
                logger.warning(f"Erro ao indexar item {idx}: {e}")

        # Estatísticas
        total_entradas = sum(len(v) for v in self.indice_componentes.values())
        logger.info(f"✅ Portfólio indexado: {len(registros)} itens, {len(self.indice_componentes)} componentes, {total_entradas} entradas")

        return self

    def _carregar_csv_nativo(self, path):
        registros = []
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                registros.append(dict(row))
        return registros

    def _enriquecer_item(self, row, idx):
        # Identificadores - ✅ AJUSTADO para múltiplos nomes de colunas possíveis
        sku = row.get('Código') or row.get('CODIGO') or row.get('codigo') or f'ITEM_{idx}'
        descricao = str(row.get('Descrição') or row.get('DESCRICAO') or row.get('descricao') or '').upper().strip()
        farmaco_raw = str(row.get('Fármaco') or row.get('FARMACO') or row.get('farmaco') or '').upper().strip()
        dosagem = str(row.get('Dosagem') or row.get('DOSAGEM') or row.get('dosagem') or '').upper().strip()
        forma = str(row.get('Forma Farmacêutica') or row.get('FORMA_FARMACEUTICA') or row.get('forma') or '').upper().strip()
        sinonimos_raw = str(row.get('Nomes Técnicos/Sinônimos') or row.get('SINONIMOS') or row.get('sinonimos') or '').upper().strip()

        # Extrair componentes da descrição e fármaco
        componentes_desc = self._extrair_componentes(descricao)
        componentes_farm = self._extrair_componentes(farmaco_raw) if farmaco_raw else []
        componentes_sin = self._extrair_componentes(sinonimos_raw) if sinonimos_raw else []

        # Unificar componentes (sem duplicatas, preservando ordem)
        todos_componentes = []
        for c in componentes_desc + componentes_farm + componentes_sin:
            if c not in todos_componentes:
                todos_componentes.append(c)

        # Normalizar componentes
        componentes_normalizados = [self._normalizar_componente(c) for c in todos_componentes]

        # Determinar tipo (simples vs combo)
        tipo = 'combo' if len(componentes_normalizados) > 1 else 'simples'

        # Extrair concentrações
        concentracoes = self._extrair_concentracoes(dosagem) if dosagem else {}

        return {
            'id': str(sku),
            'descricao': descricao,
            'farmaco_original': farmaco_raw,
            'componentes_original': todos_componentes,
            'componentes_normalizados': componentes_normalizados,
            'tipo': tipo,
            'concentracoes': concentracoes,
            'forma_farmaceutica': forma,
            'dosagem_original': dosagem,
            'sinonimos': sinonimos_raw
        }

    def _extrair_componentes(self, texto):
        if not texto:
            return []

        # Separadores comuns
        separadores = r'[+/&,;]'
        partes = re.split(separadores, texto)

        componentes = []
        for parte in partes:
            # Limpar: remover doses, unidades, textos entre parênteses
            limpo = re.sub(r'\d+[\d.,/\s]*\s*(MG|ML|G|UI|MCG|UNIDADES?)', '', parte, flags=re.I)
            limpo = re.sub(r'\(.*?\)', '', limpo)
            limpo = re.sub(r'\b(C/|COM|X|DE|DA|DO|DOS|DAS)\b', '', limpo, flags=re.I)
            limpo = limpo.strip()

            if len(limpo) > 2:
                componentes.append(limpo)

        return componentes

    def _normalizar_componente(self, comp):
        comp = comp.strip()
        # Remover acentos comuns
        substituicoes = {
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
            'Â': 'A', 'Ê': 'E', 'Ô': 'O',
            'Ã': 'A', 'Õ': 'O',
            'Ç': 'C'
        }
        for antigo, novo in substituicoes.items():
            comp = comp.replace(antigo, novo)
        return comp

    def _expandir_sinonimos(self, componente):
        expansao = {componente}
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            if componente == principal or componente in sinonimos:
                expansao.add(principal)
                expansao.update(sinonimos)
        return expansao

    def _extrair_concentracoes(self, dosagem):
        concentracoes = {}

        # Padrões: 500MG, 4MG/ML, 0,5MG, etc.
        padrao = r'(\d+[\d.,]*)\s*(MG|ML|G|UI|MCG)/?(ML)?'
        matches = re.findall(padrao, dosagem, re.I)

        for valor, unidade, per_ml in matches:
            try:
                val_float = float(valor.replace(',', '.'))
                chave = f"{unidade.upper()}_PER_ML" if per_ml else unidade.upper()
                concentracoes[chave] = val_float
            except ValueError:
                continue

        return concentracoes

    def buscar_candidatos(self, componentes_edital, top_n=50):
        contagem = defaultdict(int)
        componentes_encontrados = defaultdict(set)

        for comp in componentes_edital:
            comp_norm = self._normalizar_componente(comp)
            # Buscar componente e sinônimos
            chaves_busca = {comp_norm}
            chaves_busca.update(self._expandir_sinonimos(comp_norm))

            for chave in chaves_busca:
                if chave in self.indice_componentes:
                    for item_id in self.indice_componentes[chave]:
                        contagem[item_id] += 1
                        componentes_encontrados[item_id].add(comp_norm)

        # Ordenar por número de matches (descendente)
        candidatos_ordenados = sorted(
            contagem.items(), 
            key=lambda x: (x[1], x[0]), 
            reverse=True
        )

        return [
            {
                'id': item_id, 
                'matches_componentes': componentes_encontrados[item_id],
                'score_indice': count
            }
            for item_id, count in candidatos_ordenados[:top_n]
        ]


class MatcherHibrido:
    def __init__(self, portfolio_indexado):
        self.portfolio = portfolio_indexado

    def avaliar_licitacao(self, licitacao):
        # ✅ AJUSTADO: Suporta múltiplos nomes de campos
        objeto = licitacao.get('objeto') or licitacao.get('obj') or ''
        itens = licitacao.get('itens', [])

        if not itens and objeto:
            # Se não tem itens estruturados, tratar objeto como único item
            itens = [{'descricao': objeto, 'quantidade': 1}]

        resultados = []
        for item_edital in itens:
            try:
                matches = self._avaliar_item(item_edital)
                if matches:
                    resultados.extend(matches)
            except Exception as e:
                logger.debug(f"Erro ao avaliar item: {e}")

        # Consolidar e ordenar
        resultados = self._consolidar_resultados(resultados)
        return resultados

    def _avaliar_item(self, item_edital):
        # ✅ AJUSTADO: Suporta múltiplos nomes de campos
        descricao = str(item_edital.get('descricao') or item_edital.get('d') or '').upper()

        if not descricao or len(descricao) < 3:
            return []

        # Extrair componentes do edital
        componentes_edital = self._extrair_componentes_edital(descricao)
        if not componentes_edital:
            logger.debug(f"Nenhum componente extraído de: {descricao[:50]}...")
            return []

        tipo_edital = 'combo' if len(componentes_edital) > 1 else 'simples'
        concentracoes_edital = self._extrair_concentracoes(descricao)

        # FASE 1: Indexação - encontrar candidatos
        candidatos = self.portfolio.buscar_candidatos(componentes_edital, top_n=30)

        if not candidatos:
            logger.debug(f"Nenhum candidato encontrado para: {componentes_edital}")
            return []

        # FASE 2: Scoring detalhado
        resultados = []
        for candidato in candidatos:
            try:
                item_portfolio = self.portfolio.items_completos[candidato['id']]

                score = self._calcular_score_hibrido(
                    componentes_edital,
                    concentracoes_edital,
                    tipo_edital,
                    item_portfolio,
                    candidato['matches_componentes']
                )

                if score >= THRESHOLD_BAIXO:
                    resultados.append({
                        'item_portfolio': item_portfolio,
                        'score': score,
                        'tipo_match': self._classificar_score(score),
                        'componentes_match': list(candidato['matches_componentes']),
                        'detalhes': self._gerar_detalhes(componentes_edital, item_portfolio)
                    })
            except Exception as e:
                logger.debug(f"Erro ao calcular score: {e}")

        # Ordenar por score
        resultados.sort(key=lambda x: x['score'], reverse=True)
        return resultados

    def _extrair_componentes_edital(self, descricao):
        componentes = []

        # Padrão: "NOME CONCENTRAÇÃO + NOME2 CONCENTRAÇÃO2"
        padrao_componente = r'([A-Z][A-Z\s]+?)\s+\d+[\d.,/]*\s*(?:MG|ML|G|UI|MCG)'
        matches = re.findall(padrao_componente, descricao, re.I)

        if matches:
            for m in matches:
                comp = m.strip()
                if len(comp) > 2:
                    componentes.append(comp)

        # Se não encontrou padrão farmacêutico, usar separadores genéricos
        if not componentes:
            separadores = r'[+/&,;]'
            partes = re.split(separadores, descricao)
            for parte in partes:
                limpo = re.sub(r'\d+[\d.,/\s]*\s*(MG|ML|G|UI|MCG|UNIDADES?)', '', parte, flags=re.I)
                limpo = re.sub(r'\(.*?\)', '', limpo)
                limpo = limpo.strip()
                if len(limpo) > 3:
                    componentes.append(limpo)

        return componentes

    def _calcular_score_hibrido(self, comps_edital, concs_edital, tipo_edital, 
                                 item_portfolio, matches_indices):
        comps_portfolio = item_portfolio['componentes_normalizados']
        tipo_portfolio = item_portfolio['tipo']

        # 1. COBERTURA DE COMPONENTES (40%)
        comps_edital_norm = [self.portfolio._normalizar_componente(c) for c in comps_edital]
        comps_portfolio_norm = comps_portfolio

        matches_diretos = sum(1 for c in comps_edital_norm if any(
            self._match_componente(c, cp) for cp in comps_portfolio_norm
        ))

        if not comps_edital:
            cobertura = 0
        else:
            cobertura = matches_diretos / len(comps_edital)

        score_cobertura = cobertura * 0.40

        # 2. PRECISÃO DO MATCH (30%)
        if HAS_RAPIDFUZZ and comps_edital and comps_portfolio:
            similaridades = []
            for ce in comps_edital:
                melhor_match = max(
                    fuzz.ratio(ce, cp) for cp in comps_portfolio
                )
                similaridades.append(melhor_match)
            score_similaridade = (sum(similaridades) / len(similaridades) / 100) * 0.30
        else:
            score_similaridade = cobertura * 0.30

        # 3. REGRAS DE NEGÓCIO (30%)
        score_regras = 0

        # Regra 3.1: Penalidade/Bonificação por tipo de combinação
        if tipo_edital == 'combo' and tipo_portfolio == 'combo':
            if cobertura >= 0.8:
                score_regras += 0.20
            else:
                score_regras += 0.10
        elif tipo_edital == 'combo' and tipo_portfolio == 'simples':
            score_regras -= 0.15
        elif tipo_edital == 'simples' and tipo_portfolio == 'combo':
            score_regras += 0.05
        else:
            score_regras += 0.10

        # Regra 3.2: Validação de concentração
        if concs_edital and item_portfolio['concentracoes']:
            match_conc = self._validar_concentracoes(concs_edital, item_portfolio['concentracoes'])
            score_regras += match_conc * 0.10
        else:
            score_regras += 0.05

        # Score total
        score_total = score_cobertura + score_similaridade + score_regras

        # ✅ DEBUG: Log quando score é baixo mas componentes parecem matching
        if score_total < THRESHOLD_BAIXO and cobertura > 0:
            logger.debug(f"Score baixo ({score_total:.2f}) mas cobertura={cobertura:.2f}: {comps_edital} vs {comps_portfolio}")

        return max(0.0, min(1.0, score_total))

    def _match_componente(self, comp1, comp2):
        if comp1 == comp2:
            return True

        # Verificar sinônimos
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            grupo = {principal} | set(sinonimos)
            if comp1 in grupo and comp2 in grupo:
                return True

        # Similaridade fuzzy
        if HAS_RAPIDFUZZ:
            return fuzz.ratio(comp1, comp2) > 85
        else:
            return comp1[:5] == comp2[:5]

    def _validar_concentracoes(self, concs_edital, concs_portfolio):
        if not concs_edital or not concs_portfolio:
            return 0.5

        matches = 0
        for chave, valor_edital in concs_edital.items():
            if chave in concs_portfolio:
                valor_portfolio = concs_portfolio[chave]
                if valor_portfolio == 0:
                    continue
                diff = abs(valor_edital - valor_portfolio) / valor_portfolio
                if diff <= 0.10:
                    matches += 1

        total = len(concs_edital)
        return matches / total if total > 0 else 0.5

    def _classificar_score(self, score):
        if score >= THRESHOLD_ALTO:
            return 'ALTO'
        elif score >= THRESHOLD_MEDIO:
            return 'MEDIO'
        elif score >= THRESHOLD_BAIXO:
            return 'BAIXO'
        else:
            return 'INCOMPATIVEL'

    def _consolidar_resultados(self, resultados):
        if not resultados:
            return []

        # Agrupar por ID do item do portfólio
        agrupados = defaultdict(list)
        for r in resultados:
            agrupados[r['item_portfolio']['id']].append(r)

        # Pegar melhor score de cada grupo
        consolidados = []
        for item_id, matches in agrupados.items():
            melhor = max(matches, key=lambda x: x['score'])
            consolidados.append(melhor)

        # Ordenar final
        consolidados.sort(key=lambda x: x['score'], reverse=True)
        return consolidados

    def _gerar_detalhes(self, comps_edital, item_portfolio):
        comps_port = item_portfolio['componentes_normalizados']
        return f"Edital:{comps_edital} ↔ Portfolio:{comps_port}"


def main():
    """Função principal."""
    logger.info("🚀 Iniciando avaliação de portfólio v3.1 (Corrigida)")
    logger.info(f"⚙️ Thresholds: ALTO≥{THRESHOLD_ALTO}, MEDIO≥{THRESHOLD_MEDIO}, BAIXO≥{THRESHOLD_BAIXO}")

    # 1. Carregar e indexar portfólio
    portfolio = PortfolioIndexado().carregar_portfolio()

    # 2. CARREGAR LICITAÇÕES DO ARQUIVO CORRETO
    licitacoes = []
    
    # Procura arquivo específico do limpeza.py
    padroes = [
        ARQ_LICITACOES,
        'pregacoes_pharma_limpos_*.json.gz',
        'licitacoes_*.json*'
    ]
    
    arquivos_licitacoes = []
    for padrao in padroes:
        encontrados = list(Path('.').glob(padrao))
        if encontrados:
            arquivos_licitacoes = encontrados
            logger.info(f"📂 Encontrado padrão '{padrao}': {len(encontrados)} arquivo(s)")
            for arq in encontrados:
                logger.info(f"   📄 {arq.name}")
            break
    
    if not arquivos_licitacoes:
        logger.error(f"❌ Nenhum arquivo de licitações encontrado!")
        logger.info(f"   📁 Diretório atual: {os.getcwd()}")
        logger.info(f"   📁 Arquivos disponíveis: {[f for f in os.listdir('.') if '.json' in f or '.gz' in f]}")
        return

    # Carregar licitações dos arquivos encontrados
    for arquivo in arquivos_licitacoes:
        try:
            if str(arquivo).endswith('.gz'):
                with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
                    dados = json.load(f)
            else:
                with open(arquivo, 'r', encoding='utf-8') as f:
                    dados = json.load(f)

            if isinstance(dados, list):
                licitacoes.extend(dados)
                logger.info(f"✅ {arquivo.name}: {len(dados)} licitações")
            else:
                licitacoes.append(dados)
                logger.info(f"✅ {arquivo.name}: 1 licitação (dict)")

        except Exception as e:
            logger.error(f"❌ Erro ao carregar {arquivo}: {e}")

    logger.info(f"📋 Total: {len(licitacoes)} licitações para avaliar")

    if not licitacoes:
        logger.warning("⚠️ Nenhuma licitação encontrada para avaliar")
        return

    # ✅ DEBUG: Mostrar estrutura da primeira licitação
    if licitacoes:
        primeira = licitacoes[0]
        logger.info(f"🔍 Estrutura da primeira licitação: id={primeira.get('id')}, obj={primeira.get('obj', '')[:50]}...")
        logger.info(f"   Campos disponíveis: {list(primeira.keys())}")
        if primeira.get('itens'):
            logger.info(f"   Número de itens: {len(primeira['itens'])}")
            if primeira['itens']:
                logger.info(f"   Primeiro item: {list(primeira['itens'][0].keys())}")

    # 3. Processar licitações - ✅ SEMPRE EM MODO SERIAL para evitar problemas de multiprocessing
    resultados_finais = []
    matcher = MatcherHibrido(portfolio)

    logger.info(f"🔧 Processando em modo serial (mais estável)")
    
    for idx, lic in enumerate(licitacoes):
        try:
            matches = matcher.avaliar_licitacao(lic)
            if matches:
                resultados_finais.append({
                    'licitacao': lic.get('id', f'idx_{idx}'),
                    'matches': matches
                })
                # ✅ DEBUG: Log do primeiro match encontrado
                if len(resultados_finais) == 1:
                    logger.info(f"✅ Primeiro match encontrado na licitação {idx}: {matches[0]['item_portfolio']['id']} (score: {matches[0]['score']:.2%})")
        except Exception as e:
            logger.debug(f"❌ Erro processando licitação {idx}: {e}")

        # Progresso a cada 500
        if (idx + 1) % 500 == 0:
            logger.info(f"   Processadas {idx + 1}/{len(licitacoes)} licitações...")

    # 4. Gerar relatório
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_saida = f"relatorio_compatibilidade_{timestamp}.csv"

    # ✅ CSV com todas as colunas esperadas pelo HTML
    with open(arquivo_saida, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'licitacao_id', 'item_portfolio_id', 'descricao_portfolio',
            'score', 'tipo_match', 'componentes_match', 'detalhes'
        ])

        for resultado in resultados_finais:
            lic_id = resultado['licitacao']
            for match in resultado['matches']:
                writer.writerow([
                    lic_id,
                    match['item_portfolio']['id'],
                    match['item_portfolio']['descricao'][:100],
                    f"{match['score']:.2%}",
                    match['tipo_match'],
                    '|'.join(match['componentes_match']),
                    match['detalhes']
                ])

    logger.info(f"✅ Relatório gerado: {arquivo_saida}")
    logger.info(f"📊 Licitações com matches: {len(resultados_finais)}/{len(licitacoes)}")
    
    # Resumo por tipo de match
    resumo = {'ALTO': 0, 'MEDIO': 0, 'BAIXO': 0, 'INCOMPATIVEL': 0}
    for r in resultados_finais:
        for m in r['matches']:
            resumo[m['tipo_match']] = resumo.get(m['tipo_match'], 0) + 1
    
    logger.info(f"📈 Matches: ALTO={resumo['ALTO']}, MEDIO={resumo['MEDIO']}, BAIXO={resumo['BAIXO']}")

    # ✅ Alerta se não encontrou nada
    if len(resultados_finais) == 0:
        logger.warning("⚠️ NENHUM MATCH ENCONTRADO!")
        logger.warning("   Possíveis causas:")
        logger.warning("   1. Estrutura dos dados de licitação não corresponde ao esperado")
        logger.warning("   2. Nomes de campos diferentes (obj vs objeto, d vs descricao)")
        logger.warning("   3. Componentes farmacêuticos não estão sendo extraídos corretamente")
        logger.warning("   4. Thresholds muito altos para os dados atuais")


if __name__ == "__main__":
    main()
