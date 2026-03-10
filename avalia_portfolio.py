#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
avalia_portfolio_v3.py - Matcher Farmacêutico Híbrido
Versão 3.0: Indexação + Regras de Negócio Rigorous

Arquitetura:
  1. INDEXAÇÃO: Encontra candidatos em O(1) por componente
  2. SCORING DETALHADO: Fuzzy matching + similaridade de cosseno
  3. REGRAS DE NEGÓCIO: Validações que indexação pura não captura
     - Penalidade para match parcial de combos
     - Validação de concentração
     - Bonificação para matches completos

Resolve problemas da v2:
  - Falso positivo: edital combo vs item simples
  - Ignorância de concentração na indexação
  - Over-matching de itens genéricos
"""

import json
import re
import os
import sys
import gzip
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import numpy as np

# Tentar importar pandas, senão usar csv nativo
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    import csv

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

# Constantes
THRESHOLD_ALTO = 0.70      # ≥70% = ALTO
THRESHOLD_MEDIO = 0.50     # 50-69% = MÉDIO  
THRESHOLD_BAIXO = 0.30     # 30-49% = BAIXO
MAX_WORKERS = 4
CHECKPOINT_FILE = "checkpoint_avaliacao_v3.json"

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
    """
    Estrutura de indexação híbrida:
    - Índice invertido por componente (velocidade)
    - Metadados enriquecidos (precisão)
    """

    def __init__(self):
        self.indice_componentes = defaultdict(list)  # componente -> [items]
        self.items_completos = {}  # id -> dados completos
        self.sinonimos_expandidos = {}  # componente -> [sinônimos]

    def carregar_portfolio(self, csv_path='Exportar Dados.csv'):
        """Carrega e indexa o portfólio com enriquecimento."""
        logger.info(f"📂 Carregando portfólio: {csv_path}")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

        # Carregar dados
        if HAS_PANDAS:
            df = pd.read_csv(csv_path, encoding='utf-8', sep=None, engine='python')
            df = df.where(pd.notnull(df), None)
            registros = df.to_dict('records')
        else:
            registros = self._carregar_csv_nativo(csv_path)

        logger.info(f"📊 {len(registros)} itens carregados")

        # Indexar cada item
        for idx, row in enumerate(registros):
            item = self._enriquecer_item(row, idx)
            self.items_completos[item['id']] = item

            # Indexar por cada componente
            for comp in item['componentes_normalizados']:
                self.indice_componentes[comp].append(item['id'])

                # Indexar também pelos sinônimos
                for sinonimo in self._expandir_sinonimos(comp):
                    if sinonimo != comp:
                        self.indice_componentes[sinonimo].append(item['id'])

        # Estatísticas
        total_entradas = sum(len(v) for v in self.indice_componentes.values())
        logger.info(f"✅ Portfólio indexado: {len(registros)} itens, {len(self.indice_componentes)} componentes, {total_entradas} entradas")

        return self

    def _carregar_csv_nativo(self, path):
        """Fallback para carregamento sem pandas."""
        registros = []
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                registros.append(dict(row))
        return registros

    def _enriquecer_item(self, row, idx):
        """Enriquece item do CSV com metadados para matching."""
        # Identificadores
        sku = row.get('Código', f'ITEM_{idx}')
        descricao = str(row.get('Descrição', '')).upper().strip()
        farmaco_raw = str(row.get('Fármaco', '')).upper().strip()
        dosagem = str(row.get('Dosagem', '')).upper().strip()
        forma = str(row.get('Forma Farmacêutica', '')).upper().strip()
        sinonimos_raw = str(row.get('Nomes Técnicos/Sinônimos', '')).upper().strip()

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
            'id': sku,
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
        """Extrai componentes químicos de texto."""
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
            limpo = re.sub(r'(C/|COM|X|DE|DA|DO|DOS|DAS)', '', limpo, flags=re.I)
            limpo = limpo.strip()

            if len(limpo) > 2:
                componentes.append(limpo)

        return componentes

    def _normalizar_componente(self, comp):
        """Normaliza nome do componente."""
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
        """Expande componente para incluir sinônimos conhecidos."""
        expansao = {componente}
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            if componente == principal or componente in sinonimos:
                expansao.add(principal)
                expansao.update(sinonimos)
        return expansao

    def _extrair_concentracoes(self, dosagem):
        """Extrai concentrações numéricas da string de dosagem."""
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
        """
        Busca candidatos via índice.
        Retorna lista de IDs ordenados por frequência de matches.
        """
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
    """
    Motor de matching híbrido:
    - Usa indexação para encontrar candidatos (velocidade)
    - Aplica regras de negócio rigorosas (precisão)
    """

    def __init__(self, portfolio_indexado):
        self.portfolio = portfolio_indexado

    def avaliar_licitacao(self, licitacao):
        """
        Avalia uma licitação completa contra o portfólio.
        Retorna lista de matches ordenados por score.
        """
        # Extrair dados do edital
        objeto = licitacao.get('objeto', '')
        itens = licitacao.get('itens', [])

        if not itens and objeto:
            # Se não tem itens estruturados, tratar objeto como único item
            itens = [{'descricao': objeto, 'quantidade': 1}]

        resultados = []
        for item_edital in itens:
            matches = self._avaliar_item(item_edital)
            if matches:
                resultados.extend(matches)

        # Consolidar e ordenar
        resultados = self._consolidar_resultados(resultados)
        return resultados

    def _avaliar_item(self, item_edital):
        """Avalia um item do edital contra o portfólio."""
        descricao = str(item_edital.get('descricao', '')).upper()

        # Extrair componentes do edital
        componentes_edital = self._extrair_componentes_edital(descricao)
        if not componentes_edital:
            return []

        tipo_edital = 'combo' if len(componentes_edital) > 1 else 'simples'
        concentracoes_edital = self._extrair_concentracoes(descricao)

        # FASE 1: Indexação - encontrar candidatos
        candidatos = self.portfolio.buscar_candidatos(componentes_edital, top_n=30)

        if not candidatos:
            return []

        # FASE 2: Scoring detalhado
        resultados = []
        for candidato in candidatos:
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

        # Ordenar por score
        resultados.sort(key=lambda x: x['score'], reverse=True)
        return resultados

    def _extrair_componentes_edital(self, descricao):
        """Extrai componentes de descrição do edital."""
        # Primeiro tentar extrair de padrões farmacêuticos
        componentes = []

        # Padrão: "NOME CONCENTRAÇÃO + NOME2 CONCENTRAÇÃO2"
        # Ex: "DIPIRONA 500MG/ML + ESCOPOLAMINA 4MG/ML"
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
                # Limpar similar ao portfólio
                limpo = re.sub(r'\d+[\d.,/\s]*\s*(MG|ML|G|UI|MCG|UNIDADES?)', '', parte, flags=re.I)
                limpo = re.sub(r'\(.*?\)', '', limpo)
                limpo = limpo.strip()
                if len(limpo) > 3:
                    componentes.append(limpo)

        return componentes

    def _calcular_score_hibrido(self, comps_edital, concs_edital, tipo_edital, 
                                 item_portfolio, matches_indices):
        """
        Calcula score híbrido considerando múltiplos fatores.
        """
        comps_portfolio = item_portfolio['componentes_normalizados']
        tipo_portfolio = item_portfolio['tipo']

        # 1. COBERTURA DE COMPONENTES (40%)
        # Quantos componentes do edital estão no portfólio?
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
        # Similaridade textual dos componentes
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

        # 3. REGRAS DE NEGÓCIO (30%) - CRÍTICO
        score_regras = 0

        # Regra 3.1: Penalidade/Bonificação por tipo de combinação
        if tipo_edital == 'combo' and tipo_portfolio == 'combo':
            # Edital quer combo, portfólio tem combo = IDEAL
            if cobertura >= 0.8:  # Match quase completo
                score_regras += 0.20  # Bonificação máxima
            else:
                score_regras += 0.10
        elif tipo_edital == 'combo' and tipo_portfolio == 'simples':
            # Edital quer combo, portfólio tem simples = PROBLEMA
            score_regras -= 0.15  # Penalidade severa
        elif tipo_edital == 'simples' and tipo_portfolio == 'combo':
            # Edital quer simples, portfólio tem combo = Aceitável (pode atender)
            score_regras += 0.05
        else:
            # Ambos simples
            score_regras += 0.10

        # Regra 3.2: Validação de concentração
        if concs_edital and item_portfolio['concentracoes']:
            match_conc = self._validar_concentracoes(concs_edital, item_portfolio['concentracoes'])
            score_regras += match_conc * 0.10
        else:
            score_regras += 0.05  # Sem dados de concentração = neutro

        # Score total
        score_total = score_cobertura + score_similaridade + score_regras

        # Normalizar para 0-1
        return max(0.0, min(1.0, score_total))

    def _match_componente(self, comp1, comp2):
        """Verifica se dois componentes são equivalentes (direto ou via sinônimo)."""
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
            return comp1[:5] == comp2[:5]  # Fallback simples

    def _validar_concentracoes(self, concs_edital, concs_portfolio):
        """Valida compatibilidade de concentrações."""
        if not concs_edital or not concs_portfolio:
            return 0.5  # Sem dados

        matches = 0
        for chave, valor_edital in concs_edital.items():
            if chave in concs_portfolio:
                valor_portfolio = concs_portfolio[chave]
                # Tolerância de 10%
                if valor_portfolio == 0:
                    continue
                diff = abs(valor_edital - valor_portfolio) / valor_portfolio
                if diff <= 0.10:
                    matches += 1

        total = len(concs_edital)
        return matches / total if total > 0 else 0.5

    def _classificar_score(self, score):
        """Classifica score em categorias."""
        if score >= THRESHOLD_ALTO:
            return 'ALTO'
        elif score >= THRESHOLD_MEDIO:
            return 'MEDIO'
        elif score >= THRESHOLD_BAIXO:
            return 'BAIXO'
        else:
            return 'INCOMPATIVEL'

    def _consolidar_resultados(self, resultados):
        """Consolida resultados duplicados (mesmo item em múltiplos lotes)."""
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
        """Gera texto explicativo do match."""
        comps_port = item_portfolio['componentes_normalizados']
        return f"Edital:{comps_edital} ↔ Portfolio:{comps_port}"


def processar_licitacao_wrapper(args):
    """Wrapper para processamento paralelo."""
    licitacao, portfolio_data = args

    # Reconstruir objetos (necessário para multiprocessing)
    portfolio = PortfolioIndexado()
    portfolio.indice_componentes = defaultdict(list, portfolio_data['indice'])
    portfolio.items_completos = portfolio_data['items']

    matcher = MatcherHibrido(portfolio)
    return matcher.avaliar_licitacao(licitacao)


def main():
    """Função principal."""
    logger.info("🚀 Iniciando avaliação de portfólio v3 (Híbrida)")
    logger.info(f"⚙️ Thresholds: ALTO≥{THRESHOLD_ALTO}, MEDIO≥{THRESHOLD_MEDIO}, BAIXO≥{THRESHOLD_BAIXO}")

    # 1. Carregar e indexar portfólio
    portfolio = PortfolioIndexado().carregar_portfolio()

    # 2. Carregar licitações
    licitacoes = []
    arquivos_licitacoes = list(Path('.').glob('licitacoes_*.json*'))

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
            else:
                licitacoes.append(dados)
        except Exception as e:
            logger.error(f"Erro ao carregar {arquivo}: {e}")

    logger.info(f"📋 {len(licitacoes)} licitações carregadas")

    if not licitacoes:
        logger.warning("Nenhuma licitação encontrada para avaliar")
        return

    # 3. Processar licitações
    resultados_finais = []

    # Serial para debug, paralelo para produção
    if len(licitacoes) < 10:
        # Modo serial para poucas licitações
        matcher = MatcherHibrido(portfolio)
        for lic in licitacoes:
            matches = matcher.avaliar_licitacao(lic)
            if matches:
                resultados_finais.append({
                    'licitacao': lic.get('id', 'N/A'),
                    'matches': matches
                })
    else:
        # Modo paralelo
        # Preparar dados serializáveis
        portfolio_data = {
            'indice': dict(portfolio.indice_componentes),
            'items': portfolio.items_completos
        }

        tarefas = [(lic, portfolio_data) for lic in licitacoes]

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(processar_licitacao_wrapper, t): i 
                      for i, t in enumerate(tarefas)}

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    matches = future.result()
                    if matches:
                        resultados_finais.append({
                            'licitacao': licitacoes[idx].get('id', f'idx_{idx}'),
                            'matches': matches
                        })
                except Exception as e:
                    logger.error(f"Erro processando licitação {idx}: {e}")

    # 4. Gerar relatório
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_saida = f"relatorio_compatibilidade_v3_{timestamp}.csv"

    # CSV
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
    logger.info(f"📊 Total de licitações com matches: {len(resultados_finais)}")


if __name__ == "__main__":
    main()
