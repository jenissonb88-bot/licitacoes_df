import json
import re
import os
import sys
import gzip
import logging
import csv
import signal
from pathlib import Path
from datetime import datetime
from collections import defaultdict

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

# ✅ Handler global para capturar crashes
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error(f"💥 ERRO CRÍTICO NÃO CAPTURADO: {exc_value}", exc_info=(exc_type, exc_value, exc_traceback))
    sys.exit(1)

sys.excepthook = handle_exception

# ✅ Timeout handler para evitar travamentos infinitos
class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError("Operação excedeu tempo limite")

# Constantes - ✅ AJUSTADOS para serem mais permissivos
THRESHOLD_ALTO = 0.60      # Reduzido de 0.70
THRESHOLD_MEDIO = 0.40     # Reduzido de 0.50  
THRESHOLD_BAIXO = 0.20     # Reduzido de 0.30

ARQ_LICITACOES = 'pregacoes_pharma_limpos.json.gz'
ARQ_PORTFOLIO = 'Exportar Dados.csv'

# ✅ EXPANDIDO: Dicionário de sinônimos farmacêuticos
SINONIMOS_FARMACOS = {
    # Analgésicos/Antitérmicos
    "DIPIRONA": ["METAMIZOL", "DIPIRONA SODICA", "DIPIRONA MONOIDRATADA", "NOVALGINA"],
    "METAMIZOL": ["DIPIRONA", "NOVALGINA"],
    "PARACETAMOL": ["ACETAMINOFEN", "TYLENOL"],
    "IBUPROFENO": ["ADVIL", "MOTRIN"],
    
    # Antibióticos
    "AMOXICILINA": ["AMOXI", "AMOXICILINA TRIHIDRATADA", "AMOXIL"],
    "CLAVULANATO": ["CLAV", "CLAVULANATO DE POTASSIO", "ÁCIDO CLAVULÂNICO"],
    "AZITROMICINA": ["ZITROMAX", "AZITROMICINA DIIDRATADA"],
    "CIPROFLOXACINO": ["CIPRO", "CIPROXIN"],
    "CEFALEXINA": ["KEFLEX", "CEFALEXINA MONOIDRATADA"],
    
    # Anti-inflamatórios/Corticoides
    "BETAMETASONA": ["BETAMET", "BETAMETASONA", "CELESTONE"],
    "DEXAMETASONA": ["DECADRON", "DEXAMETASONA FOSFATO"],
    "PREDNISONA": ["METICORTEN", "PREDNISONA"],
    "PREDNISOLONA": ["PRELONE", "PREDNISOLONA"],
    
    # Gastrointestinais
    "OMEPRAZOL": ["OMEPRAZOL MAGNESICO", "OMEPRAZOL SODICO", "LOSEC", "PRILOSEC"],
    "ESOMEPRAZOL": ["NEXIUM", "ESOMEPRAZOL MAGNESICO"],
    "PANTOPRAZOL": ["PANTOZOL", "PANTOPRAZOL SODICO"],
    "RANITIDINA": ["zantac", "RANITIDINA CLORIDRATO"],
    "METOCLOPRAMIDA": ["PLASIL", "METOCLOPRAMIDA CLORIDRATO"],
    
    # Antiespasmódicos
    "ESCOPOLAMINA": ["HIOSCINA", "BUSCOPAN", "BUTILBROMETO DE ESCOPOLAMINA", "ESCOPOLAMINA BUTILBROMETO"],
    "HIOSCINA": ["ESCOPOLAMINA", "BUSCOPAN"],
    
    # Antialérgicos
    "LORATADINA": ["CLARITIN", "LORATADINA"],
    "DESLORATADINA": ["AERIUS", "DESLORATADINA"],
    "CETIRIZINA": ["ZYRTEC", "CETIRIZINA DICLORIDRATO"],
    
    # Psiquiatria/Neurologia
    "DIAZEPAM": ["VALIUM", "DIAZEPAM"],
    "CLONAZEPAM": ["RIVOTRIL", "CLONAZEPAM"],
    "ALPRAZOLAM": ["FRONTAl", "ALPRAZOLAM"],
    "SERTRALINA": ["ZOLOFT", "SERTRALINA"],
    "FLUOXETINA": ["PROZAC", "FLUOXETINA"],
    "RISPERIDONA": ["RISPERDAL", "RISPERIDONA"],
    "OLANZAPINA": ["ZYPREXA", "OLANZAPINA"],
    
    # Cardiovasculares
    "ENALAPRIL": ["RENITEc", "ENALAPRIL MALEATO"],
    "CAPTOPRIL": ["CAPOTEN", "CAPTOPRIL"],
    "LOSARTANA": ["COZAAR", "LOSARTANA POTASSICA"],
    "ATENOLOL": ["ATELOCARD", "ATENOLOL"],
    "METOPROLOL": ["LOPRESSOR", "METOPROLOL"],
    "AMLODIPINO": ["NORVASC", "AMLODIPINO"],
    "ATORVASTATINA": ["LIPITOR", "ATORVASTATINA CALCICA"],
    "SINVASTATINA": ["ZOCOR", "SINVASTATINA"],
    
    # Diabetes
    "METFORMINA": ["GLUCOPHAGE", "METFORMINA CLORIDRATO"],
    "GLIBENCLAMIDA": ["DAONIL", "GLIBENCLAMIDA"],
    "GLIMEPIRIDA": ["AMARYL", "GLIMEPIRIDA"],
    "INSULINA": ["NOVORAPID", "HUMALOG", "LANTUS", "INSULINA GLARGINA", "INSULINA ASPART"],
    
    # Outros comuns
    "SULFAMETOXAZOL": ["SULFA", "SULFAMETOXAZOL", "BACTRIM"],
    "TRIMETOPRIMA": ["TRI", "TMP", "BACTRIM"],
    "NISTATINA": ["MICOSTATIN", "NISTATINA"],
    "MICONAZOL": ["MONISTAT", "MICONAZOL"],
    "FLUCONAZOL": ["DIFLUCAN", "FLUCONAZOL"],
}


class PortfolioIndexado:
    def __init__(self):
        self.indice_componentes = defaultdict(list)
        self.items_completos = {}
        # ✅ Limitar tamanho do cache de sinônimos para economizar memória
        self._sinonimos_cache = {}
        self._cache_max_size = 10000

    def carregar_portfolio(self, csv_path=ARQ_PORTFOLIO):
        logger.info(f"📂 Carregando portfólio: {csv_path}")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

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

        for idx, row in enumerate(registros):
            try:
                item = self._enriquecer_item(row, idx)
                self.items_completos[item['id']] = item

                for comp in item['componentes_normalizados']:
                    self.indice_componentes[comp].append(item['id'])

                    # ✅ Limitar expansão de sinônimos para componentes muito comuns
                    if len(self.indice_componentes[comp]) <= 100:  # Só expandir se não for muito comum
                        for sinonimo in self._expandir_sinonimos(comp):
                            if sinonimo != comp:
                                self.indice_componentes[sinonimo].append(item['id'])
            except Exception as e:
                logger.warning(f"Erro ao indexar item {idx}: {e}")

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
        sku = row.get('Código') or row.get('CODIGO') or row.get('codigo') or f'ITEM_{idx}'
        descricao = str(row.get('Descrição') or row.get('DESCRICAO') or row.get('descricao') or '').upper().strip()
        farmaco_raw = str(row.get('Fármaco') or row.get('FARMACO') or row.get('farmaco') or '').upper().strip()
        dosagem = str(row.get('Dosagem') or row.get('DOSAGEM') or row.get('dosagem') or '').upper().strip()
        forma = str(row.get('Forma Farmacêutica') or row.get('FORMA_FARMACEUTICA') or row.get('forma') or '').upper().strip()
        sinonimos_raw = str(row.get('Nomes Técnicos/Sinônimos') or row.get('SINONIMOS') or row.get('sinonimos') or '').upper().strip()

        componentes_desc = self._extrair_componentes(descricao)
        componentes_farm = self._extrair_componentes(farmaco_raw) if farmaco_raw else []
        componentes_sin = self._extrair_componentes(sinonimos_raw) if sinonimos_raw else []

        todos_componentes = []
        for c in componentes_desc + componentes_farm + componentes_sin:
            if c not in todos_componentes:
                todos_componentes.append(c)

        componentes_normalizados = [self._normalizar_componente(c) for c in todos_componentes]

        tipo = 'combo' if len(componentes_normalizados) > 1 else 'simples'

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

        componentes = []
        
        # Padrão 1: Palavras em maiúsculas de 4-20 caracteres (possíveis nomes de fármacos)
        padrao_farmacos = r'\b([A-Z]{4,20})\b'
        matches = re.findall(padrao_farmacos, texto)
        componentes.extend(matches)
        
        # Padrão 2: Nomes com hífen (ex: "SULFAMETOXAZOL-TRIMETOPRIMA")
        padrao_composto = r'\b([A-Z]+-[A-Z]+)\b'
        matches_composto = re.findall(padrao_composto, texto)
        componentes.extend(matches_composto)
        
        # Padrão 3: Separadores clássicos
        separadores = r'[+/&,;]'
        partes = re.split(separadores, texto)
        for parte in partes:
            limpo = re.sub(r'\d+[\d.,/\s]*\s*(MG|ML|G|UI|MCG|UNIDADES?|MG/ML|G/ML)', '', parte, flags=re.I)
            limpo = re.sub(r'\(.*?\)', '', limpo)
            limpo = re.sub(r'\b(C/|COM|X|DE|DA|DO|DOS|DAS|PARA|COMO|E|OU)\b', '', limpo, flags=re.I)
            limpo = limpo.strip()
            if len(limpo) >= 4 and limpo.isalpha():
                componentes.append(limpo)

        # Remover duplicatas preservando ordem
        vistos = set()
        resultado = []
        for c in componentes:
            c_norm = c.strip()
            if c_norm and c_norm not in vistos and len(c_norm) >= 3:
                vistos.add(c_norm)
                resultado.append(c_norm)

        return resultado

    def _normalizar_componente(self, comp):
        comp = comp.strip()
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
        # ✅ Usar cache com limite de tamanho
        if componente in self._sinonimos_cache:
            return self._sinonimos_cache[componente]
        
        expansao = {componente}
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            if componente == principal or componente in sinonimos:
                expansao.add(principal)
                expansao.update(sinonimos)
        
        # ✅ Limitar tamanho do cache
        if len(self._sinonimos_cache) < self._cache_max_size:
            self._sinonimos_cache[componente] = expansao
        
        return expansao

    def _extrair_concentracoes(self, dosagem):
        concentracoes = {}
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
            chaves_busca = {comp_norm}
            chaves_busca.update(self._expandir_sinonimos(comp_norm))

            for chave in chaves_busca:
                if chave in self.indice_componentes:
                    for item_id in self.indice_componentes[chave]:
                        contagem[item_id] += 1
                        componentes_encontrados[item_id].add(comp_norm)

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
        self._debug_count = 0
        self._total_processado = 0

    def avaliar_licitacao(self, licitacao):
        objeto = licitacao.get('objeto') or licitacao.get('obj') or ''
        itens = licitacao.get('itens', [])

        if not itens and objeto:
            itens = [{'descricao': objeto, 'quantidade': 1}]

        resultados = []
        for item_edital in itens:
            try:
                # ✅ Timeout por item para evitar travamentos
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(5)  # 5 segundos por item
                
                matches = self._avaliar_item(item_edital)
                
                signal.alarm(0)  # Cancelar alarme
                
                if matches:
                    resultados.extend(matches)
            except TimeoutError:
                logger.warning(f"⏱️ Timeout ao processar item: {item_edital.get('descricao', 'N/A')[:50]}...")
                signal.alarm(0)
            except Exception as e:
                logger.debug(f"Erro ao avaliar item: {e}")
                signal.alarm(0)

        resultados = self._consolidar_resultados(resultados)
        return resultados

    def _avaliar_item(self, item_edital):
        descricao = str(item_edital.get('descricao') or item_edital.get('d') or '').upper()

        if not descricao or len(descricao) < 3:
            return []

        # ✅ DEBUG: Log das primeiras descrições para ver o que está chegando
        if self._debug_count < 3:
            logger.info(f"   🔍 Descrição item {self._debug_count}: {descricao[:80]}...")
            self._debug_count += 1

        componentes_edital = self._extrair_componentes_edital(descricao)
        
        if not componentes_edital:
            logger.debug(f"Nenhum componente extraído de: {descricao[:50]}...")
            return []
        
        # ✅ DEBUG: Mostrar componentes extraídos
        if self._debug_count <= 3:
            logger.info(f"      Componentes extraídos: {componentes_edital}")

        tipo_edital = 'combo' if len(componentes_edital) > 1 else 'simples'
        concentracoes_edital = self._extrair_concentracoes(descricao)

        # ✅ Limitar número de candidatos para evitar processamento excessivo
        candidatos = self.portfolio.buscar_candidatos(componentes_edital, top_n=20)

        if not candidatos:
            if self._debug_count <= 3:
                logger.info(f"      ❌ Nenhum candidato no índice para: {componentes_edital}")
            return []

        if self._debug_count <= 3:
            logger.info(f"      ✅ Candidatos encontrados: {len(candidatos)}")

        resultados = []
        for candidato in candidatos[:3]:  # ✅ Reduzido para top 3
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
                    if self._debug_count <= 3:
                        logger.info(f"      ⭐ Match: {item_portfolio['id']} score={score:.2%}")
            except Exception as e:
                logger.debug(f"Erro ao calcular score: {e}")

        resultados.sort(key=lambda x: x['score'], reverse=True)
        return resultados

    def _extrair_componentes_edital(self, descricao):
        componentes = []

        # ✅ MELHORADO: Mesma lógica do portfólio para consistência
        
        # Padrão 1: Palavras em maiúsculas de 4-20 caracteres
        padrao_farmacos = r'\b([A-Z][A-Z\s]{3,19})\b'
        matches = re.findall(padrao_farmacos, descricao)
        for m in matches:
            limpo = m.strip()
            if len(limpo) >= 4:
                componentes.append(limpo)

        # Padrão 2: Separadores clássicos
        separadores = r'[+/&,;]'
        partes = re.split(separadores, descricao)
        for parte in partes:
            limpo = re.sub(r'\d+[\d.,/\s]*\s*(MG|ML|G|UI|MCG|UNIDADES?|MG/ML)', '', parte, flags=re.I)
            limpo = re.sub(r'\(.*?\)', '', limpo)
            limpo = re.sub(r'\b(C/|COM|X|DE|DA|DO|DOS|DAS|PARA)\b', '', limpo, flags=re.I)
            limpo = limpo.strip()
            if len(limpo) >= 4:
                # Pegar apenas a parte que parece nome de fármaco (primeiras palavras maiúsculas)
                palavras = limpo.split()
                nome_farmaco = []
                for p in palavras:
                    if p.isalpha() and p.isupper() and len(p) >= 3:
                        nome_farmaco.append(p)
                    elif len(nome_farmaco) > 0:
                        break
                if nome_farmaco:
                    componentes.append(' '.join(nome_farmaco))

        # Remover duplicatas
        vistos = set()
        resultado = []
        for c in componentes:
            c_norm = c.strip()
            if c_norm and c_norm not in vistos:
                vistos.add(c_norm)
                resultado.append(c_norm)

        return resultado

    def _calcular_score_hibrido(self, comps_edital, concs_edital, tipo_edital, 
                                 item_portfolio, matches_indices):
        comps_portfolio = item_portfolio['componentes_normalizados']
        tipo_portfolio = item_portfolio['tipo']

        # 1. COBERTURA DE COMPONENTES (50%)
        comps_edital_norm = [self.portfolio._normalizar_componente(c) for c in comps_edital]
        
        matches_diretos = 0
        for ce in comps_edital_norm:
            for cp in comps_portfolio:
                if self._match_componente(ce, cp):
                    matches_diretos += 1
                    break

        if not comps_edital:
            cobertura = 0
        else:
            cobertura = matches_diretos / len(comps_edital)

        score_cobertura = cobertura * 0.50

        # 2. PRECISÃO DO MATCH (30%)
        if HAS_RAPIDFUZZ and comps_edital and comps_portfolio:
            similaridades = []
            for ce in comps_edital_norm:
                # ✅ Limitar comparações para evitar O(n²) excessivo
                melhores = [fuzz.ratio(ce, cp) for cp in comps_portfolio[:5]]
                if melhores:
                    similaridades.append(max(melhores))
            if similaridades:
                score_similaridade = (sum(similaridades) / len(similaridades) / 100) * 0.30
            else:
                score_similaridade = 0
        else:
            score_similaridade = cobertura * 0.30

        # 3. REGRAS DE NEGÓCIO (20%)
        score_regras = 0

        if tipo_edital == 'combo' and tipo_portfolio == 'combo':
            if cobertura >= 0.8:
                score_regras += 0.15
            else:
                score_regras += 0.05
        elif tipo_edital == 'combo' and tipo_portfolio == 'simples':
            score_regras -= 0.10
        elif tipo_edital == 'simples' and tipo_portfolio == 'combo':
            score_regras += 0.05
        else:
            score_regras += 0.10

        # Validação de concentração (bonus)
        if concs_edital and item_portfolio['concentracoes']:
            match_conc = self._validar_concentracoes(concs_edital, item_portfolio['concentracoes'])
            score_regras += match_conc * 0.05
        else:
            score_regras += 0.05

        score_total = score_cobertura + score_similaridade + score_regras
        return max(0.0, min(1.0, score_total))

    def _match_componente(self, comp1, comp2):
        c1 = comp1.strip()
        c2 = comp2.strip()
        
        if c1 == c2:
            return True

        # ✅ Otimização: verificar tamanho primeiro
        if abs(len(c1) - len(c2)) > 3:
            return False

        # Verificar sinônimos
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            grupo = {principal} | set(sinonimos)
            if c1 in grupo and c2 in grupo:
                return True

        # Similaridade fuzzy - Reduzido threshold para 70
        if HAS_RAPIDFUZZ:
            return fuzz.ratio(c1, c2) > 70
        else:
            return c1[:4] == c2[:4]

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
                if diff <= 0.20:  # Aumentado tolerância para 20%
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

        agrupados = defaultdict(list)
        for r in resultados:
            agrupados[r['item_portfolio']['id']].append(r)

        consolidados = []
        for item_id, matches in agrupados.items():
            melhor = max(matches, key=lambda x: x['score'])
            consolidados.append(melhor)

        consolidados.sort(key=lambda x: x['score'], reverse=True)
        return consolidados

    def _gerar_detalhes(self, comps_edital, item_portfolio):
        comps_port = item_portfolio['componentes_normalizados']
        return f"Edital:{comps_edital} ↔ Portfolio:{comps_port}"


def main():
    logger.info("🚀 Iniciando avaliação de portfólio v3.3 (Otimizado)")
    logger.info(f"⚙️ Thresholds: ALTO≥{THRESHOLD_ALTO}, MEDIO≥{THRESHOLD_MEDIO}, BAIXO≥{THRESHOLD_BAIXO}")

    try:
        portfolio = PortfolioIndexado().carregar_portfolio()
    except Exception as e:
        logger.error(f"💥 Erro ao carregar portfólio: {e}")
        sys.exit(1)

    licitacoes = []
    
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
        sys.exit(1)

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
        sys.exit(1)

    # ✅ DEBUG: Mostrar estrutura da primeira licitação e alguns itens
    if licitacoes:
        primeira = licitacoes[0]
        logger.info(f"🔍 Exemplo licitação: id={primeira.get('id')}")
        logger.info(f"   obj={primeira.get('obj', '')[:100]}...")
        if primeira.get('itens'):
            for i, item in enumerate(primeira['itens'][:3]):
                desc = item.get('d', 'N/A')
                logger.info(f"   Item {i}: {desc[:80]}...")

    # ✅ DEBUG: Mostrar alguns componentes do portfólio
    logger.info(f"🔍 Exemplo componentes no portfólio (primeiros 10):")
    for i, comp in enumerate(list(portfolio.indice_componentes.keys())[:10]):
        logger.info(f"   {comp}: {len(portfolio.indice_componentes[comp])} itens")

    resultados_finais = []
    matcher = MatcherHibrido(portfolio)

    logger.info(f"🔧 Processando em modo serial...")
    
    # ✅ Processar em batches com checkpoint
    batch_size = 100
    total_licitacoes = len(licitacoes)
    
    for batch_start in range(0, total_licitacoes, batch_size):
        batch_end = min(batch_start + batch_size, total_licitacoes)
        logger.info(f"📦 Processando batch {batch_start//batch_size + 1}: {batch_start}-{batch_end-1}")
        
        for idx in range(batch_start, batch_end):
            try:
                lic = licitacoes[idx]
                matches = matcher.avaliar_licitacao(lic)
                if matches:
                    resultados_finais.append({
                        'licitacao': lic.get('id', f'idx_{idx}'),
                        'matches': matches
                    })
                
                matcher._total_processado += 1
                
            except Exception as e:
                logger.error(f"❌ Erro processando licitação {idx}: {e}")
                # ✅ Continuar mesmo com erro em uma licitação
                continue

        # ✅ Log de progresso a cada batch
        progresso = (batch_end / total_licitacoes) * 100
        logger.info(f"   📊 Progresso: {progresso:.1f}% | Matches até agora: {len(resultados_finais)}")
        
        # ✅ Forçar garbage collection a cada batch para liberar memória
        import gc
        gc.collect()

    # Gerar relatório
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_saida = f"relatorio_compatibilidade_{timestamp}.csv"

    try:
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
    except Exception as e:
        logger.error(f"❌ Erro ao gerar relatório: {e}")

    logger.info(f"📊 Licitações com matches: {len(resultados_finais)}/{len(licitacoes)}")
    
    resumo = {'ALTO': 0, 'MEDIO': 0, 'BAIXO': 0, 'INCOMPATIVEL': 0}
    for r in resultados_finais:
        for m in r['matches']:
            resumo[m['tipo_match']] = resumo.get(m['tipo_match'], 0) + 1
    
    logger.info(f"📈 Matches: ALTO={resumo['ALTO']}, MEDIO={resumo['MEDIO']}, BAIXO={resumo['BAIXO']}")

    if len(resultados_finais) == 0:
        logger.warning("⚠️ NENHUM MATCH ENCONTRADO!")
        logger.warning("   Verifique se os nomes dos fármacos no portfólio correspondem às descrições das licitações.")
    
    logger.info("🏁 Processamento concluído com sucesso!")


if __name__ == "__main__":
    main()
