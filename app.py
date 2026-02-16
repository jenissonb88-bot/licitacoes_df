import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# --- CONFIGURAÇÕES E FILTROS DE BARREIRA ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 10 
DATA_CORTE_FIXA = datetime(2026, 1, 1)

ESTADOS_ALVO = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', 'ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

BLACKLIST = [''.join(c for c in unicodedata.normalize('NFD', x).upper() if unicodedata.category(c) != 'Mn') for x in [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "OBRAS", "ENGENHARIA", "CONSTRUCAO", "REFORMA", "PINTURA", "FROTA", 
    "PECAS PARA CARRO", "PNEU", "COMBUSTIVEL", "LIMPEZA PREDIAL", "AR CONDICIONADO", "INFORMATICA", "COMPUTADOR", 
    "SOFTWARE", "TONER", "CARTUCHO", "IMPRESSORA", "MOBILIARIO", "ESTANTE", "CADEIRA", "MESA", "PAPELARIA", 
    "EXPEDIENTE", "FARDAMENTO", "UNIFORME", "CONFECCAO", "COPA", "COZINHA", "ALIMENTAR", "MERENDA", "COFFEE BREAK", 
    "AGUA MINERAL", "GELO", "KIT LANCHE", "ESPORTIVO", "BRINQUEDO", "EVENTOS", "SHOW", "PALCO", "SEGURANCA", 
    "VIGILANCIA", "LOCACAO", "ASSESSORIA", "CONSULTORIA", "TREINAMENTO", "CURSO", "FUNERARIO", "GASES MEDICINAIS", 
    "OXIGENIO", "REFEICAO", "RESTAURANTE", "HOSPEDAGEM"
]]

WHITELIST_PHARMA = [''.join(c for c in unicodedata.normalize('NFD', x).upper() if unicodedata.category(c) != 'Mn') for x in [
    "MEDICAMENTO", "REMEDIO", "FARMACO", "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", "ANTI-INFLAMAT", 
    "ANTIBIOTIC", "ANTIDEPRESSIV", "ANSIOLITIC", "DIABETIC", "GLICEMIC", "SORO", "FRALDA", "ABSORVENTE", "MMH", "MATERIAL MEDICO"
]]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def processar_licitacao(lic, session):
    try:
        # 1. Filtro de Data
        dt_enc_str = lic.get('dataEncerramentoProposta')
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: return None

        # 2. Filtro de UF
        uf = (lic.get('unidadeOrgao', {}).get('ufSigla') or '').upper()
        if uf not in ESTADOS_ALVO: return None

        # 3. Filtro de Objeto (Blacklist vs Whitelist)
        obj = normalize(lic.get('objetoCompra') or "")
        
        # Se cair na Blacklist e não for Dieta/Nutrição, descarta
        if any(t in obj for t in BLACKLIST):
            if not any(t in obj for t in ["DIETA", "FORMULA", "NUTRICIONAL", "ENTERAL"]):
                return None
        
        # Se não tiver nada da Whitelist, descarta
        if not any(t in obj for t in WHITELIST_PHARMA):
            return None

        # Se passou pelos filtros, busca os itens
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        # (Lógica de busca de itens e resultados mantida conforme versão anterior...)
        # [AQUI VAI O RESTANTE DA FUNÇÃO PROCESSAR_LICITACAO DA V4.7]
        # ...
        return item_processado # (Exemplo de retorno)
    except: return None
