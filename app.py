import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import csv
import re
import concurrent.futures
import time
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_TEMP = ARQDADOS + '.tmp'
ARQ_CHECKPOINT = 'checkpoint.json'
ARQ_LOCK = 'execucao.lock'
ARQ_DICIONARIO = 'dicionario_ouro.json'
ARQ_LOG = 'log_captura.txt'
MAXWORKERS = 15
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- API OFICIAL PNCP ---
API_BASE = "https://pncp.gov.br/api/pncp/v1"
API_CONSULTA = "https://pncp.gov.br/api/consulta/v1"

# --- GEOGRAFIA ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
# Regiões Permitidas para Medicamentos: NE + SE + CO + AM + PA + TO
UFS_PERMITIDAS_MED = NE_ESTADOS + ['DF', 'ES', 'MG', 'RJ', 'SP', 'GO', 'MT', 'MS', 'AM', 'PA', 'TO', 'BR', '']
# Regiões Permitidas para MMH/DIETAS: Apenas NE + DF
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']
# Bloqueio Total: SUL + AP, RO, AC
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- LISTAS DE VETOS E INTERESSES ---
VETOS_TI = [normalize(x) for x in ["SOFTWARE", "SISTEMA", "IMPLANTACAO", "LICENCA", "COMPUTADOR", "INFORMATICA", "IMPRESSAO"]]
VETOS_SERVICOS = [normalize(x) for x in ["PRESTACAO DE SERVICO", "TERCEIRIZACAO", "LOCACAO", "ASSINATURA", "MANUTENCAO", "LIMPEZA", "EXAME", "RADIOLOGIA", "IMAGEM", "VIGILANCIA", "SEGURANCA", "OFICINA"]]
VETOS_OUTROS = [normalize(x) for x in ["GASES MEDICINAIS", "OXIGENIO", "ODONTOLOGICO", "PROTESE", "ORTESE", "CILINDRO", "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "ALIMENTACAO ESCOLAR", "MERENDA"]]
VETOS_ABSOLUTOS = VETOS_TI + VETOS_SERVICOS + VETOS_OUTROS

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA"]]
WL_NUTRI_MMH = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "DIETA ENTERAL", "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "GAZE", "SONDA", "LUVA"]]
WL_TERMOS_VAGOS = [normalize(x) for x in ["SAUDE", "HOSPITAL", "MATERNIDADE", "CLINICA", "FUNDO MUNICIPAL", "SECRETARIA DE"]]

def log(msg, console=True):
    timestamp = datetime.now().strftime('%H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    if console: print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f: f.write(linha + '\n')

def carregar_termos_portfolio():
    if os.path.exists(ARQ_DICIONARIO):
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    return set()

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/24.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw):
    obj = normalize(obj_raw)
    for v in VETOS_ABSOLUTOS:
        if v in obj: return True
    return False

def safe_float(val):
    if val is None: return 0.0
    try:
        val_str = str(val).strip().replace('R$', '').replace(' ', '')
        if ',' in val_str: val_str = val_str.replace('.', '').replace(',', '.')
        res = float(val_str)
        if res > 100000 and res == int(res): res /= 100.0
        return res
    except: return 0.0

def buscar_itens_oficial(cnpj, ano, seq, session):
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    try:
        r = session.get(url, params={'pagina': 1, 'tamanhoPagina': 500}, timeout=20)
        if r.status_code == 200:
            data = r.json()
            return data.get('data', []) if isinstance(data, dict) else data, 'ok'
    except: pass
    return None, 'erro_api'

def processar_licitacao(lic, session, termos_ouro, forcado=False):
    try:
        cnpj = lic.get('orgaoEntidade', {}).get('cnpj', '0000')
        ano = str(lic.get('anoCompra', '0000'))
        seq = str(lic.get('sequencialCompra', '0000'))
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or '').upper().strip() or 'BR'
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()

        if not forcado:
            # 1. Bloqueio Geográfico Total (SUL/Parte do Norte)
            if uf in ESTADOS_BLOQUEADOS: return ('IGNORADO_GEO', None, 0, 'estado_bloqueado')
            
            # 2. Veto Absoluto (Software/Serviços/Gases)
            if veta_edital(obj_raw): return ('VETADO', None, 0, 'veto_absoluto')

            # 3. Classificação de Interesse
            tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
            tem_mmh = any(t in obj_norm for t in WL_NUTRI_MMH)
            tem_vago = any(t in obj_norm for t in WL_TERMOS_VAGOS)

            precisa_checar_itens = False
            
            if tem_med:
                if uf not in UFS_PERMITIDAS_MED: return ('IGNORADO_GEO', None, 0, 'fora_regiao_med')
            elif tem_mmh:
                if uf not in UFS_PERMITIDAS_MMH: return ('IGNORADO_GEO', None, 0, 'fora_regiao_mmh')
            elif tem_vago:
                precisa_checar_itens = True
            else:
                return ('IGNORADO_TEMATICA', None, 0, 'sem_interesse')

        # Busca de Itens
        itens_brutos, fonte = buscar_itens_oficial(cnpj, ano, seq, session)
        if not itens_brutos: return ('ERRO_ITENS', None, 0, fonte)

        # ✅ Opção A: Checagem contra Dicionário de Ouro
        teve_match = False
        itens_mapeados = []
        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            match_termo = next((t for t in termos_ouro if t in desc_item), None)
            if match_termo: teve_match = True
            
            itens_mapeados.append({
                'n': it.get('numeroItem'), 'd': it.get('descricao', ''),
                'q': safe_float(it.get('quantidade')), 'u': it.get('unidadeMedida', 'UN'),
                'v_est': safe_float(it.get('valorUnitarioEstimado')), 'sit': 'EM ANDAMENTO'
            })

        if precisa_checar_itens and not teve_match:
            return ('IGNORADO_PORTFOLIO', None, 0, 'sem_produtos_no_edital_vago')

        dados = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf,
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'),
            'obj': obj_raw, 'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'itens': itens_mapeados, 'sit_global': 'DIVULGADA'
        }
        return ('CAPTURADO', dados, len(itens_mapeados), 'ok')
    except: return ('ERRO', None, 0, 'excecao')

def buscar_periodo(session, banco, d_ini, d_fim):
    termos_ouro = carregar_termos_portfolio()
    log(f"🚀 Sniper Pharma iniciado | 🧠 Dicionário: {len(termos_ouro)} termos")
    
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        log(f"📅 DATA: {dia}")
        pag = 1
        while True:
            r = session.get(f"{API_CONSULTA}/contratacoes/publicacao", params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
            if r.status_code != 200: break
            lics = r.json().get('data', [])
            if not lics: break
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session, termos_ouro) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    st, d, i_qtd, info = f.result()
                    if st == 'CAPTURADO' and d: banco[d['id']] = d
            
            if len(lics) < 50: break
            pag += 1

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=6)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        
        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        buscar_periodo(session, banco, dt_start, dt_end)
        with gzip.open(ARQ_TEMP, 'wt', encoding='utf-8') as f: json.dump(list(banco.values()), f, ensure_ascii=False)
        os.replace(ARQ_TEMP, ARQDADOS)
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
