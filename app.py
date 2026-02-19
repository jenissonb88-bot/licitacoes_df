import requests, json, os, unicodedata, gzip, argparse, sys, csv, re, concurrent.futures
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_CATALOGO = 'Exportar Dados.csv'
ARQ_MANUAL = 'links_manuais.txt'
MAXWORKERS = 15
DATA_CORTE_FIXA = datetime(2026, 1, 1)

# VETO ABSOLUTO PRIORITÁRIO
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- DICIONÁRIOS ---
VETOS_IMEDIATOS = [normalize(x) for x in ["PRESTACAO DE SERVICO", "SERVICO DE ENGENHARIA", "LOCACAO", "INSTALACAO", "MANUTENCAO", "UNIFORME", "TEXTIL", "REFORMA", "LIMPEZA PREDIAL", "LAVANDERIA", "IMPRESSAO", "CONSULTORIA", "TREINAMENTO", "VIGILANCIA", "PORTARIA", "RECEPCAO", "EVENTOS", "BUFFET", "SONDAGEM", "GEOLOGIA", "OBRAS", "PAVIMENTACAO"]]
WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO"]]
WL_MATERIAIS_NUTRI = [normalize(x) for x in ["MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS", "MASCARA", "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO", "DIETA", "NUTRICAO CLINICA"]]

CATALOGO = set()
if os.path.exists(ARQ_CATALOGO):
    try:
        with open(ARQ_CATALOGO, 'r', encoding='latin-1') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader, None)
            for row in reader:
                if len(row) > 2:
                    for termo in [row[0], row[2]]:
                        n = normalize(termo)
                        if len(n) > 3: CATALOGO.add(n)
    except: pass

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/14.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def processar_licitacao(lic, session, forcado=False):
    try:
        uo = lic.get('unidadeOrgao', {})
        uf = uo.get('ufSigla', '').upper()
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        obj_norm = normalize(obj_raw)
        
        if not forcado:
            # 1. BARREIRA GEOGRÁFICA
            if uf in ESTADOS_BLOQUEADOS: return ('VETADO', None, 0, 0)
            
            # 2. FILTRO DE DATA
            dt_enc_str = lic.get('dataEncerramentoProposta')
            if not dt_enc_str: return ('ERRO', None, 0, 0)
            dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)
            
            # 3. VETOS DE RUÍDO
            if any(v in obj_norm for v in VETOS_IMEDIATOS): return ('VETADO', None, 0, 0)
            
            # 4. PERTINÊNCIA BÁSICA
            tem_interesse = False
            if any(t in obj_norm for t in WL_MEDICAMENTOS): tem_interesse = True
            elif uf in NE_ESTADOS and any(t in obj_norm for t in WL_MATERIAIS_NUTRI): tem_interesse = True
            elif "SAUDE" in obj_norm or "HOSPITAL" in obj_norm: tem_interesse = True

            if not tem_interesse: return ('IGNORADO', None, 0, 0)

        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        if r.status_code != 200: return ('ERRO', None, 0, 0)
        
        itens_raw = r.json().get('data', []) if isinstance(r.json(), dict) else r.json()
        itens_brutos = []
        tem_catalogo = forcado
        
        for it in itens_raw:
            if not isinstance(it, dict): continue
            desc = it.get('descricao', '')
            desc_norm = normalize(desc)
            
            if any(v in desc_norm for v in ["ARROZ", "FEIJAO", "PNEU", "GASOLINA"]): continue
            if any(c in desc_norm for c in CATALOGO) or str(it.get('ncmNbsCodigo','')).startswith('30'):
                tem_catalogo = True
                
            sit_id = int(it.get('situacaoCompraItem') or 1)
            
            itens_brutos.append({
                'n': it.get('numeroItem'), 'd': desc, 'q': float(it.get('quantidade') or 0),
                'u': it.get('unidadeMedida', 'UN'), 'v_est': float(it.get('valorUnitarioEstimado') or 0),
                'benef': int(it.get('tipoBeneficioId') or 4), # Valor Original da API
                'sit': {1:"EM ANDAMENTO",2:"HOMOLOGADO",3:"CANCELADO",4:"DESERTO",5:"FRACASSADO"}.get(sit_id, "EM ANDAMENTO"), 
                'res_forn': None, 'res_val': 0.0
            })

        if not itens_brutos: return ('IGNORADO', None, 0, 0)
        if not forcado and uf not in NE_ESTADOS and not tem_catalogo and not any(m in obj_norm for m in WL_MEDICAMENTOS):
            return ('IGNORADO', None, 0, 0)

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': lic.get('dataEncerramentoProposta'), 'uf': uf, 
            'uasg': uo.get('codigoUnidade'), 'org': lic['orgaoEntidade']['razaoSocial'], 
            'unid_nome': uo.get('nomeUnidade'), 'cid': uo.get('municipioNome'), 'obj': obj_raw, 
            'edit': f"{lic.get('numeroCompra')}/{ano}", 'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': float(lic.get('valorTotalEstimado') or 0), 'itens': itens_brutos
        }
        return ('CAPTURADO', dados_finais, len(itens_brutos), 0)
    except: return ('ERRO', None, 0, 0)

# (Funções buscar_periodo, inclusoes_manuais e main permanecem iguais à estrutura anterior)
