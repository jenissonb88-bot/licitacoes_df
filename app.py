import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import concurrent.futures
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES FILTRADAS ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_DICIONARIO = 'dicionario_ouro.json'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 10 # Reduzido levemente para estabilidade no GitHub

# --- GEOGRAFIA (Mantida a sua regra) ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
UFS_PERMITIDAS_MED = NE_ESTADOS + ['DF', 'ES', 'MG', 'RJ', 'SP', 'GO', 'MT', 'MS', 'AM', 'PA', 'TO', 'BR', '']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# Vetos Consolidados (Mais rápido)
VETOS_ABSOLUTOS = [normalize(x) for x in [
    "SOFTWARE", "SISTEMA", "LICENCA", "INFORMATICA", "PRESTACAO DE SERVICO", 
    "LOCACAO", "MANUTENCAO", "GASES MEDICINAIS", "OXIGENIO", "OBRAS", "CONSTRUCAO"
]]

def carregar_termos_portfolio():
    if os.path.exists(ARQ_DICIONARIO):
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    return set()

def criar_sessao():
    s = requests.Session()
    s.mount('https://', HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.3)))
    return s

def processar_licitacao(lic, session, termos_ouro):
    try:
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or 'BR').upper().strip()
        
        # 1. Filtro Geográfico e Vetos de Objeto
        if uf in ESTADOS_BLOQUEADOS: return None
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)
        if any(v in obj_norm for v in VETOS_ABSOLUTOS): return None

        # 2. Busca de Itens (API Oficial)
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        res = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens", params={'pagina': 1, 'tamanhoPagina': 500}, timeout=15)
        if res.status_code != 200: return None
        itens_brutos = res.json().get('data', [])

        # 3. Cruzamento Sniper (Dicionário de Ouro)
        teve_match = False
        itens_mapeados = []
        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            # Só marca match se o termo do dicionário estiver na descrição do item
            if any(termo in desc_item for termo in termos_ouro):
                teve_match = True
            
            # Minificação de chaves para poupar memória
            itens_mapeados.append({
                'n': it.get('numeroItem'),
                'd': it.get('descricao', ''),
                'q': it.get('quantidade'),
                'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0),
                'sit': 'EM ANDAMENTO'
            })

        # Se não tem nenhum item do seu portfólio, ignora o edital (Limpeza total)
        if not teve_match: return None

        return {
            'id': f"{cnpj}{ano}{seq}",
            'dt_enc': lic.get('dataEncerramentoProposta'),
            'uf': uf,
            'org': lic['orgaoEntidade']['razaoSocial'],
            'obj': obj_raw,
            'edit': f"{lic.get('numeroCompra')}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'itens': itens_mapeados,
            'sit_global': 'DIVULGADA'
        }
    except: return None

def buscar_periodo(session, banco, d_ini, d_fim):
    termos_ouro = carregar_termos_portfolio()
    print(f"🚀 Sniper Iniciado | Dicionário: {len(termos_ouro)} termos")
    
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        print(f"📅 Processando: {dia}")
        
        r = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao", params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': 1, 'tamanhoPagina': 50})
        if r.status_code == 200:
            lics = r.json().get('data', [])
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session, termos_ouro) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: banco[res['id']] = res

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start'); parser.add_argument('--end')
        args = parser.parse_args()
        
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=6)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        
        session = criar_sessao()
        banco = {}
        
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                dados_velhos = json.load(f)
                for x in dados_velhos: banco[x['id']] = x

        buscar_periodo(session, banco, dt_start, dt_end)
        
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
            
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
