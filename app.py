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

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 10 
DATA_CORTE_FIXA = datetime(2026, 1, 1)

# Estados de atuação Drogafonte
ESTADOS_ALVO = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', 'ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# Palavras que abrem a porta para o robô checar os itens (Whitelist de Entrada)
WHITELIST_SAUDE = [normalize(x) for x in [
    "MEDICAMENTO", "REMEDIO", "FARMACO", "SORO", "SAUDE", "HOSPITAL", "FUNDO MUNICIPAL", 
    "FMS", "FSA", "HOSPITALAR", "MMH", "INSUMO", "DIETA", "FORMULA", "NUTRI", "FARMAC"
]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 100}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 100: break
            pag += 1
        except: break
    return itens

def buscar_resultado_item(session, cnpj, ano, seq, num_item):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            res_list = r.json()
            if isinstance(res_list, list) and len(res_list) > 0: return res_list[0]
            elif isinstance(res_list, dict): return res_list
    except: pass
    return None

def processar_licitacao(lic, session):
    try:
        # REGRA 1: DATA (Corte 2026)
        dt_enc_str = lic.get('dataEncerramentoProposta')
        if not dt_enc_str: return None
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: return None

        # REGRA 2: UF
        unid = lic.get('unidadeOrgao', {})
        uf = (unid.get('ufSigla') or '').upper()
        if uf not in ESTADOS_ALVO: return None

        # REGRA 3: SUSPEITA DE SAÚDE (Porteiro)
        obj = normalize(lic.get('objetoCompra') or "")
        org_nome = normalize(lic.get('orgaoEntidade', {}).get('razaoSocial') or "")
        unid_nome = normalize(unid.get('nomeUnidade') or "")
        
        if not any(t in obj or t in org_nome or t in unid_nome for t in WHITELIST_SAUDE):
            return None

        # SE PASSOU, BUSCA DETALHES
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itens_raw: return None

        itens_limpos = []
        for it in itens_raw:
            try:
                num = it.get('numeroItem')
                res = None
                if it.get('temResultado'):
                    res = buscar_resultado_item(session, cnpj, ano, seq, num)
                
                sit_txt = str(it.get('situacaoCompraItemName', '')).upper()
                status = "ABERTO"
                if res: status = "HOMOLOGADO"
                elif any(x in sit_txt for x in ["CANCELADO", "FRACASSADO", "DESERTO"]): status = sit_txt

                itens_limpos.append({
                    'n': num, 'd': it.get('descricao', ''), 'q': float(it.get('quantidade', 0)),
                    'u': it.get('unidadeMedida', ''), 'v_est': float(it.get('valorUnitarioEstimado', 0)),
                    'benef': it.get('tipoBeneficioId') or it.get('tipoBeneficio', {}).get('id') or 4,
                    'sit': status,
                    'res_forn': (res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')) if res else None,
                    'res_val': float(res.get('valorUnitarioHomologado') or 0) if res else 0
                })
            except: continue

        return {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 'uasg': unid.get('codigoUnidade', '---'),
            'org': lic['orgaoEntidade']['razaoSocial'], 'unid_nome': unid.get('nomeUnidade', 'Não Inf.'),
            'cid': unid.get('municipioNome'), 'obj': lic.get('objetoCompra'), 'edit': lic.get('numeroCompra'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 'val_tot': float(lic.get('valorTotalEstimado') or 0),
            'itens': itens_limpos
        }
    except: return None

def buscar_periodo(session, banco):
    for i in range(3): # Busca os últimos 3 dias
        dia = (date.today() - timedelta(days=i)).strftime('%Y%m%d')
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
        pag = 1
        print(f"🔎 Coletando dia: {dia}")
        while True:
            r = session.get(url, params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
            if r.status_code != 200: break
            dados = r.json()
            lics = dados.get('data', [])
            if not lics: break
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: banco[res['id']] = res
            
            if pag >= dados.get('totalPaginas', 1): break
            pag += 1

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        session = criar_sessao(); banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f); banco = {x['id']: x for x in d}
        
        buscar_periodo(session, banco)

        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        print("✅ Coleta finalizada!")
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
