import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import traceback
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 10 
DATA_CORTE_FIXA = datetime(2025, 12, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
EXT_ESTADOS = ['ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- LEI DO VETO SUPREMO ---
VETOS_ABSOLUTOS = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "BUFFET", "EVENTOS", "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "EXAME LABORATORI", "OBRAS", "PNEU", "VEICULO"]]
RESGATES_MEDICOS = [normalize(x) for x in ["PROCEDIMENTO", "CIRURGICO", "ESTERIL", "EXAME", "70%", "GEL", "HOSPITALAR", "LATEX", "NITRILICA"]]
VETOS_CONTEXTUAIS = [normalize(x) for x in ["JARDINAGEM", "COZINHA", "DOMESTICA", "LIMPEZA", "PVC", "RASPA", "VAQUETA"]]

# --- WHITELISTS ---
WL_GLOBAL_MEDS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", "COMPRIMIDO", "INJETAVEL"]]
WL_GENERICO_SAUDE = [normalize(x) for x in ["SAUDE", "HOSPITAL", "FMS", "FARMACIA", "UNIDADE BASICA"]]
WL_NE_MATS = [normalize(x) for x in ["MMH", "INSUMO", "MATERIAL MEDIC", "DIETA", "NUTRI", "EQUIPO", "SONDA", "LUVA", "SERINGA"]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    if any(v in obj for v in VETOS_ABSOLUTOS):
        if uf in NE_ESTADOS and any(x in obj for x in ["DIETA", "FORMULA"]): return False
        return True
    if any(x in obj for x in ["LUVA", "ALCOOL", "LIMPEZA"]):
        if any(v in obj for v in VETOS_CONTEXTUAIS):
            if not any(r in obj for r in RESGATES_MEDICOS): return True
    return False

def safe_float(val):
    try:
        if val is None: return 0.0
        return float(val)
    except:
        return 0.0

def processar_licitacao(lic, session):
    try:
        if not isinstance(lic, dict): return ('ERRO_FMT', None, 0, 0)
        
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        
        # Proteção contra unidadeOrgao nula ou mal formatada
        uo = lic.get('unidadeOrgao')
        if not isinstance(uo, dict): uo = {}
        uf = uo.get('ufSigla', '').upper()
        
        dt_enc_str = lic.get('dataEncerramentoProposta')
        if not dt_enc_str: return ('ERRO_DATA', None, 0, 0)
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO_DATA', None, 0, 0)

        if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 0)

        # Filtro de Interesse
        obj_norm = normalize(obj_raw)
        tem_interesse = False
        if any(t in obj_norm for t in WL_GLOBAL_MEDS + WL_GENERICO_SAUDE):
            tem_interesse = True
        elif uf in NE_ESTADOS and any(t in obj_norm for t in WL_NE_MATS):
            tem_interesse = True
            
        if not tem_interesse: return ('IGNORADO_INTERESSE', None, 0, 0)

        # Captura Itens
        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r_itens = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        
        if r_itens.status_code != 200: return ('ERRO_API', None, 0, 0)
        itens_raw = r_itens.json().get('data', [])
        if not itens_raw: return ('IGNORADO_VAZIO', None, 0, 0)

        itens_limpos = []
        homologados = 0
        for it in itens_raw:
            # BLINDAGEM: Pula item se não for dicionário
            if not isinstance(it, dict): continue

            num = it.get('numeroItem')
            res = None
            if it.get('temResultado'):
                try:
                    r_res = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados", timeout=15)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        if isinstance(rl, list):
                            res = rl[0] if len(rl) > 0 else None
                        else:
                            res = rl
                        
                        # BLINDAGEM: Garante que res seja dict
                        if not isinstance(res, dict): res = None
                        
                        if res: homologados += 1
                except: pass

            itens_limpos.append({
                'n': num, 
                'd': it.get('descricao', ''), 
                'q': safe_float(it.get('quantidade')),
                'u': it.get('unidadeMedida', ''), 
                'v_est': safe_float(it.get('valorUnitarioEstimado')),
                'benef': it.get('tipoBeneficioId') or 4,
                'sit': "HOMOLOGADO" if res else str(it.get('situacaoCompraItemName', 'ABERTO')).upper(),
                'res_forn': (res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')) if res else None,
                'res_val': safe_float(res.get('valorUnitarioHomologado')) if res else 0.0
            })

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': lic['unidadeOrgao'].get('codigoUnidade', '---'),
            'org': lic['orgaoEntidade']['razaoSocial'], 'unid_nome': lic['unidadeOrgao'].get('nomeUnidade', '---'),
            'cid': lic['unidadeOrgao'].get('municipioNome'), 'obj': obj_raw, 
            'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': safe_float(lic.get('valorTotalEstimado')), 
            'itens': itens_limpos
        }
        
        return ('CAPTURADO', dados_finais, len(itens_limpos), homologados)

    except Exception as e:
        return ('ERRO_FATAL', str(e), 0, 0)

def buscar_periodo(session, banco, d_ini, d_fim):
    stats_geral = {'vetados': 0, 'capturados': 0, 'itens': 0, 'homologados': 0, 'sem_interesse': 0, 'erros': 0}
    erros_amostra = []

    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        print(f"\n📅 --- DATA: {dia} ---")
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
        pag = 1
        while True:
            r = session.get(url, params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
            if r.status_code != 200: break
            dados = r.json(); lics = dados.get('data', [])
            if not lics: break
            
            total_paginas = dados.get('totalPaginas', 1)
            stats_pag = {'vetados': 0, 'capturados': 0, 'itens': 0, 'homologados': 0, 'sem_interesse': 0, 'erros': 0}

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    status, dados_lic, qtd_i, qtd_h = f.result()
                    
                    if status == 'CAPTURADO':
                        stats_pag['capturados'] += 1
                        stats_pag['itens'] += qtd_i
                        stats_pag['homologados'] += qtd_h
                        if dados_lic: banco[dados_lic['id']] = dados_lic
                    elif status == 'VETADO':
                        stats_pag['vetados'] += 1
                    elif status.startswith('IGNORADO'):
                        stats_pag['sem_interesse'] += 1
                    else: 
                        stats_pag['erros'] += 1
                        # Guarda amostra do erro
                        if len(erros_amostra) < 5 and dados_lic:
                            erros_amostra.append(dados_lic) # dados_lic aqui é a msg de erro
            
            for k in stats_geral: stats_geral[k] += stats_pag[k]
            print(f"   📄 Pág {pag}/{total_paginas}: 🎯 {stats_pag['capturados']} Caps | 🚫 {stats_pag['vetados']} Vetos | 👁️ {stats_pag['sem_interesse']} Ignorados | 🔥 {stats_pag['erros']} Erros")
            
            if pag >= total_paginas: break
            pag += 1

    print("\n" + "="*50)
    print("📊 RESUMÃO GERAL")
    print("="*50)
    print(f"✅ PREGÕES COMPATÍVEIS:  {stats_geral['capturados']}")
    print(f"🚫 PREGÕES VETADOS:      {stats_geral['vetados']}")
    print(f"🔥 ERROS TÉCNICOS:       {stats_geral['erros']}")
    if erros_amostra:
        print(f"\n🐛 AMOSTRA DE ERROS: {erros_amostra}")
    print("="*50 + "\n")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=2)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        session = criar_sessao(); banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f); banco = {x['id']: x for x in d}
        buscar_periodo(session, banco, dt_start, dt_end)
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
