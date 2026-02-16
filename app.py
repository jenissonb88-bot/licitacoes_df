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
DATA_CORTE_FIXA = datetime(2025, 12, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
EXT_ESTADOS = ['ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- LEI DO VETO ---
VETOS_ABSOLUTOS = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "BUFFET", "EVENTOS", "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "EXAME LABORATORI", "OBRAS", "PNEU"]]
RESGATES_MEDICOS = [normalize(x) for x in ["PROCEDIMENTO", "CIRURGICO", "ESTERIL", "EXAME", "70%", "GEL", "HOSPITALAR"]]
VETOS_CONTEXTUAIS = [normalize(x) for x in ["JARDINAGEM", "COZINHA", "DOMESTICA", "LIMPEZA"]]

# --- WHITELISTS ---
WL_GLOBAL_MEDS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO"]]
WL_NE_MATS = [normalize(x) for x in ["MMH", "INSUMO", "MATERIAL MEDIC", "DIETA", "NUTRI", "EQUIPO", "SONDA"]]
WL_SAUDE = [normalize(x) for x in ["SAUDE", "HOSPITAL", "FMS", "FARMACIA"]]

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    if any(v in obj for v in VETOS_ABSOLUTOS):
        if uf in NE_ESTADOS and any(x in obj for x in ["DIETA", "FORMULA"]): return False
        return True
    if any(x in obj for x in ["LUVA", "ALCOOL", "LIMPEZA"]):
        if any(v in obj for v in VETOS_CONTEXTUAIS):
            if not any(r in obj for r in RESGATES_MEDICOS): return True
    return False

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def processar_licitacao(lic, session):
    try:
        obj_raw = lic.get('objetoCompra') or ""
        uf = (lic.get('unidadeOrgao', {}).get('ufSigla') or '').upper()
        
        # Log de análise inicial
        # print(f" 👀 Analisando: {obj_raw[:60]}... ({uf})")

        dt_enc_str = lic.get('dataEncerramentoProposta')
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: return None

        # Veto Supremo
        if veta_edital(obj_raw, uf):
            print(f" 🚫 VETADO: {obj_raw[:50]}...")
            return None

        # Filtro de Interesse
        obj_norm = normalize(obj_raw)
        interesses = WL_GLOBAL_MEDS + WL_SAUDE + (WL_NE_MATS if uf in NE_ESTADOS else [])
        if not any(t in obj_norm for t in interesses): return None

        # Captura de Itens
        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        r_itens = session.get(f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens', params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        itens_raw = r_itens.json().get('data', []) if r_itens.status_code == 200 else []
        if not itens_raw: return None

        itens_limpos = []
        homologados = 0
        for it in itens_raw:
            num = it.get('numeroItem')
            res = None
            if it.get('temResultado'):
                try:
                    r_res = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados", timeout=15)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        res = rl[0] if isinstance(rl, list) and rl else rl
                        if res: homologados += 1
                except: pass

            itens_limpos.append({
                'n': num, 'd': it.get('descricao', ''), 'q': float(it.get('quantidade', 0)),
                'u': it.get('unidadeMedida', ''), 'v_est': float(it.get('valorUnitarioEstimado', 0)),
                'benef': it.get('tipoBeneficioId') or 4,
                'sit': "HOMOLOGADO" if res else str(it.get('situacaoCompraItemName', 'ABERTO')).upper(),
                'res_forn': (res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')) if res else None,
                'res_val': float(res.get('valorUnitarioHomologado') or 0) if res else 0
            })

        print(f" ✅ CAPTURADO: {obj_raw[:50]}... | 📦 {len(itens_limpos)} itens | 🏆 {homologados} homolog.")
        
        return {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': lic['unidadeOrgao'].get('codigoUnidade', '---'),
            'org': lic['orgaoEntidade']['razaoSocial'], 'unid_nome': lic['unidadeOrgao'].get('nomeUnidade', '---'),
            'cid': lic['unidadeOrgao'].get('municipioNome'), 'obj': obj_raw, 
            'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': float(lic.get('valorTotalEstimado') or 0), 'itens': itens_limpos
        }
    except Exception as e: 
        return None

def buscar_periodo(session, banco, d_ini, d_fim):
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
            
            print(f" 📄 Página {pag} de {dados.get('totalPaginas', 1)} | Analisando {len(lics)} editais...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: banco[res['id']] = res
            
            if pag >= dados.get('totalPaginas', 1): break
            pag += 1
