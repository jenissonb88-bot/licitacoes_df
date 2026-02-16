import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import csv
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import time

# --- CONFIGURAÇÕES E TRAVAS DE SEGURANÇA ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
ARQCSV = 'Exportar Dados.csv'
MAXWORKERS = 10 

# DATA DE CORTE FIXA: Impede a entrada de dados anteriores a 01/12/2025
DATA_CORTE_FIXA = datetime(2025, 12, 1)

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CATÁLOGO (BARREIRA DE PRECISÃO) ---
catalogo_df = set()
if os.path.exists(ARQCSV):
    for enc in ['latin-1', 'utf-8', 'cp1252']:
        try:
            with open(ARQCSV, 'r', encoding=enc) as f:
                leitor = csv.reader(f)
                next(leitor, None)
                for linha in leitor:
                    if linha:
                        for i in [0, 1, 5]: # Descrição, Fármaco, Nome Técnico
                            if len(linha) > i:
                                termo = normalize(linha[i])
                                if len(termo) > 4: catalogo_df.add(termo)
            print(f"📦 Catálogo CSV carregado: {len(catalogo_df)} termos.")
            break
        except: continue

# --- 2. FILTROS DE BLOQUEIO (BARREIRA CONTRA LIXO) ---
BLACKLIST = [normalize(x) for x in [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "OBRAS", "ENGENHARIA", "CONSTRUCAO",
    "REFORMA", "PINTURA", "FROTA", "PECAS PARA CARRO", "PNEU", "COMBUSTIVEL",
    "AR CONDICIONADO", "INFORMATICA", "MOBILIARIO", "PAPELARIA", "FARDAMENTO", 
    "ALIMENTAR", "MERENDA", "COFFEE BREAK", "AGUA MINERAL", "EVENTOS", "SHOW",
    "VIGILANCIA", "LOCACAO", "ASSESSORIA", "TREINAMENTO", "CURSO", "FUNERARIO"
]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 50}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 50: break
            pag += 1
        except: break
    return itens

def buscar_resultado_item(session, cnpj, ano, seq, num_item):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            res_list = r.json()
            if isinstance(res_list, list) and len(res_list) > 0:
                return res_list[0]
            elif isinstance(res_list, dict):
                return res_list
    except: pass
    return None

def e_pharma_saude(lic):
    obj = normalize(lic.get('objetoCompra') or lic.get('objeto', ''))
    unid = normalize(lic.get('unidadeOrgao', {}).get('nomeUnidade', ''))
    
    # Se cair na Blacklist e não for nutrição, bloqueia na entrada
    if any(t in obj for t in BLACKLIST):
        if not any(t in obj for t in ["DIETA", "FORMULA", "NUTRICIONAL"]):
            return False

    termos = ['MEDICAMENT', 'FARMAC', 'HOSPITAL', 'SAUDE', 'ODONTO', 'ENFERMAGEM', 'MATERIAL MEDICO', 'INSUMO', 'LUVA', 'SERINGA', 'AGULHA', 'LABORATORI', 'FRALDA', 'ABSORVENTE', 'REMEDIO', 'SORO', 'MMH']
    if any(t in obj for t in termos): return True
    if any(t in unid for t in ['SAUDE', 'HOSPITAL', 'FUNDO MUNICIPAL']): return True
    return False

def processar_licitacao(lic_resumo, session):
    try:
        # --- BARREIRA 1: DATA E TÍTULO ---
        dt_enc_str = lic_resumo.get('dataEncerramentoProposta')
        if not dt_enc_str: return None
        
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: return None

        if not e_pharma_saude(lic_resumo): return None

        cnpj = lic_resumo['orgaoEntidade']['cnpj']
        ano = lic_resumo['anoCompra']
        seq = lic_resumo['sequencialCompra']
        obj_norm = normalize(lic_resumo.get('objetoCompra') or "")

        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itens_raw: return None

        # --- BARREIRA 2: VALIDAÇÃO POR ITEM (CSV) ---
        # Se o título não é explicitamente "Medicamento", precisamos achar um item do CSV
        confirmado_pharma = any(t in obj_norm for t in ["MEDICAMENTO", "FARMACO", "REMEDIO"])
        itens_limpos = []
        achou_no_catalogo = False

        for item in itens_raw:
            try:
                desc_item = normalize(item.get('descricao', ''))
                
                # Checa contra o catálogo Drogafonte
                if any(prod in desc_item for prod in catalogo_df):
                    achou_no_catalogo = True

                num_item = item.get('numeroItem')
                bid = item.get('tipoBeneficio') or item.get('tipoBeneficioId') or 4
                if isinstance(bid, dict): bid = bid.get('value') or bid.get('id', 4)
                
                res_data = None
                if item.get('temResultado'):
                    res_data = buscar_resultado_item(session, cnpj, ano, seq, num_item)
                
                sit_txt = str(item.get('situacaoCompraItemName', '')).upper()
                status_final = "ABERTO"
                if res_data: status_final = "HOMOLOGADO"
                elif any(x in sit_txt for x in ["CANCELADO", "FRACASSADO", "DESERTO"]): status_final = sit_txt

                item_obj = {
                    'n': num_item, 'd': item.get('descricao', ''), 'q': float(item.get('quantidade', 0)),
                    'u': item.get('unidadeMedida', ''), 'v_est': float(item.get('valorUnitarioEstimado', 0)),
                    'benef': bid, 'sit': status_final
                }
                if res_data:
                    item_obj['res_forn'] = res_data.get('nomeRazaoSocialFornecedor') or res_data.get('razaoSocial')
                    item_obj['res_val'] = float(res_data.get('valorUnitarioHomologado') or 0)
                itens_limpos.append(item_obj)
            except: continue

        # Decisão Final da Dupla Barreira
        if not confirmado_pharma and not achou_no_catalogo:
            return None # Descarta se não tem Pharma no título nem item no CSV

        unid = lic_resumo.get('unidadeOrgao', {})
        return {
            'id': f"{cnpj}{ano}{seq}", 'dt_pub': lic_resumo.get('dataPublicacaoPncp'),
            'dt_enc': lic_resumo.get('dataEncerramentoProposta'), 'dt_upd_pncp': lic_resumo.get('dataAtualizacao'),
            'uf': unid.get('ufSigla'), 'cid': unid.get('municipioNome'), 'org': lic_resumo['orgaoEntidade']['razaoSocial'],
            'unid_nome': unid.get('nomeUnidade', 'Não Informada'), 'obj': lic_resumo.get('objetoCompra') or lic_resumo.get('objeto', ''),
            'edit': f"{str(lic_resumo.get('numeroCompra', '')).zfill(5)}/{ano}", 'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 'val_tot': float(lic_resumo.get('valorTotalEstimado') or 0),
            'itens': itens_limpos, 'timestamp': datetime.now().isoformat()
        }
    except: return None

def buscar_dia_completo(session, data_obj, banco):
    dstr = data_obj.strftime('%Y%m%d')
    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    total_capturados = 0
    pag = 1
    print(f"🔎 Capturando {dstr}...")
    
    while True:
        params = {'dataInicial': dstr, 'dataFinal': dstr, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}
        try:
            r = session.get(url_pub, params=params, timeout=30)
            if r.status_code != 200: break
            payload = r.json()
            lics = payload.get('data', [])
            if not lics: break
            
            # Aplica o filtro de "Porteiro" inicial
            pharma_lics = [l for l in lics if e_pharma_saude(l)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
                for futuro in concurrent.futures.as_completed(futuros):
                    res = futuro.result()
                    if res:
                        banco[res['id']] = res
                        total_capturados += 1
                        n_res = sum(1 for i in res['itens'] if 'res_forn' in i)
                        print(f"   + {res['uf']} - {res['edit']} | Itens: {len(res['itens'])} | Venc: {n_res}")
            
            if pag >= payload.get('totalPaginas', 1): break
            pag += 1
        except: break
    return total_capturados

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write(datetime.now().isoformat())
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        session = criar_sessao(); banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f); banco = {i['id']: i for i in d}
        
        datas = []
        if args.start and args.end:
            dt_i = datetime.strptime(args.start, '%Y-%m-%d').date()
            dt_f = datetime.strptime(args.end, '%Y-%m-%d').date()
            for i in range((dt_f - dt_i).days + 1): datas.append(dt_i + timedelta(days=i))
        else:
            # Padrão: Últimos 2 dias se não informar data
            for i in range(2): datas.append(date.today() - timedelta(days=i))
        
        for dia in datas: buscar_dia_completo(session, dia, banco)

        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        print(f"✅ Coleta Finalizada. Banco atualizado com {len(banco)} registros.")
            
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
