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
import time

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 10  # Aumentei para compensar as múltiplas requisições de itens

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data_obj):
    return data_obj.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    # Retry ajustado para ser resiliente mas não travar muito
    retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
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

def buscar_resultado_item(session, cnpj, ano, seq, seq_item):
    """
    Estratégia Cirúrgica: Busca o resultado direto no endpoint do item.
    É mais lento, mas é 100% confiável.
    """
    url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{seq_item}/resultados/1'
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            return r.json() # Retorna o objeto do vencedor direto
    except: pass
    return None

def e_pharma_saude(lic):
    obj = normalize(lic.get('objetoCompra') or lic.get('objeto', ''))
    unid = normalize(lic.get('unidadeOrgao', {}).get('nomeUnidade', ''))
    
    termos = [
        'MEDICAMENT', 'FARMAC', 'HOSPITAL', 'SAUDE', 'ODONTO', 'ENFERMAGEM', 
        'MATERIAL MEDICO', 'INSUMO', 'LUVA', 'SERINGA', 'AGULHA', 'LABORATORI', 
        'FRALDA', 'ABSORVENTE', 'REMEDIO', 'SORO', 'MMH', 'EXPEDIENTE', 'ESPORTIVO'
    ]
    
    if any(t in obj for t in termos): return True
    if any(t in unid for t in ['SAUDE', 'HOSPITAL', 'FUNDO MUNICIPAL']): return True
    return False

def processar_licitacao(lic_resumo, session):
    try:
        if not e_pharma_saude(lic_resumo): return None
        
        cnpj = lic_resumo['orgaoEntidade']['cnpj']
        ano = lic_resumo['anoCompra']
        seq = lic_resumo['sequencialCompra']
        
        # 1. Busca Itens
        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itens_raw: return None

        itens_limpos = []
        
        # 2. Processa cada item e busca resultado se existir
        for item in itens_raw:
            try:
                num = int(item.get('numeroItem'))
                # O SEGREDO: Usar o ID interno para buscar o resultado
                seq_item = item.get('sequencialItem') 
                tem_res = item.get('temResultado', False)
                
                # Captura ME/EPP
                bid = item.get('tipoBeneficio') or item.get('tipoBeneficioId') or 4
                if isinstance(bid, dict): bid = bid.get('value') or bid.get('id', 4)
                
                # --- BUSCA CIRÚRGICA DE RESULTADO ---
                res_data = None
                if tem_res:
                    res_data = buscar_resultado_item(session, cnpj, ano, seq, seq_item)
                
                # Define Situação
                sit_txt = str(item.get('situacaoCompraItemName', '')).upper()
                status_final = "ABERTO"
                
                if res_data: 
                    status_final = "HOMOLOGADO"
                elif any(x in sit_txt for x in ["CANCELADO", "FRACASSADO", "DESERTO"]): 
                    status_final = sit_txt

                # Monta Objeto Slim
                item_obj = {
                    'n': num, 
                    'd': item.get('descricao', ''), 
                    'q': float(item.get('quantidade', 0)),
                    'u': item.get('unidadeMedida', ''), 
                    'v_est': float(item.get('valorUnitarioEstimado', 0)),
                    'benef': bid, 
                    'sit': status_final
                }

                # Se achou resultado, salva os dados do vencedor
                if res_data:
                    item_obj['res_forn'] = res_data.get('nomeRazaoSocialFornecedor') or res_data.get('razaoSocial')
                    item_obj['res_val'] = float(res_data.get('valorUnitarioHomologado', 0))

                itens_limpos.append(item_obj)
            except: continue

        unid = lic_resumo.get('unidadeOrgao', {})
        
        return {
            'id': f"{cnpj}{ano}{seq}", 
            'dt_pub': lic_resumo.get('dataPublicacaoPncp'),
            'dt_enc': lic_resumo.get('dataEncerramentoProposta'),
            'dt_upd_pncp': lic_resumo.get('dataAtualizacao'),
            'uf': unid.get('ufSigla'), 
            'cid': unid.get('municipioNome'), 
            'org': lic_resumo['orgaoEntidade']['razaoSocial'],
            'unid_nome': unid.get('nomeUnidade', 'Não Informada'), 
            'obj': lic_resumo.get('objetoCompra') or lic_resumo.get('objeto', ''),
            'edit': f"{str(lic_resumo.get('numeroCompra', '')).zfill(5)}/{ano}", 
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': float(lic_resumo.get('valorTotalEstimado') or 0),
            'itens': itens_limpos, 
            'timestamp': datetime.now().isoformat()
        }
    except: return None

def buscar_dia_completo(session, data_obj, banco):
    dstr = formatar_data_pncp(data_obj)
    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    total_capturados = 0
    pag = 1
    print(f"🔎 Varrendo {dstr}...")
    
    while True:
        params = {
            'dataInicial': dstr, 'dataFinal': dstr, 
            'codigoModalidadeContratacao': 6, 
            'pagina': pag, 'tamanhoPagina': 50
        }
        try:
            r = session.get(url_pub, params=params, timeout=30)
            if r.status_code != 200: break
            payload = r.json()
            lics = payload.get('data', [])
            total_paginas = payload.get('totalPaginas', 1)
            
            if not lics: break
            pharma_lics = [l for l in lics if e_pharma_saude(l)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
                for futuro in concurrent.futures.as_completed(futuros):
                    res = futuro.result()
                    if res:
                        banco[res['id']] = res
                        total_capturados += 1
                        n_res = sum(1 for i in res['itens'] if 'res_forn' in i)
                        # Log simplificado para não poluir
                        print(f"   SALVO: {res['uf']} - {res['edit']} | Homologados: {n_res}")
            
            if pag >= total_paginas: break
            pag += 1
        except: break
    return total_capturados

if __name__ == '__main__':
    print(f"🚀 SNIPER PHARMA V4.3 (Cirúrgico)")
    
    # TRAVA DE SEGURANÇA
    if os.path.exists(ARQ_LOCK):
        print("⚠️ Trava de execução encontrada. Abortando.")
        sys.exit(0)
    
    with open(ARQ_LOCK, 'w') as f: f.write(datetime.now().isoformat())
    
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        session = criar_sessao()
        banco = {}
        
        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    d = json.load(f)
                    banco = {i['id']: i for i in d}
            except: pass

        datas = []
        if args.start and args.end:
            dt_i = datetime.strptime(args.start, '%Y-%m-%d').date()
            dt_f = datetime.strptime(args.end, '%Y-%m-%d').date()
            for i in range((dt_f - dt_i).days + 1): datas.append(dt_i + timedelta(days=i))
        else:
            # Padrão: Hoje + 5 dias para trás
            for i in range(6): datas.append(date.today() - timedelta(days=i))
            
        total = 0
        for dia in datas: 
            total += buscar_dia_completo(session, dia, banco)
            
        if total > 0:
            print(f"\n💾 Salvando alterações...")
            with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
                json.dump(list(banco.values()), f, ensure_ascii=False)
        else:
            print("\n💤 Nenhuma atualização necessária.")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
