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

# Configurações
ARQDADOS = 'dadosoportunidades.json.gz'
MAXWORKERS = 3

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data_obj):
    if isinstance(data_obj, date):
        return data_obj.strftime('%Y%m%d')
    return data_obj.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
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
            time.sleep(0.3)
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    resultados = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 50}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            resultados.extend(lista)
            if len(lista) < 50: break
            pag += 1
            time.sleep(0.3)
        except: break
    return resultados

def e_pharma(lic):
    obj = lic.get('objetoCompra') or lic.get('objeto', '')
    obj_norm = normalize(obj)
    
    termos_gatilho = [
        'MEDICAMENT', 'FARMAC', 'HOSPITAL', 'SAUDE', 'ODONTO', 'ENFERMAGEM',
        'MATERIAL MEDICO', 'INSUMO', 'LUVA', 'SERINGA', 'AGULHA', 'LABORATORI',
        'FRALDA', 'ABSORVENTE', 'REMEDIO', 'SORO', 
        'HIPERTENSIV', 'INJETAV', 'ONCOLOGIC', 'ANALGESIC', 
        'ANTI-INFLAMAT', 'ANTIBIOTIC', 'ANTIDEPRESSIV', 
        'ANSIOLITIC', 'DIABETIC', 'GLICEMIC', 'CONTROLAD',
        'MATERIAL PENSO', 'DIETA', 'FORMULA', 'PROTEIC', 
        'CALORIC', 'GAZE', 'ATADURA'
    ]
    
    return any(t in obj_norm for t in termos_gatilho)

def precisa_atualizar(lic_atual, lic_nova):
    # Se não temos a licitação no banco, claro que precisa salvar
    if not lic_atual: return True
    
    # 1. Critério Forte: Data de Atualização do PNCP mudou?
    # O PNCP manda 'dataAtualizacao' na busca. Se for diferente do que temos, mudou algo.
    dt_atual_salva = lic_atual.get('dataAtualizacaoPncp')
    dt_nova_api = lic_nova.get('dataAtualizacaoPncp') # Pegamos isso no processar_licitacao
    
    if dt_nova_api and dt_atual_salva:
        if dt_nova_api != dt_atual_salva:
            return True

    # 2. Critério de Fallback (Segurança): Número de resultados aumentou?
    if len(lic_nova.get('resultadosraw', [])) > len(lic_atual.get('resultadosraw', [])):
        return True
        
    return False

def processar_licitacao(lic, session):
    try:
        if not e_pharma(lic): return None
        
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
        # Estrutura Base
        dados_tratados = {
            'id': f"{cnpj}{ano}{seq}",
            'dataPub': lic.get('dataPublicacaoPncp'),
            'dataEnc': lic.get('dataEncerramentoProposta'),
            # Captura a data de atualização oficial do registro
            'dataAtualizacaoPncp': lic.get('dataAtualizacao'), 
            'uf': unid.get('ufSigla'),
            'cidade': unid.get('municipioNome'),
            'orgao': lic['orgaoEntidade']['razaoSocial'],
            'unidadeCompradora': unid.get('nomeUnidade', 'Não Informada'),
            'objeto': lic.get('objetoCompra') or lic.get('objeto', ''),
            'editaln': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic.get('valorTotalEstimado') or 0),
            'itensraw': [],
            'resultadosraw': [],
            'ultimaAtualizacao': datetime.now().isoformat()
        }
        
        # Busca Pesada (Itens e Resultados)
        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itensraw: return None 
        dados_tratados['itensraw'] = itensraw
        
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)
        dados_tratados['resultadosraw'] = resultadosraw
        
        return dados_tratados
    except: return None

def buscar_dia_completo(session, data_obj, banco):
    if isinstance(data_obj, date):
        data_obj = datetime.combine(data_obj, datetime.min.time())
    
    dstr = formatar_data_pncp(data_obj)
    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    total_capturados = 0
    
    pag = 1
    while True:
        params = {
            'dataInicial': dstr, 'dataFinal': dstr,
            'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50
        }
        
        print(f"📄 {data_obj.strftime('%Y-%m-%d')} - Pg {pag}...")
        try:
            r = session.get(url_pub, params=params, timeout=30)
            if r.status_code != 200: break
            dados = r.json()
            lics = dados.get('data', [])
            total_paginas = dados.get('totalPaginas', pag) or 999
            
            if not lics: break
            
            pharma_lics = [lic for lic in lics if e_pharma(lic)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
                for futuro in concurrent.futures.as_completed(futuros):
                    try:
                        res = futuro.result()
                        if res:
                            lic_banco = banco.get(res['id'])
                            if precisa_atualizar(lic_banco, res):
                                banco[res['id']] = res
                                total_capturados += 1
                                n_res = len(res['resultadosraw'])
                                status = "ATUALIZADO" if lic_banco else "NOVO"
                                print(f"✅ {status}: {res['uf']} - {res['editaln']} (Res: {n_res})")
                    except: pass

            if len(lics) < 50 or pag >= total_paginas: break
            pag += 1
            time.sleep(1)
        except Exception as e:
            print(f"Erro paginação: {e}")
            break
    
    return total_capturados

if __name__ == '__main__':
    print(f"🚀 SNIPER PHARMA V-APP 2.2 (Sincronização Real)")
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
    args = parser.parse_args()

    session = criar_sessao()
    banco = {}
    
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f)
                banco = {i['id']: i for i in (d if isinstance(d, list) else [])}
            print(f"📦 Carregados: {len(banco)}")
        except: pass

    datas = []
    if args.start and args.end:
        dt_ini = datetime.strptime(args.start, '%Y-%m-%d').date()
        dt_fim = datetime.strptime(args.end, '%Y-%m-%d').date()
        delta = dt_fim - dt_ini
        for i in range(delta.days + 1): datas.append(dt_ini + timedelta(days=i))
    else:
        datas.append(date.today() - timedelta(days=1))

    total = 0
    for dia in datas:
        print(f"\n🔄 Processando {dia}...")
        total += buscar_dia_completo(session, dia, banco)

    if total > 0 or datas:
        print("\n💾 Salvando...")
        if "/" in ARQDADOS: os.makedirs(os.path.dirname(ARQDADOS), exist_ok=True)
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
            
    print("🏁 Fim.")
