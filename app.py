import requests
import json
import os
import unicodedata
import gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

ARQDADOS = 'dadosoportunidades.json.gz'
ARQCHECKPOINT = 'checkpoint.txt'
MAXWORKERS = 3

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data):
    return data.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1)
    s.mount('https://', HTTPAdapter(max_retries=retries))
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

def buscar_todos_resultados(session, cnpj, ano, seq):
    resultados = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 100}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            resultados.extend(lista)
            if len(lista) < 100: break
            pag += 1
        except: break
    return resultados

def e_pharma(lic):
    """Filtro pharma INTELIGENTE"""
    obj = lic.get('objetoCompra') or lic.get('objeto', '')
    obj_norm = normalize(obj)
    uf = lic.get('unidadeOrgao', {}).get('ufSigla', '').upper()
    
    # BLOQUEIA SUL + Norte extremo
    if uf in ['PR','SC','RS','AP','AC']: return False
    
    # NORDESTE: mais flexível
    if uf in ['AL','BA','CE','MA','PB','PE','PI','RN','SE']:
        pharma_terms = ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','MATERIAL MEDICO','DIETA','LU VAS','ALCOOL','SERINGA']
        return any(t in obj_norm for t in pharma_terms)
    
    # DEMAIS: só pharma pura
    pharma_pura = ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','REMEDIO','FARMACEUTICO']
    return any(t in obj_norm for t in pharma_pura)

def processar_licitacao(lic, session):
    try:
        if not e_pharma(lic): return None
        
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
        print(f"💊 {unid.get('ufSigla','??')} - {lic.get('objetoCompra','')[:60]}")
        
        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itensraw: return None
        
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)

        return {
            'id': f"{cnpj}{ano}{seq}",
            'dataPub': lic.get('dataPublicacaoPncp'),
            'dataEnc': lic.get('dataEncerramentoProposta'),
            'uf': unid.get('ufSigla'),
            'cidade': unid.get('municipioNome'),
            'orgao': lic['orgaoEntidade']['razaoSocial'],
            'unidadeCompradora': unid.get('nomeUnidade'),
            'objeto': lic.get('objetoCompra') or lic.get('objeto'),
            'editaln': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic.get('valorTotalEstimado') or 0),
            'itensraw': itensraw,
            'resultadosraw': resultadosraw
        }
    except: return None

if __name__ == '__main__':
    print("🚀 SNIPER PHARMA v2.0")
    
    hoje = datetime.now()
    session = criar_sessao()
    banco = {}
    
    # Carrega existentes
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.load(f)}
        except: pass

    data_alvo = hoje - timedelta(days=1)
    dstr = formatar_data_pncp(data_alvo)
    print(f"🎯 Dia alvo: {data_alvo.strftime('%Y-%m-%d')} (API: {dstr})")

    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    
    print("\n📡 BUSCANDO PREGÕES...")
    pag = 1; total_capturados = 0
    
    while pag <= 5:  # Máx 5 páginas teste
        params = {
            'dataInicial': dstr,
            'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,
            'pagina': pag,
            'tamanhoPagina': 50
        }
        
        print(f"\n📄 Página {pag}...")
        r = session.get(url_pub, params=params, timeout=30)
        
        if r.status_code != 200:
            print(f"❌ STATUS {r.status_code}")
            break
            
        dados = r.json()
        lics = dados.get('data', [])
        print(f"📊 {len(lics)} pregões totais na página")
        
        if not lics: break
        
        # FILTRA PHARMA LOCALMENTE
        pharma_lics = [lic for lic in lics if e_pharma(lic)]
        print(f"💊 {len(pharma_lics)} PHARMA encontrados!")
        
        # PROCESSA PARALELO
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
            for futuro in concurrent.futures.as_completed(futuros):
                res = futuro.result()
                if res:
                    banco[res['id']] = res
                    total_capturados += 1

        if len(lics) < 50: break
        pag += 1

    # SALVA
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    with open(ARQCHECKPOINT, 'w') as f:
        f.write(data_alvo.strftime('%Y-%m-%d'))

    print(f"\n🎉 FINALIZADO: {total_capturados} pregões PHARMA salvos!")
