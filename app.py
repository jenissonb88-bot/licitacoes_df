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
    """Remove acentos e uppercase"""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data):
    """PNCP exige YYYYMMDD"""
    return data.strftime('%Y%m%d')

def criar_sessao():
    """Sessão com retry automático"""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    """Busca TODOS itens paginados"""
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
    """Busca TODOS resultados paginados"""
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
    """Filtro inteligente PHARMA por região"""
    obj = lic.get('objetoCompra') or lic.get('objeto', '')
    obj_norm = normalize(obj)
    uf = lic.get('unidadeOrgao', {}).get('ufSigla', '').upper()
    
    # ❌ BLOQUEIA SUL + NORTE EXTREMO
    if uf in ['PR','SC','RS','AP','AC']: 
        return False
    
    # 🟢 NORDESTE: mais flexível (materiais + pharma)
    if uf in ['AL','BA','CE','MA','PB','PE','PI','RN','SE']:
        termos = ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','MATERIAL MEDICO','DIETA','LU VAS','ALCOOL','SERINGA','VACINA']
        return any(t in obj_norm for t in termos)
    
    # 🔵 DEMAIS: só pharma pura
    termos_puros = ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','REMEDIO','FARMACEUTICO','MANIPULACAO']
    return any(t in obj_norm for t in termos_puros)

def processar_licitacao(lic, session):
    """Extrai detalhes completos (itens + resultados)"""
    try:
        if not e_pharma(lic): return None
        
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
        print(f"💊 [{unid.get('ufSigla','??')}] {lic.get('objetoCompra','')[:70]}")
        
        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itensraw: 
            print(f"⚠️  Sem itens")
            return None
            
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)
        
        return {
            'id': f"{cnpj}{ano}{seq}",
            'dataPub': lic.get('dataPublicacaoPncp'),
            'dataEnc': lic.get('dataEncerramentoProposta'),
            'uf': unid.get('ufSigla'),
            'cidade': unid.get('municipioNome'),
            'orgao': lic['orgaoEntidade']['razaoSocial'],
            'unidadeCompradora': unid.get('nomeUnidade', 'Não Informada'),
            'objeto': lic.get('objetoCompra') or lic.get('objeto', ''),
            'editaln': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic.get('valorTotalEstimado') or 0),
            'itensraw': itensraw,
            'resultadosraw': resultadosraw
        }
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None

if __name__ == '__main__':
    print("🚀 SNIPER PHARMA v2.1 - CAPTURA COMPLETA")
    
    hoje = datetime.now()
    session = criar_sessao()
    banco = {}
    
    # Carrega dados existentes
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.load(f)}
            print(f"📦 {len(banco)} pregões carregados")
        except Exception as e:
            print(f"⚠️ Erro carregando: {e}")

    # Data alvo (ontem)
    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQCHECKPOINT):
        try:
            with open(ARQCHECKPOINT, 'r') as f:
                data_alvo = datetime.strptime(f.read().strip(), '%Y-%m-%d').date()
                data_alvo = datetime.combine(data_alvo, datetime.min.time())
        except: pass

    dstr = formatar_data_pncp(data_alvo)
    print(f"🎯 Dia alvo: {data_alvo.strftime('%Y-%m-%d')} (API: {dstr})")

    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    
    print("\n📡 CAPTURANDO TODAS PÁGINAS DO DIA...")
    pag = 1
    total_capturados = 0
    total_paginas = 0

    while True:
        params = {
            'dataInicial': dstr,
            'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,  # Pregão Eletrônico
            'pagina': pag,
            'tamanhoPagina': 50
        }
        
        print(f"\n📄 Lendo página {pag}...")
        r = session.get(url_pub, params=params, timeout=30)
        
        if r.status_code != 200:
            print(f"❌ STATUS {r.status_code}")
            break
            
        dados = r.json()
        lics = dados.get('data', [])
        total_paginas = dados.get('totalPaginas', pag) or 999
        
        print(f"📊 Pg {pag}/{total_paginas}: {len(lics)} pregões totais")
        
        if not lics:
            print("✅ Sem mais dados")
            break
        
        # FILTRA PHARMA LOCALMENTE (evita erro 422)
        pharma_lics = [lic for lic in lics if e_pharma(lic)]
        print(f"💊 {len(pharma_lics)} PHARMA encontrados!")
        
        # Mostra quais achou
        for lic in pharma_lics:
            uf = lic.get('unidadeOrgao', {}).get('ufSigla', '??')
            obj = lic.get('objetoCompra', '')[:60]
            print(f"   💊 {uf} - {obj}")

        # PROCESSA DETALHES (itens + resultados)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
            for futuro in concurrent.futures.as_completed(futuros):
                res = futuro.result()
                if res:
                    if res['id'] not in banco:  # Evita duplicatas
                        banco[res['id']] = res
                        total_capturados += 1
                        print(f"✅ SALVO: {res['uf']} - {res['editaln']} (R$ {res['valorGlobalApi']:,.0f})")

        # PARA quando acabar
        if len(lics) < 50 or pag >= total_paginas:
            print(f"\n✅ COMPLETO: {pag}/{total_paginas} páginas processadas")
            break
            
        pag += 1

    # SALVA COMPACTADO
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False, separators=(',', ':'))

    # CHECKPOINT PRÓXIMO DIA
    with open(ARQCHECKPOINT, 'w') as f:
        f.write((data_alvo - timedelta(days=1)).strftime('%Y-%m-%d'))

    print(f"\n🎉 FINALIZADO!")
    print(f"💾 {len(banco)} pregões TOTAL no banco")
    print(f"🆕 {total_capturados} pregões NOVOS salvos hoje!")
