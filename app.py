import requests
import json
import os
import urllib3
import unicodedata
import gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQDADOS = 'dadosoportunidades.json.gz'
ARQCHECKPOINT = 'checkpoint.txt'
MAXWORKERS = 5

# CONFIG PHARMA
UFS_NE = 'AL,BA,CE,MA,PB,PE,PI,RN,SE'
UFS_MEDICAMENTOS = 'AL,BA,CE,MA,PB,PE,PI,RN,SE,ES,MG,RJ,SP,GO,MT,MS,DF,TO,PA,AM,RO'

PALAVRAS_NE_ESPECIAIS = 'material médico dieta enteral formula luvas alcool luva procedimento'
PALAVRAS_MEDICAMENTOS = 'medicamento farmacia insumo farmaceutico dosagem remedio pharma'

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data):
    """PNCP exige YYYYMMDD sem hífen"""
    return data.strftime('%Y%m%d')

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
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 100}, timeout=30)
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
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 100}, timeout=30)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            resultados.extend(lista)
            if len(lista) < 100: break
            pag += 1
        except: break
    return resultados

def processar_licitacao(lic, session):
    try:
        obj = lic.get('objetoCompra') or lic.get('objeto', '')
        obj_norm = normalize(obj)
        uf = lic.get('unidadeOrgao', {}).get('ufSigla', '').upper()
        
        if uf in ['PR', 'SC', 'RS', 'AP', 'AC']: return None
        
        if uf in ['AL','BA','CE','MA','PB','PE','PI','RN','SE']:
            if not any(t in obj_norm for t in ['MEDICAMENTO','FARMACIA','MATERIAL MEDICO','DIETA','LU VAS','ALCOOL']):
                return None
        else:
            if not any(t in obj_norm for t in ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO']):
                return None

        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
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
            'unidadeCompradora': unid.get('nomeUnidade', 'No Informada'),
            'objeto': obj,
            'editaln': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic.get('valorTotalEstimado') or 0),
            'itensraw': itensraw,
            'resultadosraw': resultadosraw
        }
    except: return None

if __name__ == '__main__':
    print("🚀 SNIPER PHARMA INICIADO")
    
    hoje = datetime.now()
    session = criar_sessao()
    banco = {}
    
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.load(f)}
            print(f"📦 {len(banco)} pregões carregados")
        except: pass

    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQCHECKPOINT):
        with open(ARQCHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y-%m-%d')
            except: pass

    # ✅ FORMATO CORRIGIDO: YYYYMMDD
    dstr = formatar_data_pncp(data_alvo)
    print(f"🎯 Capturando pregões de {data_alvo.strftime('%Y-%m-%d')} (API: {dstr})")

    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'

    # 🟢 NORDESTE
    print("\n🟢=== NORDESTE ESPECIAIS ===")
    pag = 1
    while True:
        params = {
            'dataInicial': dstr,  # 20260214 ✅
            'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,
            'palavrasChave': PALAVRAS_NE_ESPECIAIS,
            'uf': UFS_NE,
            'pagina': pag,
            'tamanhoPagina': 50
        }
        r = session.get(url_pub, params=params, timeout=30)
        print(f"STATUS: {r.status_code} (Pg {pag})")
        
        if r.status_code != 200: 
            print(f"❌ ERROR {r.status_code}")
            break
            
        dados = r.json()
        lics = dados.get('data', [])
        print(f"📄 {len(lics)} pregões NE")
        
        if not lics: break
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: 
                    banco[res['id']] = res
                    print(f"✅ +1 {res['uf']}")

        if pag >= dados.get('totalPaginas', 1): break
        pag += 1

    # 🔵 MEDICAMENTOS
    print("\n🔵=== MEDICAMENTOS ===")
    pag = 1
    while True:
        params = {
            'dataInicial': dstr,
            'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,
            'palavrasChave': PALAVRAS_MEDICAMENTOS,
            'uf': UFS_MEDICAMENTOS,
            'pagina': pag,
            'tamanhoPagina': 50
        }
        r = session.get(url_pub, params=params, timeout=30)
        print(f"STATUS: {r.status_code} (Pg {pag})")
        
        if r.status_code != 200: break
            
        dados = r.json()
        lics = dados.get('data', [])
        print(f"📄 {len(lics)} pregões MED")
        
        if not lics: break
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: 
                    banco[res['id']] = res
                    print(f"✅ +1 {res['uf']}")

        if pag >= dados.get('totalPaginas', 1): break
        pag += 1

    # SALVA
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    with open(ARQCHECKPOINT, 'w') as f:
        f.write(data_alvo.strftime('%Y-%m-%d'))

    print(f"\n🎉 FINALIZADO: {len(banco)} pregões pharma salvos!")
