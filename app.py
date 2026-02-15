import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQDADOS = 'dadosoportunidades.json.gz'
ARQCHECKPOINT = 'checkpoint.txt'
MAXWORKERS = 5

# Definições regionais
UFS_NE = 'AL,BA,CE,MA,PB,PE,PI,RN,SE'
UFS_MEDICAMENTOS = 'AL,BA,CE,MA,PB,PE,PI,RN,SE,ES,MG,RJ,SP,GO,MT,MS,DF,TO,PA,AM,RO'
UFS_EXCLUIDAS = 'PR,SC,RS,AP,AC'

PALAVRAS_NE_ESPECIAIS = '"material médico" OR "dieta enteral" OR fórmula OR luvas OR "álcool 70" OR "luva procedimento" OR dietas OR formula'
PALAVRAS_MEDICAMENTOS = 'medicamento OR farmacia OR "insumo farmaceutico" OR "principio ativo" OR dosagem OR remédio OR pharma'

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []
    pag = 1
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
    resultados = []
    pag = 1
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
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})

        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        print(f"Exausto de itens: {len(itensraw)}")
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)
        print(f"Exausto de resultados: {len(resultadosraw)}")

        return {
            'id': f"{cnpj}{ano}{seq}",
            'dataPub': lic.get('dataPublicacaoPncp'),
            'dataEnc': lic.get('dataEncerramentoProposta'),
            'uf': unid.get('ufSigla'),
            'cidade': unid.get('municipioNome'),
            'orgao': lic['orgaoEntidade']['razaoSocial'],
            'unidadeCompradora': unid.get('nomeUnidade', 'No Informada'),
            'objeto': lic.get('objetoCompra') or lic.get('objeto', ''),
            'editaln': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic.get('valorTotalEstimado') or 0),
            'itensraw': itensraw,
            'resultadosraw': resultadosraw
        }
    except Exception as e:
        print(f"Erro processando: {e}")
        return None

if __name__ == '__main__':
    hoje = datetime.now()
    session = criar_sessao()
    banco = {}
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.load(f)}
        except: pass

    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQCHECKPOINT):
        with open(ARQCHECKPOINT, 'r') as f:
            try:
                data_alvo = datetime.strptime(f.read().strip(), '%Y-%m-%d')
            except: pass

    dstr = data_alvo.strftime('%Y-%m-%d')
    print(f"Iniciando captura exaustiva do dia {dstr}")

    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'

    # === BUSCA 1/2: NORDESTE ESPECIAIS ===
    print("=== BUSCA 1/2: NORDESTE ESPECIAIS ===")
    pag = 1
    while True:
        params = {
            'dataInicial': dstr, 'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,
            'palavrasChave': PALAVRAS_NE_ESPECIAIS,
            'uf': UFS_NE,
            'pagina': pag,
            'tamanhoPagina': 50
        }
        r = session.get(url_pub, params=params, timeout=30)
        if r.status_code != 200: break
        dados = r.json()
        lics = dados.get('data', [])
        if not lics: break
        print(f"Processando página {pag} de {dados.get('totalPaginas')} (NE Especial)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: banco[res['id']] = res

        if pag >= dados.get('totalPaginas', 1): break
        pag += 1

    # === BUSCA 2/2: MEDICAMENTOS (TODAS UFs válidas) ===
    print("=== BUSCA 2/2: MEDICAMENTOS (TODAS UFs válidas) ===")
    pag = 1
    while True:
        params = {
            'dataInicial': dstr, 'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,
            'palavrasChave': PALAVRAS_MEDICAMENTOS,
            'uf': UFS_MEDICAMENTOS,
            'pagina': pag,
            'tamanhoPagina': 50
        }
        r = session.get(url_pub, params=params, timeout=30)
        if r.status_code != 200: break
        dados = r.json()
        lics = dados.get('data', [])
        if not lics: break
        print(f"Processando página {pag} de {dados.get('totalPaginas')} (Medicamentos)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: banco[res['id']] = res

        if pag >= dados.get('totalPaginas', 1): break
        pag += 1

    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    with open(ARQCHECKPOINT, 'w') as f:
        f.write((data_alvo - timedelta(days=1)).strftime('%Y-%m-%d'))

    if 'GITHUB_OUTPUT' in os.environ:
        trigger = 'true' if (data_alvo - timedelta(days=1)).date() == hoje.date() else 'false'
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            print(f'triggernext={trigger}', file=f)
