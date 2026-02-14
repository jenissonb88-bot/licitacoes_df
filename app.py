import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 5 

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []
    pag = 1
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 100}, timeout=30)
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
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    resultados = []
    pag = 1
    while True:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 100}, timeout=30)
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
        
        # MANTENDO TODAS AS INFORMAÇÕES QUE VOCÊ SE ESFORÇOU PARA CAPTURAR
        return {
            "id": f"{cnpj}{ano}{seq}",
            "data_pub": lic.get('dataPublicacaoPncp'),
            "data_enc": lic.get('dataEncerramentoProposta'),
            "uf": unid.get('ufSigla'),
            "cidade": unid.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'], # Órgão Principal
            "unidade_compradora": unid.get('nomeUnidade', 'Não Informada'), # Unidade Compradora
            "objeto": lic.get('objetoCompra') or lic.get('objeto', ''),
            "edital_n": f"{str(lic.get('numeroCompra')).zfill(5)}/{ano}", # Nº do Edital
            "uasg": unid.get('codigoUnidade', '---'), # UASG
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "valor_global_api": float(lic.get('valorTotalEstimado') or 0),
            "itens_raw": buscar_todos_itens(session, cnpj, ano, seq), # Exaustão de itens
            "resultados_raw": buscar_todos_resultados(session, cnpj, ano, seq) # Exaustão de resultados
        }
    except: return None

if __name__ == "__main__":
    hoje = datetime.now()
    session = criar_sessao()
    banco = {}

    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.load(f)}
        except: pass

    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Iniciando captura exaustiva do dia: {d_str}")

    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pag = 1
    while True:
        params = {"dataInicial": d_str, "dataFinal": d_str, "codigoModalidadeContratacao": "6", "pagina": pag, "tamanhoPagina": 50}
        r = session.get(url_pub, params=params, timeout=30)
        if r.status_code != 200: break
        dados = r.json()
        lics = dados.get('data', [])
        if not lics: break

        print(f"📦 Processando página {pag} de {dados.get('totalPaginas')}...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: banco[res['id']] = res
        
        if pag >= dados.get('totalPaginas', 1): break
        pag += 1

    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    with open(ARQ_CHECKPOINT, 'w') as f: f.write((data_alvo + timedelta(days=1)).strftime('%Y%m%d'))
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if (data_alvo + timedelta(days=1)).date() <= hoje.date() else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
