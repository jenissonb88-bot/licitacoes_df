import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 8 

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    """Lê todos os itens de uma licitação, não importa a quantidade."""
    itens = []
    pag = 1
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 100}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            # A API pode retornar lista direta ou objeto com campo 'data'
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 100: break
            pag += 1
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    """Busca os vencedores (resultados) oficiais."""
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    try:
        r = session.get(url, timeout=20)
        if r.status_code == 200:
            dados = r.json()
            return dados.get('data', []) if isinstance(dados, dict) else dados
    except: pass
    return []

def processar_licitacao(lic, session):
    try:
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
        return {
            "id": f"{cnpj}{ano}{seq}",
            "data_pub": lic.get('dataPublicacaoPncp'),
            "data_enc": lic.get('dataEncerramentoProposta'),
            "uf": unid.get('ufSigla'),
            "cidade": unid.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'],
            "unidade_compradora": unid.get('nomeUnidade', 'Não Informada'), # CONFORME MANUAL
            "objeto": lic.get('objetoCompra'),
            "edital_n": f"{str(lic.get('numeroCompra')).zfill(5)}/{ano}",
            "uasg": unid.get('codigoUnidade', '---'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "itens_raw": buscar_todos_itens(session, cnpj, ano, seq),
            "resultados_raw": buscar_todos_resultados(session, cnpj, ano, seq)
        }
    except: return None

if __name__ == "__main__":
    hoje = datetime.now()
    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    session = criar_sessao()
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): banco[i['id']] = i
        except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": d_str, "dataFinal": d_str, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50}
    
    r = session.get(url_pub, params=params, timeout=25)
    if r.status_code == 200:
        lics = r.json().get('data', [])
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: banco[res['id']] = res

    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    with open(ARQ_CHECKPOINT, 'w') as f: f.write((data_alvo + timedelta(days=1)).strftime('%Y%m%d'))
