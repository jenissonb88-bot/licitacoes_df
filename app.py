import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 8 # Menor para evitar bloqueio da API em buscas exaustivas

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    """Busca exaustiva de todos os itens (até 10.000 itens)"""
    itens = []
    pag = 1
    while pag <= 200:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 50: break
            pag += 1
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    """Busca exaustiva de todos os vencedores/resultados"""
    resultados = []
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    pag = 1
    while pag <= 200:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=20)
            if r.status_code == 200:
                dados = r.json()
                lista = dados.get('data', []) if isinstance(dados, dict) else dados
                if lista:
                    resultados.extend(lista)
                    if len(lista) < 50: break
                    pag += 1
                    continue
            break
        except: break
    return resultados

def processar_licitacao(lic, session):
    try:
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        id_lic = f"{cnpj}{ano}{seq}"
        
        unid = lic.get('unidadeOrgao', {})
        
        # Coleta exaustiva
        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq)
        
        return {
            "id": id_lic,
            "data_pub": lic.get('dataPublicacaoPncp'),
            "data_enc": lic.get('dataEncerramentoProposta'),
            "uf": unid.get('ufSigla') or lic.get('unidadeFederativaId'),
            "cidade": unid.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'],
            "objeto": lic.get('objetoCompra'),
            "edital_n": f"{lic.get('numeroCompra')}/{ano}",
            "uasg": unid.get('codigoUnidade') or "---",
            "valor_global": float(lic.get('valorTotalEstimado') or 0),
            "is_sigiloso": lic.get('niValorTotalEstimado', False),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "itens_raw": itens_raw,
            "resultados_raw": resultados_raw
        }
    except: return None

if __name__ == "__main__":
    hoje = datetime.now()
    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(hoje.strftime('%Y%m%d'))
    
    with open(ARQ_CHECKPOINT, 'r') as f: data_str = f.read().strip()
    data_alvo = datetime.strptime(data_str, '%Y%m%d')
    
    session = criar_sessao()
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): banco[i['id']] = i
        except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper Exaustivo - Dia: {data_alvo.strftime('%d/%m/%Y')}")

    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pag_pub = 1
    while True:
        params = {"dataInicial": d_str, "dataFinal": d_str, "codigoModalidadeContratacao": "6", "pagina": pag_pub, "tamanhoPagina": 50}
        r = session.get(url_pub, params=params, timeout=25)
        if r.status_code != 200: break
        lics = r.json().get('data', [])
        if not lics: break

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: banco[res['id']] = res

        pag_pub += 1

    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if (hoje - proximo).days >= 0 else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
