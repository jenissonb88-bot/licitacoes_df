import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 8

# Blacklist para o Sniper já filtrar no ato da captura
BLACKLIST_OBJETO = ["LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "MATERIAL DE PINTURA", "MATERIAIS DE CONSTRUCAO", "GENERO ALIMENTICIO", "MERENDA", "ESCOLAR", "EXPEDIENTE", "EXAMES", "LABORATORIO"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []
    pag = 1
    while pag <= 50:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 100}, timeout=20, verify=False)
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
    try:
        r = session.get(url, timeout=20, verify=False)
        if r.status_code == 200:
            dados = r.json()
            return dados.get('data', []) if isinstance(dados, dict) else dados
    except: pass
    return []

def processar_licitacao(lic, session):
    try:
        obj_norm = normalize(lic.get('objetoCompra', ''))
        # VETO IMEDIATO NO SNIPER (Sua solicitação)
        if any(termo in obj_norm for termo in BLACKLIST_OBJETO):
            return None

        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid_obj = lic.get('unidadeOrgao', {})
        
        return {
            "id": f"{cnpj}{ano}{seq}",
            "data_pub": lic.get('dataPublicacaoPncp'),
            "data_enc": lic.get('dataEncerramentoProposta'),
            "uf": unid_obj.get('ufSigla'),
            "cidade": unid_obj.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'],
            "unidade_compradora": unid_obj.get('nomeUnidade', 'Não informada'),
            "objeto": lic.get('objetoCompra'),
            "edital_n": f"{str(lic.get('numeroCompra')).zfill(5)}/{ano}",
            "uasg": unid_obj.get('codigoUnidade', '---'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "valor_estimado_cabecalho": float(lic.get('valorTotalEstimado') or 0),
            "sigiloso_original": lic.get('niValorTotalEstimado', False),
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
    
    r = session.get(url_pub, params=params, timeout=25, verify=False)
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
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if (data_alvo + timedelta(days=1)).date() <= hoje.date() else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
