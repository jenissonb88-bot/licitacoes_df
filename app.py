import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 10 

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []
    pag = 1
    while True:
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
    session = criar_sessao()
    banco = {}
    
    # 1. CARREGAR BASE EXISTENTE
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                dados_existentes = json.load(f)
                banco = {i['id']: i for i in dados_existentes}
        except: pass

    # 2. REVISOR: Atualizar resultados de pregões que já passaram da data de encerramento
    print("🔄 Revisor: Verificando se saíram resultados novos para pregões antigos...")
    ids_para_revisar = []
    for id_lic, lic in banco.items():
        # Se a data de encerramento já passou e ainda não temos resultados processados (itens_raw ainda existe)
        if lic.get('data_enc') and lic['data_enc'][:10] <= hoje.strftime('%Y-%m-%d'):
            # Se a licitação ainda não foi limpa pelo limpeza.py (ainda tem dados brutos)
            # ou se você quiser forçar a atualização de quem está "EM ANDAMENTO"
            ids_para_revisar.append(id_lic)

    # Revisão limitada aos últimos 50 para não estourar o tempo do GitHub
    for id_lic in ids_para_revisar[:50]:
        cnpj, ano, seq = id_lic[:14], id_lic[14:18], id_lic[18:]
        banco[id_lic]['resultados_raw'] = buscar_todos_resultados(session, cnpj, ano, seq)
        # Forçamos a volta dos itens_raw para que o limpeza.py processe o novo vencedor
        if 'itens' in banco[id_lic]:
             banco[id_lic]['itens_raw'] = buscar_todos_itens(session, cnpj, ano, seq)

    # 3. SNIPER: Capturar novos do dia
    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try:
                cp = datetime.strptime(f.read().strip(), '%Y%m%d')
                data_alvo = cp if cp <= hoje else data_alvo
            except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper: Capturando novos do dia {data_alvo.strftime('%d/%m/%Y')}...")
    
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

    # 4. SALVAR TUDO (Novos + Atualizados)
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    # Avançar checkpoint
    proximo_dia = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia.strftime('%Y%m%d'))
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if proximo_dia.date() <= hoje.date() else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
