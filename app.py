import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 8 

# Blacklist preventiva (não gasta tempo com o que você já proibiu)
BLACKLIST_OBJETO = ["LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "MATERIAL DE PINTURA", "MATERIAIS DE CONSTRUCAO", "GENERO ALIMENTICIO", "MERENDA", "ESCOLAR", "EXPEDIENTE", "EXAMES", "LABORATORIO"]

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
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
        obj_norm = ''.join(c for c in unicodedata.normalize('NFD', lic.get('objetoCompra', '').upper()) if unicodedata.category(c) != 'Mn')
        if any(t in obj_norm for t in BLACKLIST_OBJETO): return None

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
            "unidade_compradora": unid.get('nomeUnidade', 'Não Informada'),
            "objeto": lic.get('objetoCompra'),
            "edital_n": f"{str(lic.get('numeroCompra')).zfill(5)}/{ano}",
            "uasg": unid.get('codigoUnidade', '---'),
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

    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                dados_existentes = json.load(f)
                banco = {i['id']: i for i in dados_existentes}
        except: pass

    # --- REVISOR: Atualiza resultados de pregões que já encerram ---
    print("🔄 Revisor: Buscando resultados para pregões recentes...")
    pendentes = [id_l for id_l, l in banco.items() if l.get('data_enc', '')[:10] <= hoje.strftime('%Y-%m-%d')]
    for id_l in pendentes[-50:]: # Revisa os últimos 50 do banco
        cnpj, ano, seq = id_l[:14], id_l[14:18], id_l[18:]
        banco[id_l]['resultados_raw'] = buscar_todos_resultados(session, cnpj, ano, seq)

    # --- SNIPER: Captura novos ---
    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper: Varrendo {data_alvo.strftime('%d/%m/%Y')}")
    
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

    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if proximo.date() <= hoje.date() else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
