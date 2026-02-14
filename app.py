import requests, json, os, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 15

# === PALAVRAS-CHAVE DO SEU INDEX ORIGINAL ===
KEYWORDS_GERAIS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", "LUVA", "GAZE", "ALGODAO", "AMOXICILIN", "DIPIRON",
    "EQUIPO", "CATETER", "SONDA", "AVENTAL", "MASCARA", "CURATIVO"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_csv():
    keywords = set(KEYWORDS_GERAIS)
    if os.path.exists(ARQ_CSV):
        try:
            df = pd.read_csv(ARQ_CSV, encoding='latin1', sep=None, engine='python')
            col = [c for c in df.columns if 'FARMACO' in normalizar(c) or 'DESC' in normalizar(c)]
            if col:
                for k in df[col[0]].dropna().unique():
                    norm = normalizar(str(k))
                    if len(norm) > 3: keywords.add(norm)
        except: pass
    return list(keywords)

def validar_item(desc):
    d = normalizar(desc)
    # Se achou qualquer palavra de saúde da sua lista, CAPTURA.
    return any(k in d for k in KEYWORDS_GLOBAL)

def processar_licitacao(lic, session):
    cnpj = re.sub(r'\D', '', str(lic['orgao_cnpj']))
    lic_id = f"{cnpj}{lic['ano_compra']}{lic['sequencial_compra']}"

    try:
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
        ri = session.get(url_itens, timeout=15)
        if ri.status_code != 200: return None
        
        itens_validos = []
        for it in ri.json():
            if validar_item(it.get('descricao', '')):
                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": it.get('descricao'),
                    "qtd": it.get('quantidade') or 0,
                    "unitario_est": float(it.get('valor_unitario_estimado') or 0),
                    "total_est": float(it.get('valor_total_estimado') or 0),
                    "situacao": it.get('situacao_compra_item_nome', 'EM ANDAMENTO')
                })
        
        if not itens_validos: return None

        return {
            "id": lic_id,
            "data_pub": lic.get('data_publicacao_pncp'),
            "data_encerramento": lic.get('data_encerramento_proposta'),
            "uf": lic.get('unidade_orgao',{}).get('uf_sigla'),
            "cidade": lic.get('unidade_orgao',{}).get('municipio_nome'),
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'),
            "objeto": lic.get('objeto_compra'),
            "itens": itens_validos,
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{lic['ano_compra']}/{lic['sequencial_compra']}"
        }
    except: return None

if __name__ == "__main__":
    KEYWORDS_GLOBAL = carregar_csv()
    
    # Checkpoint
    data_alvo = datetime(2025, 12, 1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=3))
    
    # Carregar Banco Atual
    banco = {}
    if os.path.exists(ARQ_DADOS):
        with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
            for i in json.load(f): banco[i['id']] = i

    hoje = datetime.now()
    trigger_next = "false"

    if (hoje - data_alvo).days >= 0:
        d_str = data_alvo.strftime('%Y%m%d')
        print(f"🔍 Varrendo Divulgações de: {data_alvo.strftime('%d/%m/%Y')}")
        
        pag = 1
        while True:
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
            r = session.get(url, timeout=20)
            if r.status_code != 200: break
            resp = r.json()
            if not resp.get('data'): break
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session): l for l in resp['data']}
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: banco[res['id']] = res
            
            if pag >= resp.get('total_paginas', 0): break
            pag += 1
        
        # Atualiza checkpoint e define se continua
        proximo = data_alvo + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        if (hoje - proximo).days >= 0: trigger_next = "true"

    # Salva compacto
    lista_final = sorted(banco.values(), key=lambda x: x['data_pub'], reverse=True)
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger_next}", file=f)
