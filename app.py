import requests, json, os, urllib3, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# Desativar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 10 

# Palavras-chave amplas para garantir a captura na área da saúde
KEYWORDS_SAUDE = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "DISEASE", "SAUDE", "UBS", "HOSPITAL", "CLINIC"]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_csv():
    keywords = set(KEYWORDS_SAUDE)
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

def processar_licitacao(lic, session, keywords_global):
    try:
        cnpj = re.sub(r'\D', '', str(lic['orgao_cnpj']))
        lic_id = f"{cnpj}{lic['ano_compra']}{lic['sequencial_compra']}"
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
        
        r = session.get(url_itens, timeout=20, verify=False)
        if r.status_code != 200: return None
        
        itens = []
        for it in r.json():
            desc_completa = normalizar(it.get('descricao', ''))
            if any(k in desc_completa for k in keywords_global):
                itens.append({
                    "item": it.get('numero_item'),
                    "desc": it.get('descricao'),
                    "qtd": float(it.get('quantidade') or 0),
                    "unitario_est": float(it.get('valor_unitario_estimado') or 0),
                    "total_est": float(it.get('valor_total_estimado') or 0),
                    "beneficio_id": it.get('tipoBeneficioId'),
                    "situacao": it.get('situacao_compra_item_nome', 'EM ANDAMENTO'),
                    "fornecedor": it.get('nomeFornecedor') or "EM ANDAMENTO"
                })
        
        if not itens: return None
        return {
            "id": lic_id, 
            "data_pub": lic.get('data_publicacao_pncp'), 
            "data_enc": lic.get('data_encerramento_proposta'),
            "uf": lic.get('unidade_orgao', {}).get('uf_sigla'), 
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome'),
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'), 
            "objeto": lic.get('objeto_compra'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{lic['ano_compra']}/{lic['sequencial_compra']}", 
            "itens": itens
        }
    except: return None

if __name__ == "__main__":
    keywords_global = carregar_csv()
    hoje_dt = datetime.now()
    
    # Define data inicial caso o checkpoint não exista
    data_alvo = hoje_dt - timedelta(days=2) 
    
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            content = f.read().strip()
            if content: 
                try: data_alvo = datetime.strptime(content, '%Y%m%d')
                except: pass

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=5))
    
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): banco[i['id']] = i
        except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"Buscando Pregões (Cód 6) para o dia: {d_str}")
    
    novos_total = 0
    # Apenas Código 6 (Pregão)
    url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina=1&tamanho_pagina=100"
    
    r = session.get(url, verify=False)
    if r.status_code == 200:
        lics = r.json().get('data', [])
        print(f"Encontrados {len(lics)} editais no PNCP. Filtrando itens de saúde...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session, keywords_global): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: 
                    banco[res['id']] = res
                    novos_total += 1

    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False, separators=(',', ':'))

    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    trigger = "true" if (hoje_dt - proximo).days >= 0 else "false"
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: 
            print(f"trigger_next={trigger}", file=f)
    
    print(f"Fim da rodada. Capturados: {novos_total}")
