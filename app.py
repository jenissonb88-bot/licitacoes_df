import requests, json, os, urllib3, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações de Ficheiros
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 15

# Termos de Saúde
KEYWORDS_SAUDE = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "AMOXICILIN", "DIPIRON", "INSULIN", "SAUDE"]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def processar_licitacao(lic, session, keywords):
    try:
        cnpj = re.sub(r'\D', '', str(lic['orgao_cnpj']))
        uasg = lic.get('unidade_orgao', {}).get('codigo_unidade')
        # Formato de Edital Padrão PNCP: 00010/2026
        edital_n = f"{lic['numero_compra'].zfill(5)}/{lic['ano_compra']}"
        lic_id = f"{cnpj}{lic['ano_compra']}{lic['sequencial_compra']}"
        
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
        r = session.get(url_itens, timeout=15, verify=False)
        if r.status_code != 200: return None
        
        itens_validos = []
        for it in r.json():
            if any(k in normalizar(it.get('descricao', '')) for k in keywords):
                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": it.get('descricao'),
                    "qtd": it.get('quantidade') or 0,
                    "unitario_est": float(it.get('valor_unitario_estimado') or 0),
                    "total_est": float(it.get('valor_total_estimado') or 0),
                    "beneficio_id": it.get('tipoBeneficioId'),
                    "situacao": it.get('situacao_compra_item_nome', 'EM ANDAMENTO'),
                    "fornecedor": it.get('nomeFornecedor') or "EM ANDAMENTO"
                })
        
        return {
            "id": lic_id, "edital_n": edital_n, "uasg": uasg,
            "data_pub": lic.get('data_publicacao_pncp'),
            "data_enc": lic.get('data_encerramento_proposta'),
            "uf": lic.get('unidade_orgao', {}).get('uf_sigla'),
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome'),
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'),
            "objeto": lic.get('objeto_compra'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_validos
        } if itens_validos else None
    except: return None

if __name__ == "__main__":
    hoje_dt = datetime.now() # 14/02/2026
    data_alvo = hoje_dt - timedelta(days=5) # Começa a varrer 5 dias atrás

    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: 
                cp = datetime.strptime(f.read().strip(), '%Y%m%d')
                if cp <= hoje_dt: data_alvo = cp
            except: pass

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=3))
    
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): banco[i['id']] = i
        except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP - Varrendo data: {data_alvo.strftime('%d/%m/%Y')}")
    
    url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina=1&tamanho_pagina=100"
    r = session.get(url, verify=False)
    
    novos = 0
    if r.status_code == 200:
        lics = r.json().get('data', [])
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session, KEYWORDS_SAUDE): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: 
                    banco[res['id']] = res
                    novos += 1

    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False, separators=(',', ':'))

    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if (hoje_dt - proximo).days >= 0 else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
