import requests, json, os, urllib3, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 10 

KEYWORDS_SAUDE = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "DISEASE", "SAUDE", "UBS", "HOSPITAL", "CLINIC"]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def buscar_todos_itens(cnpj, ano, sequencial, session):
    """Busca exaustivamente todos os itens de uma licitação, paginando se necessário."""
    todos_itens = []
    pagina_item = 1
    
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens?pagina={pagina_item}&tamanhoPagina=500"
        try:
            r = session.get(url, timeout=25, verify=False)
            if r.status_code != 200: break
            
            itens_pagina = r.json()
            if not itens_pagina: break
            
            for it in itens_pagina:
                desc_norm = normalizar(it.get('descricao', ''))
                if any(k in desc_norm for k in KEYWORDS_SAUDE):
                    todos_itens.append({
                        "item": it.get('numero_item'),
                        "desc": it.get('descricao'),
                        "qtd": it.get('quantidade') or 0,
                        "unitario_est": float(it.get('valor_unitario_estimado') or 0),
                        "total_est": float(it.get('valor_total_estimado') or 0),
                        "beneficio_id": it.get('tipoBeneficioId'),
                        "situacao": it.get('situacao_compra_item_nome', 'EM ANDAMENTO'),
                        "fornecedor": it.get('nomeFornecedor') or "EM ANDAMENTO"
                    })
            
            # Se a página atual veio com menos que o tamanho solicitado, chegamos ao fim
            if len(itens_pagina) < 500: break
            pagina_item += 1
        except:
            break
            
    return todos_itens

def processar_licitacao(lic, session):
    """Processa a licitação capturando UASG, Edital Nº e Data de Encerramento."""
    try:
        cnpj = re.sub(r'\D', '', str(lic['orgao_cnpj']))
        # UASG: codigo_unidade conforme manual PNCP
        uasg = lic.get('unidade_orgao', {}).get('codigo_unidade')
        # Edital Nº: Numero da compra / Ano
        edital_n = f"{lic['numero_compra'].zfill(5)}/{lic['ano_compra']}"
        lic_id = f"{cnpj}{lic['ano_compra']}{lic['sequencial_compra']}"
        
        # Busca exaustiva de itens (pode ler 5000+ itens)
        itens_saude = buscar_todos_itens(cnpj, lic['ano_compra'], lic['sequencial_compra'], session)
        
        if not itens_saude: return None
        
        return {
            "id": lic_id,
            "edital_n": edital_n,
            "uasg": uasg,
            "data_pub": lic.get('data_publicacao_pncp'),
            "data_enc": lic.get('data_encerramento_proposta'),
            "uf": lic.get('unidade_orgao', {}).get('uf_sigla'),
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome'),
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'),
            "objeto": lic.get('objeto_compra'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_saude
        }
    except: return None

if __name__ == "__main__":
    hoje_dt = datetime.now() # 14/02/2026
    data_alvo = hoje_dt - timedelta(days=1)

    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
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
    print(f"🚀 Sniper Exaustivo - Varrendo Dia: {data_alvo.strftime('%d/%m/%Y')}")
    
    pagina_lic = 1
    total_capturados_hoje = 0

    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pagina_lic}&tamanho_pagina=50"
        r = session.get(url, verify=False)
        if r.status_code != 200: break
        
        res_json = r.json()
        lics = res_json.get('data', [])
        total_paginas = res_json.get('total_paginas', 1)

        if not lics: break

        print(f"📄 Processando página {pagina_lic} de {total_paginas}...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: 
                    banco[res['id']] = res
                    total_capturados_hoje += 1

        if pagina_lic >= total_paginas: break
        pagina_lic += 1

    # Salva os dados
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False, separators=(',', ':'))

    # Checkpoint e Gatilho
    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if (hoje_dt - proximo).days >= 0 else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
    
    print(f"🏁 Dia finalizado. Total de editais com itens de saúde: {total_capturados_hoje}")
