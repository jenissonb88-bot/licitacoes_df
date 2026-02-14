import requests, json, os, urllib3, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES (RESGATANDO SEU INDEX ORIGINAL) ===
ARQ_DADOS = 'dados/oportunidades.js' # Voltamos para .js para seu index ler direto
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 15

# Palavras que garantem que o robô "pesque" a licitação
KEYWORDS_SAUDE = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", "LUVA", "GAZE", "ALGODAO", "EQUIPO", "CATETER", 
    "SONDA", "AVENTAL", "MASCARA", "CURATIVO", "ESPARADRAPO"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def validar_item(desc):
    d = normalizar(desc)
    # Se achou palavra de saúde, nós queremos!
    return any(k in d for k in KEYWORDS_SAUDE)

def processar_licitacao(lic, session):
    try:
        cnpj_limpo = re.sub(r'\D', '', str(lic['orgao_cnpj']))
        lic_id = f"{cnpj_limpo}{lic['ano_compra']}{lic['sequencial_compra']}"
        
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_limpo}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
        r = session.get(url_itens, timeout=15, verify=False)
        if r.status_code != 200: return None
        
        itens_validos = []
        for it in r.json():
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
            "data_pub": lic.get('data_publicacao_pncp', ''),
            "data_encerramento": lic.get('data_encerramento_proposta', ''),
            "uf": lic.get('unidade_orgao', {}).get('uf_sigla', 'XX'),
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome', ''),
            "orgao": lic.get('orgao_nome_fantasia', '') or lic.get('orgao_razao_social', ''),
            "objeto": lic.get('objeto_compra', ''),
            "link": f"https://pncp.gov.br/app/editais/{cnpj_limpo}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_validos
        }
    except: return None

if __name__ == "__main__":
    # Carregar banco existente (se houver)
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                for i in json.loads(conteudo): banco[i['id']] = i
        except: pass

    # Checkpoint
    data_alvo = datetime(2025, 12, 1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=3))
    
    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP - Varrendo Divulgações de: {data_alvo.strftime('%d/%m/%Y')}")
    
    pag = 1
    novos_no_dia = 0
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
        r = session.get(url, timeout=20, verify=False)
        if r.status_code != 200: break
        resp = r.json()
        lics = resp.get('data', [])
        if not lics: break

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res:
                    banco[res['id']] = res
                    novos_no_dia += 1
                    print(".", end="", flush=True)

        if pag >= resp.get('total_paginas', 0): break
        pag += 1

    # Salvar no formato que seu INDEX original entende
    os.makedirs('dados', exist_ok=True)
    lista_final = sorted(banco.values(), key=lambda x: x.get('data_pub', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json_str = json.dumps(lista_final, separators=(',', ':'), ensure_ascii=False)
        f.write(f"const dadosLicitacoes = {json_str};")

    # Próximo dia para o Efeito Dominó
    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    trigger = "true" if (datetime.now() - proximo).days >= 0 else "false"
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger}", file=f)
    
    print(f"\n✅ Concluído. Capturas: {novos_no_dia}")
