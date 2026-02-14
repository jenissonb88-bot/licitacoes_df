import requests, json, os, urllib3, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# Desativar alertas de segurança de sites governamentais
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 15 

# Palavras-chave base (Saúde)
KEYWORDS_SAUDE = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", "LUVA", "GAZE", "ALGODAO", "EQUIPO", "CATETER", 
    "SONDA", "AVENTAL", "MASCARA", "CURATIVO", "ESPARADRAPO"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_inteligencia_csv():
    """Lê o seu CSV e extrai nomes de fármacos para busca."""
    keywords = set(KEYWORDS_SAUDE)
    if os.path.exists(ARQ_CSV):
        try:
            df = pd.read_csv(ARQ_CSV, encoding='latin1', sep=None, engine='python')
            # Busca colunas que possam conter nomes de remédios
            col = [c for c in df.columns if 'FARMACO' in normalizar(c) or 'DESC' in normalizar(c)]
            if col:
                for k in df[col[0]].dropna().unique():
                    norm = normalizar(str(k))
                    if len(norm) > 3: keywords.add(norm)
        except: pass
    return list(keywords)

def validar_item(descricao):
    desc_norm = normalizar(descricao)
    return any(k in desc_norm for k in KEYWORDS_GLOBAL)

def criar_sessao():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def processar_licitacao(lic, session):
    """Extrai itens e dados técnicos de cada licitação encontrada."""
    try:
        cnpj_limpo = re.sub(r'\D', '', str(lic['orgao_cnpj']))
        lic_id = f"{cnpj_limpo}{lic['ano_compra']}{lic['sequencial_compra']}"
        
        # API de Itens do PNCP
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_limpo}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
        r = session.get(url_itens, timeout=15, verify=False)
        if r.status_code != 200: return None
        
        itens_validos = []
        for it in r.json():
            if validar_item(it.get('descricao', '')):
                val = float(it.get('valor_unitario_estimado') or 0)
                qtd = float(it.get('quantidade') or 0)
                
                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": it.get('descricao'),
                    "qtd": qtd,
                    "unitario_est": val,
                    "total_est": val * qtd,
                    "me_epp_id": it.get('tipoBeneficioId'), # O Faxineiro usará isso
                    "situacao": it.get('situacao_compra_item_nome', 'EM ANDAMENTO'),
                    "fornecedor": it.get('nomeFornecedor') or "EM ANDAMENTO"
                })
        
        if not itens_validos: return None

        return {
            "id": lic_id,
            "data_pub": lic.get('data_publicacao_pncp'),
            "data_enc": lic.get('data_encerramento_proposta'),
            "uf": lic.get('unidade_orgao', {}).get('uf_sigla'),
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome'),
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'),
            "objeto": lic.get('objeto_compra'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj_limpo}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_validos
        }
    except: return None

if __name__ == "__main__":
    print("🚀 Sniper PNCP - Iniciando Captura Bruta")
    KEYWORDS_GLOBAL = carregar_inteligencia_csv()
    
    # Checkpoint de data
    data_alvo = datetime(2025, 12, 1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    session = criar_sessao()
    
    # Carregar base atual
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): banco[i['id']] = i
        except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🔍 Minerando dia: {data_alvo.strftime('%d/%m/%Y')}...")
    
    pag = 1
    novos = 0
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
        try:
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
                        novos += 1
                        print(".", end="", flush=True)

            if pag >= resp.get('total_paginas', 0): break
            pag += 1
        except: break

    # Salva o "Minério Bruto" para o Faxineiro limpar
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False, separators=(',', ':'))

    # Prepara reinício automático
    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    trigger = "true" if (datetime.now() - proximo).days >= 0 else "false"
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            print(f"trigger_next={trigger}", file=f)
            
    print(f"\n✅ Captura finalizada. {novos} licitações enviadas para limpeza.")
