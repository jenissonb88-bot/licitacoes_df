import requests, json, os, urllib3, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# Desativar avisos de SSL (comum em portais de governo)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES DE ARQUIVOS ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 15  # Velocidade máxima

# === PALAVRAS-CHAVE DE SAÚDE ===
KEYWORDS_SAUDE = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", "LUVA", "GAZE", "ALGODAO", "AMOXICILIN", "DIPIRON",
    "EQUIPO", "CATETER", "SONDA", "AVENTAL", "MASCARA", "CURATIVO"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_csv():
    """Carrega fármacos do arquivo CSV e combina com as palavras-chave gerais."""
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

def validar_item(desc):
    d = normalizar(desc)
    return any(k in d for k in KEYWORDS_GLOBAL)

def processar_licitacao(lic, session):
    """Processa uma licitação individual e seus itens."""
    try:
        cnpj = re.sub(r'\D', '', str(lic['orgao_cnpj']))
        lic_id = f"{cnpj}{lic['ano_compra']}{lic['sequencial_compra']}"
        
        # URL da API de Itens do PNCP
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
        ri = session.get(url_itens, timeout=15, verify=False)
        if ri.status_code != 200: return None
        
        itens_validos = []
        for it in ri.json():
            if validar_item(it.get('descricao', '')):
                # --- LÓGICA DE BENEFÍCIO ME/EPP (1,2,3 = Sim | 4,5 = Não) ---
                beneficio_id = it.get('tipoBeneficioId')
                me_epp = "Sim" if beneficio_id in [1, 2, 3] else "Não"
                
                val_unit = float(it.get('valor_unitario_estimado') or 0)
                qtd = float(it.get('quantidade') or 0)
                
                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": it.get('descricao'),
                    "qtd": qtd,
                    "unitario_est": val_unit,
                    "total_est": val_unit * qtd,
                    "me_epp": me_epp,
                    "fornecedor": it.get('nomeFornecedor') or "EM ANDAMENTO",
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
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_validos
        }
    except: return None

if __name__ == "__main__":
    KEYWORDS_GLOBAL = carregar_csv()
    
    # Gerenciar Checkpoint (Início em 01/12/2025 se não existir)
    data_alvo = datetime(2025, 12, 1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=3))
    
    # Carregar Banco de Dados Compactado
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): banco[i['id']] = i
        except: pass

    hoje = datetime.now()
    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP - Varrendo Dia: {data_alvo.strftime('%d/%m/%Y')}")
    
    novos_no_dia = 0
    pag = 1
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

    # Salvar Banco Compactado
    lista_final = sorted(banco.values(), key=lambda x: x.get('data_pub', ''), reverse=True)
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

    # Atualizar Checkpoint
    proximo = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
    
    # === LÓGICA DE REINÍCIO AUTOMÁTICO (EFEITO DOMINÓ) ===
    # Se o próximo dia ainda for no passado ou hoje, avisa o GitHub para rodar novamente
    trigger_next = "true" if (hoje - proximo).days >= 0 else "false"
    
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            print(f"trigger_next={trigger_next}", file=f)
    
    print(f"\n✅ Dia concluído. Capturas: {novos_no_dia} | Reiniciar: {trigger_next}")
