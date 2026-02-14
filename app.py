import requests, json, os, unicodedata, re, gzip, pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# === CONFIGURAÇÕES DE ARQUIVOS ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_EXCLUIDOS = 'excluidos.txt'
ARQ_INCLUIR = 'incluir.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 10

# === DATAS DE CONTROLO ===
DATA_INICIO_DIVULGACAO = datetime(2026, 1, 1)
HOJE = datetime(2026, 2, 14) # Mudar para datetime.now() em produção real

# === PALAVRAS-CHAVE RESGATADAS (DO CÓDIGO ANTIGO) ===
# Estes termos garantem que a licitação seja capturada mesmo se o nome do fármaco não estiver no título
KEYWORDS_GENERICAS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", "LUVA", "GAZE", "ALGODAO", "EQUIPO", "CATETER", 
    "SONDA", "AVENTAL", "MASCARA", "CURATIVO", "ESPARADRAPO"
]

UFS_NORDESTE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
KEYWORDS_NORDESTE = ["DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", "CALORIC", "PROTEIC", "LEITE", "NUTRI"]

# === BLACKLIST (FILTRO DE LIXO) ===
BLACKLIST = [
    "CONSTRUCAO", "OBRA", "PAVIMENTACAO", "CIMENTO", "ASFALTO", "TIJOLO", "PINTURA", "TINTA",
    "HIDRAULIC", "ELETRIC", "AUTOMOTIVO", "VEICULO", "PNEU", "MECANICA", "PECA",
    "REFEICAO", "LANCHE", "ALIMENTICIO", "ESCOLAR", "DIDATICO", "PAPELARIA", "LIVRO",
    "INFORMATICA", "SOFTWARE", "SAAS", "MOBILIARIO", "CADEIRA", "MESA", "LIMPEZA PREDIAL",
    "VETERINARI", "ANIMAL", "AGRO", "AGRICOLA", "BELICO", "MILITAR", "UNIFORME", "TECIDO"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_inteligencia_csv():
    """Carrega fármacos do CSV e combina com as palavras genéricas."""
    keywords = set(KEYWORDS_GENERICAS)
    if os.path.exists(ARQ_CSV):
        try:
            df = pd.read_csv(ARQ_CSV, encoding='latin1', sep=None, engine='python')
            # Busca a coluna de fármacos ou descrição
            col = [c for c in df.columns if 'FARMACO' in normalizar(c) or 'DESC' in normalizar(c)]
            if col:
                for k in df[col[0]].dropna().unique():
                    norm = normalizar(str(k))
                    if len(norm) > 3: keywords.add(norm)
            print(f"✅ Inteligência: {len(keywords)} termos carregados (CSV + Genéricos).")
        except Exception as e: print(f"⚠️ Erro CSV: {e}")
    return list(keywords)

def validar_item(descricao, uf):
    desc = normalizar(descricao)
    # REGRA PRIORITÁRIA: Se tem termo de saúde, nós queremos
    for k in KEYWORDS_GLOBAL:
        if k in desc:
            # Antes de validar, checa se não é algo óbvio da blacklist (ex: construção)
            for bad in BLACKLIST:
                if bad in desc: return False 
            return True
    
    # REGRA NORDESTE
    for n in KEYWORDS_NORDESTE:
        if n in desc: return uf in UFS_NORDESTE
    return False

def processar_licitacao(lic, session, ids_banidos, forcar=False):
    cnpj_limpo = re.sub(r'\D', '', str(lic['orgao_cnpj']))
    lic_id = f"{cnpj_limpo}{lic['ano_compra']}{lic['sequencial_compra']}"
    
    if lic_id in ids_banidos: return None

    base_url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_limpo}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}"
    try:
        ri = session.get(f"{base_url}/itens", timeout=15, verify=False)
        if ri.status_code != 200: return None
        
        itens_validos = []
        uf = lic.get('unidade_orgao', {}).get('uf_sigla', 'XX')
        
        for it in ri.json():
            if forcar or validar_item(it.get('descricao', ''), uf):
                val = it.get('valor_unitario_estimado') or 0.0
                qtd = it.get('quantidade') or 0
                itens_validos.append({
                    "item": it.get('numero_item'), "desc": it.get('descricao'),
                    "qtd": qtd, "unitario_est": float(val), "total_est": float(val) * float(qtd),
                    "situacao": it.get('situacao_compra_item_nome', 'EM ANDAMENTO')
                })
        
        if not itens_validos: return None

        return {
            "id": lic_id, "data_pub": lic.get('data_publicacao_pncp', ''),
            "data_enc": lic.get('data_encerramento_proposta', ''), "uf": uf,
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'),
            "objeto": lic.get('objeto_compra'), "itens": itens_validos,
            "link": f"https://pncp.gov.br/app/editais/{cnpj_limpo}/{lic['ano_compra']}/{lic['sequencial_compra']}"
        }
    except: return None

def processar_manuais(session, banco, ids_banidos):
    if not os.path.exists(ARQ_INCLUIR): return
    with open(ARQ_INCLUIR, 'r') as f: urls = f.readlines()
    for url in urls:
        m = re.search(r"editais/(\d+)/(\d+)/(\d+)", url.strip())
        if m:
            cnpj, ano, seq = m.groups()
            rb = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}", verify=False)
            if rb.status_code == 200:
                res = processar_licitacao(rb.json(), session, ids_banidos, forcar=True)
                if res: banco[res['id']] = res
    with open(ARQ_INCLUIR, 'w') as f: f.write("")

if __name__ == "__main__":
    KEYWORDS_GLOBAL = carregar_inteligencia_csv()
    IDS_EXCLUIDOS = set()
    if os.path.exists(ARQ_EXCLUIDOS):
        with open(ARQ_EXCLUIDOS, 'r') as f: IDS_EXCLUIDOS = {l.strip() for l in f if l.strip()}

    data_alvo = DATA_INICIO_DIVULGACAO
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f: 
            try: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))
    
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                for i in json.load(f): 
                    if i['id'] not in IDS_EXCLUIDOS: banco[i['id']] = i
        except: pass

    processar_manuais(session, banco, IDS_EXCLUIDOS)

    trigger_next = "false"
    dias_atraso = (HOJE - data_alvo).days
    
    if dias_atraso >= 0:
        d_str = data_alvo.strftime('%Y%m%d')
        print(f"🔍 Varrendo: {data_alvo.strftime('%d/%m/%Y')}...")
        pag = 1
        novos = 0
        while True:
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
            r = session.get(url, timeout=20, verify=False)
            if r.status_code != 200: break
            resp = r.json()
            if not resp.get('data'): break
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session, IDS_EXCLUIDOS): l for l in resp['data']}
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: 
                        banco[res['id']] = res
                        novos += 1; print(".", end="", flush=True)
            
            if pag >= resp.get('total_paginas', 0): break
            pag += 1
        
        proximo = data_alvo + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        if (HOJE - proximo).days >= 0: trigger_next = "true"
        print(f"\n✅ Dia concluído. Capturas: {novos}")

    # Salva banco compactado
    os.makedirs('dados', exist_ok=True)
    lista_final = sorted(banco.values(), key=lambda x: x['data_pub'], reverse=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger_next}", file=f)
