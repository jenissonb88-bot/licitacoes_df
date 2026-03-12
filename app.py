import requests
import json
import os
import unicodedata
import gzip
import concurrent.futures
import sys
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ORIGINAIS ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz'
ARQ_DICIONARIO = 'dicionario_ouro.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_LOCK = 'execucao.lock'
ARQ_LOG = 'log_captura.txt'
MAXWORKERS = 10

# --- GEOGRAFIA E BLOQUEIOS (Sua Estrutura) ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
UFS_PERMITIDAS_MED = NE_ESTADOS + ['DF', 'ES', 'MG', 'RJ', 'SP', 'GO', 'MT', 'MS', 'AM', 'PA', 'TO', 'BR', '']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- LISTAS DE VETOS E INTERESSES (Sua Estrutura) ---
VETOS_ABSOLUTOS = [normalize(x) for x in [
    "SOFTWARE", "SISTEMA", "IMPLANTACAO", "LICENCA", "INFORMATICA", "IMPRESSAO",
    "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "LOCACAO", "ASSINATURA", "MANUTENCAO",
    "LIMPEZA", "EXAME", "RADIOLOGIA", "IMAGEM", "VIGILANCIA", "SEGURANCA", "OFICINA",
    "GASES MEDICINAIS", "OXIGENIO", "ODONTOLOGICO", "PROTESE", "ORTESE", "CILINDRO",
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "ALIMENTACAO ESCOLAR", "MERENDA"
]]

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA"]]
WL_TERMOS_VAGOS = [normalize(x) for x in ["SAUDE", "HOSPITAL", "MATERNIDADE", "CLINICA", "FUNDO MUNICIPAL", "SECRETARIA DE"]]

def log_mensagem(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/24.1'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def carregar_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return f.read().strip()
    return "2026-02-01"

def salvar_checkpoint(data_atual_str):
    data_atual = datetime.strptime(data_atual_str, '%Y-%m-%d')
    proxima_data = data_atual + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proxima_data.strftime('%Y-%m-%d'))

def processar_licitacao(lic, session, termos_ouro):
    try:
        # 1. Dados Básicos
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or 'BR').upper().strip()
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)

        # 2. Filtros Geográficos e Vetos Absolutos (Sua Estrutura)
        if uf in ESTADOS_BLOQUEADOS: return None
        if any(v in obj_norm for v in VETOS_ABSOLUTOS): return None

        # 3. Inteligência Sniper: Busca de Itens para cada Edital
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        res = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 500}, timeout=15)
        if res.status_code != 200: return None
        
        itens_brutos = res.json().get('data', [])
        teve_match = False
        itens_mapeados = []

        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            # Cruzamento com o Dicionário de Ouro
            if any(termo in desc_item for termo in termos_ouro):
                teve_match = True
            
            # Estrutura de chaves minificadas para o seu Index (1)
            itens_mapeados.append({
                'n': it.get('numeroItem'),
                'd': it.get('descricao', ''),
                'q': it.get('quantidade'),
                'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0),
                'sit': 'EM ANDAMENTO'
            })

        # 4. Decisão de Captura
        # Se achou termo do dicionário OU se o título é MEDICAMENTO em região permitida
        if teve_match:
            pass # Mantém
        elif any(t in obj_norm for t in WL_MEDICAMENTOS) and uf in UFS_PERMITIDAS_MED:
            pass # Mantém
        else:
            return None # Descarta (Limpeza Profunda)

        return {
            'id': f"{cnpj}{ano}{seq}",
            'dt_enc': lic.get('dataEncerramentoProposta'),
            'uf': uf,
            'org': lic['orgaoEntidade']['razaoSocial'],
            'obj': obj_raw,
            'edit': f"{lic.get('numeroCompra')}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'itens': itens_mapeados,
            'sit_global': 'DIVULGADA'
        }
    except: return None

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")

    try:
        data_alvo = carregar_checkpoint()
        if data_alvo > date.today().strftime('%Y-%m-%d'):
            log_mensagem(f"✅ Checkpoint {data_alvo} já alcançou a data atual.")
            sys.exit(0)

        log_mensagem(f"🚀 Iniciando captura inteligente do dia: {data_alvo}")
        
        # Carregar Dicionário
        if not os.path.exists(ARQ_DICIONARIO):
            log_mensagem("❌ Dicionário de Ouro não encontrado!")
            sys.exit(1)
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            termos_ouro = [normalize(t) for t in json.load(f)]

        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        dia_api = data_alvo.replace('-', '')
        url_busca = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {'dataInicial': dia_api, 'dataFinal': dia_api, 'codigoModalidadeContratacao': 6, 'pagina': 1, 'tamanhoPagina': 50}
        
        res_busca = session.get(url_busca, params=params, timeout=30)
        if res_busca.status_code == 200:
            lics = res_busca.json().get('data', [])
            log_mensagem(f"📦 Encontradas {len(lics)} licitações brutas. Analisando itens...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session, termos_ouro) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: banco[res['id']] = res

            # Salvar Banco e Atualizar Checkpoint
            with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
                json.dump(list(banco.values()), f, ensure_ascii=False)
            
            salvar_checkpoint(data_alvo)
            log_mensagem(f"✅ Dia {data_alvo} concluído com sucesso.")
        else:
            log_mensagem(f"⚠️ Erro na API PNCP para o dia {data_alvo}")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
