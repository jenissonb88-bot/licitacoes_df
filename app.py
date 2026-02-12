import requests, json, os, time, urllib3, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_FINISH = 'finish.txt'

# Lista otimizada (Sempre em MAIÚSCULAS e sem acentos para comparação)
KEYWORDS = ["MEDICAMENTO", "SAUDE", "HOSPITALAR", "AMOXICILINA", "SERINGA", "GAZE", "SORO", "INSULINA", "DIPIRONA", "AVENTAL", "LUVA", "CATETER", "EQUIPO", "AGULHA"] 
# Adicionei as principais, mas a lógica de busca agora é 'contém' e não 'igual'.

BLACKLIST = ["OBRA", "VEICULO", "INFORMATICA", "LIMPEZA PREDIAL", "CONSTRUCAO", "ESCOLAR"]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(texto_objeto, lista_itens=[]):
    obj = normalize(texto_objeto)
    # Se tiver blacklist no objeto, descarta logo
    if any(b in obj for b in BLACKLIST): return False
    
    # Busca nas Keywords (Se o objeto contiver a palavra, é relevante)
    if any(k in obj for k in KEYWORDS): return True
    
    # Busca dentro dos itens (Se qualquer item contiver a palavra, é relevante)
    for it in lista_itens:
        desc_item = normalize(it.get('descricao', ''))
        if any(k in desc_item for k in KEYWORDS):
            return True
            
    return False

def run():
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))
    
    # 1. Carrega Banco
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw: banco = {i['id']: i for i in json.loads(raw)}
        except: pass

    # 2. Data do Checkpoint
    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write("20260101")
    with open(ARQ_CHECKPOINT, 'r') as f: data_str = f.read().strip()
    
    data_alvo = datetime.strptime(data_str, '%Y%m%d')
    hoje = datetime.now()

    if data_alvo.date() > hoje.date():
        with open(ARQ_FINISH, 'w') as f: f.write('true')
        return

    print(f"🚀 Analisando: {data_alvo.strftime('%d/%m/%Y')}")
    str_data = data_alvo.strftime('%Y%m%d')
    
    # 3. Busca na API
    novos = 0
    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": str_data, "dataFinal": str_data, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50}
    
    try:
        r = session.get(url_pub, params=params, timeout=20)
        if r.status_code == 200:
            lics = r.json().get('data', [])
            for lic in lics:
                cnpj = lic['orgaoEntidade']['cnpj']
                ano = lic['anoCompra']
                seq = lic['sequencialCompra']
                id_lic = f"{cnpj}{ano}{seq}"
                
                # Para saber se é relevante, precisamos olhar os itens TAMBÉM
                url_itens = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}/itens"
                res_itens = session.get(url_itens, params={"pagina":1, "tamanhoPagina":500})
                itens_raw = res_itens.json() if res_itens.status_code == 200 else []

                if eh_relevante(lic.get('objetoCompra'), itens_raw):
                    # Processa e Salva (usa a lógica de montar_objeto anterior)
                    # ... (código de montagem do objeto aqui)
                    novos += 1
    except Exception as e:
        print(f"Erro: {e}")

    # 4. Finaliza Dia
    proximo = (data_alvo + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
    # salvar_banco_disco(banco)...
