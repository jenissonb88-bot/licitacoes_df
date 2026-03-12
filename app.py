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
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

VETOS_ABSOLUTOS = [normalize(x) for x in [
    "SOFTWARE", "SISTEMA", "IMPLANTACAO", "LICENCA", "INFORMATICA", "IMPRESSAO",
    "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "LOCACAO", "ASSINATURA", "MANUTENCAO",
    "LIMPEZA", "EXAME", "RADIOLOGIA", "IMAGEM", "VIGILANCIA", "SEGURANCA", "OFICINA",
    "GASES MEDICINAIS", "OXIGENIO", "ODONTOLOGICO", "PROTESE", "ORTESE", "CILINDRO",
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "ALIMENTACAO ESCOLAR", "MERENDA"
]]

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA"]]

def log_mensagem(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/24.2'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

# --- 🔄 NOVIDADE: BUSCA TODOS OS ITENS DE UMA LICITAÇÃO (PAGINADO) ---
def buscar_todos_os_itens(cnpj, ano, seq, session):
    todos_itens = []
    pagina_item = 1
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={'pagina': pagina_item, 'tamanhoPagina': 500}, timeout=20)
            if r.status_code != 200: break
            
            dados = r.json().get('data', [])
            if not dados: break # Fim das páginas de itens
            
            todos_itens.extend(dados)
            
            if len(dados) < 500: break # Se veio menos que o tamanho da página, é a última
            pagina_item += 1
        except:
            break
    return todos_itens

def processar_licitacao(lic, session, termos_ouro):
    try:
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or 'BR').upper().strip()
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)

        if uf in ESTADOS_BLOQUEADOS: return None
        if any(v in obj_norm for v in VETOS_ABSOLUTOS): return None

        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        # 🔄 Captura exaustiva de todos os itens (até 5000 ou mais)
        itens_brutos = buscar_todos_os_itens(cnpj, ano, seq, session)
        
        teve_match = False
        itens_mapeados = []

        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            if any(termo in desc_item for termo in termos_ouro):
                teve_match = True
            
            itens_mapeados.append({
                'n': it.get('numeroItem'),
                'd': it.get('descricao', ''),
                'q': it.get('quantidade'),
                'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0),
                'sit': 'EM ANDAMENTO'
            })

        if teve_match or (any(t in obj_norm for t in WL_MEDICAMENTOS) and uf in UFS_PERMITIDAS_MED):
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
        return None
    except: return None

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")

    try:
        # Checkpoint incremental
        if os.path.exists(ARQ_CHECKPOINT):
            with open(ARQ_CHECKPOINT, 'r') as f: data_alvo = f.read().strip()
        else: data_alvo = "2026-02-01"

        if data_alvo > date.today().strftime('%Y-%m-%d'):
            log_mensagem(f"✅ Tudo capturado até hoje.")
            sys.exit(0)

        log_mensagem(f"🚀 Iniciando varredura TOTAL do dia: {data_alvo}")
        
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            termos_ouro = [normalize(t) for t in json.load(f)]

        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        # 🔄 NOVIDADE: LOOP POR TODAS AS PÁGINAS DE RESULTADOS DA PESQUISA
        pagina_busca = 1
        total_capturado_hoje = 0
        
        while True:
            dia_api = data_alvo.replace('-', '')
            url_busca = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                'dataInicial': dia_api, 
                'dataFinal': dia_api, 
                'codigoModalidadeContratacao': 6, 
                'pagina': pagina_busca, 
                'tamanhoPagina': 50
            }
            
            res_busca = session.get(url_busca, params=params, timeout=30)
            if res_busca.status_code != 200: break
            
            lics = res_busca.json().get('data', [])
            if not lics: break # Fim das páginas de resultados
            
            log_mensagem(f"📄 Processando página {pagina_busca} de resultados...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session, termos_ouro) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: 
                        banco[res['id']] = res
                        total_capturado_hoje += 1

            if len(lics) < 50: break # Última página de resultados
            pagina_busca += 1

        # Salvar banco e pular para o próximo dia
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        
        proximo = (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
        
        log_mensagem(f"✅ Dia {data_alvo} concluído. {total_capturado_hoje} licitações sniper salvas.")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
