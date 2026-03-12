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

# --- CONFIGURAÇÕES ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz'
ARQ_DICIONARIO = 'dicionario_ouro.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_LOCK = 'execucao.lock'
ARQ_LOG = 'log_captura.txt'
MAXWORKERS = 10

# --- GEOGRAFIA (Sua Estrutura) ---
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
    timestamp = datetime.now().strftime('%H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Auditor/24.3'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_todos_os_itens(cnpj, ano, seq, session):
    todos_itens = []
    pagina_item = 1
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={'pagina': pagina_item, 'tamanhoPagina': 500}, timeout=20)
            if r.status_code != 200: break
            dados = r.json().get('data', [])
            if not dados: break
            todos_itens.extend(dados)
            if len(dados) < 500: break
            pagina_item += 1
        except: break
    return todos_itens

def processar_licitacao(lic, session, termos_ouro):
    try:
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or 'BR').upper().strip()
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)
        edit = f"{lic.get('numeroCompra')}/{lic.get('anoCompra')}"

        # 🚩 MOTIVO 1: Bloqueio Geográfico
        if uf in ESTADOS_BLOQUEADOS:
            return ('VETO_GEO', f"UF {uf} bloqueada")

        # 🚩 MOTIVO 2: Veto por Palavra-Chave no Título
        for v in VETOS_ABSOLUTOS:
            if v in obj_norm:
                return ('VETO_TITULO', f"Termo proibido '{v}' no objeto")

        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        itens_brutos = buscar_todos_os_itens(cnpj, ano, seq, session)
        
        teve_match = False
        itens_mapeados = []
        termos_encontrados = []

        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            
            # Checa match com dicionário
            match_local = [t for t in termos_ouro if t in desc_item]
            if match_local:
                teve_match = True
                termos_encontrados.extend(match_local)
            
            itens_mapeados.append({
                'n': it.get('numeroItem'), 'd': it.get('descricao', ''),
                'q': it.get('quantidade'), 'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0), 'sit': 'EM ANDAMENTO'
            })

        # 🚩 MOTIVO 3: Não passou no Dicionário E não é Medicamento Explícito
        is_med_titulo = any(t in obj_norm for t in WL_MEDICAMENTOS)
        
        if teve_match:
            return ('OK', {
                'id': f"{cnpj}{ano}{seq}", 'dt_enc': lic.get('dataEncerramentoProposta'),
                'uf': uf, 'org': lic['orgaoEntidade']['razaoSocial'], 'obj': obj_raw,
                'edit': edit, 'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                'itens': itens_mapeados, 'sit_global': 'DIVULGADA'
            })
        
        if is_med_titulo and uf in UFS_PERMITIDAS_MED:
             # Se o título é medicamento, deixamos passar mesmo sem match nos itens (para análise manual)
             return ('OK', {
                'id': f"{cnpj}{ano}{seq}", 'dt_enc': lic.get('dataEncerramentoProposta'),
                'uf': uf, 'org': lic['orgaoEntidade']['razaoSocial'], 'obj': obj_raw,
                'edit': edit, 'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                'itens': itens_mapeados, 'sit_global': 'DIVULGADA'
            })

        return ('VETO_DICIONARIO', f"Nenhum dos {len(itens_mapeados)} itens consta no dicionário e título não é med.")
    except Exception as e:
        return ('ERRO', str(e))

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")

    try:
        if os.path.exists(ARQ_CHECKPOINT):
            with open(ARQ_CHECKPOINT, 'r') as f: data_alvo = f.read().strip()
        else: data_alvo = "2026-02-01"

        log_mensagem(f"🔍 MODO AUDITOR: Analisando dia {data_alvo}")
        
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            termos_ouro = [normalize(t) for t in json.load(f)]

        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        dia_api = data_alvo.replace('-', '')
        pagina = 1
        
        while True:
            r = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao", 
                            params={'dataInicial': dia_api, 'dataFinal': dia_api, 'codigoModalidadeContratacao': 6, 'pagina': pagina, 'tamanhoPagina': 50})
            if r.status_code != 200: break
            lics = r.json().get('data', [])
            if not lics: break
            
            log_mensagem(f"📄 Pag {pagina}: Analisando {len(lics)} editais...")
            
            for l in lics:
                status, info = processar_licitacao(l, session, termos_ouro)
                edit = f"{l.get('numeroCompra')}/{l.get('anoCompra')}"
                
                if status == 'OK':
                    log_mensagem(f"   ✅ CAPTURADO: {edit} | {l.get('orgaoEntidade', {}).get('razaoSocial')[:30]}")
                    banco[info['id']] = info
                else:
                    # Loga o motivo do veto para você entender o que o robô está rejeitando
                    log_mensagem(f"   ❌ PULADO ({status}): {edit} -> {info}")

            if len(lics) < 50: break
            pagina += 1

        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        
        proximo = (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
        log_mensagem(f"🏁 Fim do dia {data_alvo}.")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
