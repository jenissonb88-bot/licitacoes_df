import requests
import json
import gzip
import os
import concurrent.futures
import unicodedata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
MAXWORKERS = 6  # Aumentei para compensar o maior número de requisições de detalhe

# --- MAPAS OFICIAIS ---
MAPA_SITUACAO = {
    1: "EM ANDAMENTO",
    2: "HOMOLOGADO",
    3: "CANCELADO",
    4: "DESERTO",
    5: "FRACASSADO"
}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def inferir_beneficio_por_texto(desc):
    d = normalize(desc)
    termos_exclusivos = ["EXCLUSIVA ME", "EXCLUSIVO ME", "EXCLUSIVA PARA ME", "COTA EXCLUSIVA", "SOMENTE ME", "EXCLUSIVIDADE ME"]
    termos_reservada = ["COTA RESERVADA", "RESERVADA ME", "RESERVADA PARA ME"]

    for t in termos_exclusivos:
        if t in d: return 1 
    for t in termos_reservada:
        if t in d: return 3 
    return None

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma Auditor/9.9'})
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def safe_float(val):
    try: return float(val) if val is not None else 0.0
    except: return 0.0

def safe_int(val, default=4):
    try: return int(val) if val is not None else default
    except: return default

def extrair_beneficio(item_data):
    """
    Tenta extrair o ID do benefício de todas as formas que a API manda.
    """
    # Tentativa 1: Campo direto ID
    b_id = item_data.get('tipoBeneficioId')
    if b_id is not None: return int(b_id)
    
    # Tentativa 2: Objeto aninhado
    b_obj = item_data.get('tipoBeneficio')
    if isinstance(b_obj, dict):
        return int(b_obj.get('id', 4))
        
    return 4 # Padrão Amplo

def precisa_atualizar(licitacao):
    """
    Força atualização se:
    1. Tem item aberto.
    2. Tem item marcado como Amplo (4) -> Precisamos confirmar se é verdade.
    """
    itens = licitacao.get('itens', [])
    if not itens: return True 

    for it in itens:
        sit = str(it.get('sit', '')).upper()
        if sit in ['ABERTO', 'EM ANDAMENTO', '']:
            return True
        
        # Se diz que é 4 (Amplo), força atualização para tirar a prova real
        if safe_int(it.get('benef'), 4) == 4:
            return True
            
    return False

def atualizar_licitacao(lic_id, dados_antigos, session):
    try:
        cnpj = lic_id[:14]
        ano = lic_id[14:18]
        seq = lic_id[18:]

        # 1. Busca Lista de Itens
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        
        if r.status_code != 200: return None

        resp_json = r.json()
        itens_novos_raw = resp_json.get('data', []) if isinstance(resp_json, dict) else resp_json
        if not itens_novos_raw: return None

        itens_atualizados = []

        for it in itens_novos_raw:
            num = it.get('numeroItem')
            desc = it.get('descricao', '')
            
            # --- STATUS ---
            sit_id = safe_int(it.get('situacaoCompraItem'), 1)
            sit_nome = it.get('situacaoCompraItemName', 'EM ANDAMENTO')
            status_final = MAPA_SITUACAO.get(sit_id, sit_nome).upper()
            
            # --- BENEFÍCIO (A CORREÇÃO) ---
            # Pega o que veio na lista
            benef_final = extrair_beneficio(it)

            # A REGRA DE OURO: Se a lista diz 4 (Amplo), NÃO ACREDITE.
            # Vá no detalhe do item conferir.
            if benef_final == 4:
                try:
                    url_det = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}'
                    r_det = session.get(url_det, timeout=10)
                    if r_det.status_code == 200:
                        detalhe = r_det.json()
                        benef_real = extrair_beneficio(detalhe)
                        
                        # Se o detalhe diz que é 1, 2 ou 3, ele vence.
                        if benef_real != 4:
                            benef_final = benef_real
                            # print(f"✅ CORRIGIDO via Detalhe: Item {num} é {benef_real}")
                except: 
                    pass # Se falhar a conexão, paciência, tenta o texto

            # Se ainda assim for 4, tenta salvar pelo texto
            if benef_final == 4:
                benef_texto = inferir_beneficio_por_texto(desc)
                if benef_texto:
                    benef_final = benef_texto

            # --- RESULTADO ---
            res_forn = None
            res_val = 0.0
            
            if it.get('temResultado') or sit_id == 2:
                try:
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados"
                    r_res = session.get(url_res, timeout=10)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        if isinstance(rl, list) and len(rl) > 0:
                            res_obj = rl[0]
                            res_forn = res_obj.get('nomeRazaoSocialFornecedor') or res_obj.get('razaoSocial')
                            res_val = safe_float(res_obj.get('valorUnitarioHomologado'))
                            if sit_id == 1 and res_forn: status_final = "HOMOLOGADO"
                except: pass

            itens_atualizados.append({
                'n': num,
                'd': desc,
                'q': safe_float(it.get('quantidade')),
                'u': it.get('unidadeMedida', ''),
                'v_est': safe_float(it.get('valorUnitarioEstimado')),
                'benef': benef_final,
                'sit': status_final,
                'res_forn': res_forn,
                'res_val': res_val
            })
        
        dados_novos = dados_antigos.copy()
        dados_novos['itens'] = itens_atualizados
        return dados_novos

    except Exception: return None

# --- EXECUÇÃO ---

if not os.path.exists(ARQDADOS): exit()

print("🩺 Auditoria Profunda (Lista + Detalhe Obrigatório) Iniciada...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    banco_raw = json.load(f)

if isinstance(banco_raw, list): banco_dict = {item['id']: item for item in banco_raw}
else: banco_dict = banco_raw

session = criar_sessao()

# Seleciona TODOS que tem Amplo(4) ou estão Abertos
alvos = [lid for lid, d in banco_dict.items() if precisa_atualizar(d)]

print(f"📊 Banco Total: {len(banco_dict)}")
print(f"🎯 Alvos para Revalidação: {len(alvos)}")

atualizados = 0
erros = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
    future_to_id = {exe.submit(atualizar_licitacao, lid, banco_dict[lid], session): lid for lid in alvos}
    
    for future in concurrent.futures.as_completed(future_to_id):
        lid = future_to_id[future]
        try:
            res = future.result()
            if res:
                banco_dict[lid] = res
                atualizados += 1
            else: erros += 1
        except: erros += 1

print(f"💾 Salvando...")
print(f"   ✅ Registros Processados: {atualizados}")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(list(banco_dict.values()), f, ensure_ascii=False)

print("🏁 Concluído.")
