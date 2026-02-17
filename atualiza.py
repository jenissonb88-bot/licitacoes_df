import requests
import json
import gzip
import os
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
MAXWORKERS = 4  # Reduzido levemente para permitir a dupla checagem sem bloquear

# --- MAPAS OFICIAIS ---
MAPA_SITUACAO = {
    1: "EM ANDAMENTO",
    2: "HOMOLOGADO",
    3: "CANCELADO",
    4: "DESERTO",
    5: "FRACASSADO"
}

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma Auditor/9.5'})
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except: return 0.0

def safe_int(val, default=4):
    try:
        if val is None: return default
        return int(val)
    except: return default

def precisa_atualizar(licitacao):
    """
    Critério: Atualiza se houver itens 'Abertos' ou se suspeitarmos de dados incompletos.
    """
    itens = licitacao.get('itens', [])
    if not itens: return True 

    for it in itens:
        # Se o status indicar que ainda está vivo
        sit = str(it.get('sit', '')).upper()
        if sit in ['ABERTO', 'EM ANDAMENTO', '']:
            return True
    return False

def atualizar_licitacao(lic_id, dados_antigos, session):
    try:
        # Desmonta o ID (CNPJ + ANO + SEQ)
        # Ex: 08109444000171202626 -> CNPJ=14, ANO=4, SEQ=Resto
        cnpj = lic_id[:14]
        ano = lic_id[14:18]
        seq = lic_id[18:]

        # 1. Busca a lista geral de itens
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        
        if r.status_code != 200: return None

        resp_json = r.json()
        itens_novos_raw = resp_json.get('data', []) if isinstance(resp_json, dict) else resp_json
        if not itens_novos_raw: return None

        itens_atualizados = []
        mudanca_beneficio = False

        for it in itens_novos_raw:
            num = it.get('numeroItem')
            desc = it.get('descricao', '')
            
            # --- CAPTURA INICIAL ---
            sit_id = safe_int(it.get('situacaoCompraItem'), 1)
            sit_nome_api = it.get('situacaoCompraItemName', 'EM ANDAMENTO')
            status_final = MAPA_SITUACAO.get(sit_id, sit_nome_api).upper()
            
            benef_id = safe_int(it.get('tipoBeneficioId'), 4)

            # --- A DUPLA CHECAGEM (O Pulo do Gato) ---
            # Se a lista diz que é "Ampla" (4) e o item ainda está aberto,
            # vamos consultar o detalhe do item para ter certeza absoluta.
            if benef_id == 4 and sit_id == 1:
                try:
                    url_detalhe = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}'
                    r_detalhe = session.get(url_detalhe, timeout=10)
                    if r_detalhe.status_code == 200:
                        detalhe = r_detalhe.json()
                        benef_real = safe_int(detalhe.get('tipoBeneficioId'), 4)
                        
                        if benef_real != 4:
                            # print(f"🔧 CORREÇÃO: Item {num} do edital {lic_id} mudou de Ampla para Benefício {benef_real}")
                            benef_id = benef_real
                            mudanca_beneficio = True
                except:
                    pass # Se falhar o detalhe, mantém o da lista

            # --- BUSCA RESULTADO ---
            res_fornecedor = None
            res_valor = 0.0
            
            if it.get('temResultado') or sit_id == 2:
                try:
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados"
                    r_res = session.get(url_res, timeout=10)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        if isinstance(rl, list) and len(rl) > 0:
                            res_obj = rl[0]
                            res_fornecedor = res_obj.get('nomeRazaoSocialFornecedor') or res_obj.get('razaoSocial')
                            res_valor = safe_float(res_obj.get('valorUnitarioHomologado'))
                            
                            if sit_id == 1 and res_fornecedor:
                                status_final = "HOMOLOGADO"
                except: pass

            itens_atualizados.append({
                'n': num,
                'd': desc,
                'q': safe_float(it.get('quantidade')),
                'u': it.get('unidadeMedida', ''),
                'v_est': safe_float(it.get('valorUnitarioEstimado')),
                'benef': benef_id, # Agora duplamente checado
                'sit': status_final,
                'res_forn': res_fornecedor,
                'res_val': res_valor
            })
        
        dados_novos = dados_antigos.copy()
        dados_novos['itens'] = itens_atualizados
        return dados_novos

    except Exception:
        return None

# --- EXECUÇÃO PRINCIPAL ---

if not os.path.exists(ARQDADOS):
    print("❌ Banco de dados não encontrado.")
    exit()

print("🩺 Iniciando Auditoria Profunda de Benefícios...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    banco_raw = json.load(f)

# Garante formato de dicionário para processamento
if isinstance(banco_raw, list):
    banco_dict = {item['id']: item for item in banco_raw}
else:
    banco_dict = banco_raw

session = criar_sessao()
alvos = []

# Seleciona alvos para revalidação
for lid, dados in banco_dict.items():
    if precisa_atualizar(dados):
        alvos.append(lid)

print(f"📊 Banco Total: {len(banco_dict)} | 🎯 Alvos para Revalidação: {len(alvos)}")

atualizados = 0
erros = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as executor:
    future_to_id = {executor.submit(atualizar_licitacao, lid, banco_dict[lid], session): lid for lid in alvos}
    
    for future in concurrent.futures.as_completed(future_to_id):
        lid = future_to_id[future]
        try:
            res = future.result()
            if res:
                banco_dict[lid] = res
                atualizados += 1
            else:
                erros += 1
        except:
            erros += 1

print(f"💾 Salvando correções...")
print(f"   ✅ Registros Atualizados/Corrigidos: {atualizados}")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(list(banco_dict.values()), f, ensure_ascii=False)

print("🏁 Auditoria concluída.")
