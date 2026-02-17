import requests
import json
import gzip
import os
import unicodedata
import concurrent.futures
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
MAXWORKERS = 5 # Menos workers para não bloquear a API, já que faremos muitas consultas pontuais

# --- MAPAS OFICIAIS (MANUAL PNCP) ---
MAPA_SITUACAO = {
    1: "EM ANDAMENTO",
    2: "HOMOLOGADO",
    3: "CANCELADO",
    4: "DESERTO",
    5: "FRACASSADO"
}

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Auditoria)'})
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except: return 0.0

def safe_int(val, default=4):
    try:
        return int(val) if val is not None else default
    except: return default

def precisa_atualizar(licitacao):
    """
    Decide se vale a pena gastar uma requisição para atualizar este edital.
    Critério: Se houver pelo menos 1 item que NÃO esteja em estado terminal.
    Estados Terminais: HOMOLOGADO (2), CANCELADO (3), DESERTO (4), FRACASSADO (5).
    """
    itens = licitacao.get('itens', [])
    if not itens: return True # Se não tem itens, tenta buscar (pode ter sido erro de captura)

    for it in itens:
        # Verifica se o status textual indica que ainda está aberto
        sit = str(it.get('sit', '')).upper()
        if sit in ['ABERTO', 'EM ANDAMENTO', '']:
            return True
            
        # Verificação extra: Se for "Aberto" no nosso banco, precisa atualizar
        # Se já estiver HOMOLOGADO, DESERTO, FRACASSADO ou CANCELADO, consideramos finalizado.
    
    return False

def atualizar_licitacao(lic_id, dados_antigos, session):
    try:
        # Desmonta o ID para criar a URL (ID formato: CNPJ+ANO+SEQUENCIAL)
        # Ex: 123456780001992025123 -> CNPJ=14chars, ANO=4chars, SEQ=resto
        cnpj = lic_id[:14]
        ano = lic_id[14:18]
        seq = lic_id[18:]

        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        
        if r.status_code != 200:
            return None # Falha na conexão, mantem o antigo

        resp_json = r.json()
        itens_novos_raw = resp_json.get('data', []) if isinstance(resp_json, dict) else resp_json

        if not itens_novos_raw:
            return None

        itens_atualizados = []
        homologados_count = 0

        for it in itens_novos_raw:
            if not isinstance(it, dict): continue

            num = it.get('numeroItem')
            desc = it.get('descricao', '')
            
            # --- ATUALIZAÇÃO DE STATUS (MAPA OFICIAL) ---
            sit_id = safe_int(it.get('situacaoCompraItem'), 1)
            sit_nome_api = it.get('situacaoCompraItemName', 'EM ANDAMENTO')
            status_final = MAPA_SITUACAO.get(sit_id, sit_nome_api).upper()

            # --- ATUALIZAÇÃO DE ME/EPP ---
            # Força a leitura correta do ID
            benef_id = safe_int(it.get('tipoBeneficioId'), 4)
            
            # --- BUSCA RESULTADO (Se necessário) ---
            res_fornecedor = None
            res_valor = 0.0
            
            # Se o status diz que tem resultado (2) ou a flag está True
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
                                status_final = "HOMOLOGADO" # Corrige delay do status
                except: pass
            
            if status_final == "HOMOLOGADO": homologados_count += 1

            # Reconstrói o item mantendo a estrutura usada pelo sistema
            # Importante: Mantemos a descrição original para não quebrar filtros do limpeza.py
            # A menos que queiramos atualizar a descrição também.
            itens_atualizados.append({
                'n': num,
                'd': desc, # Atualiza descrição caso tenha mudado
                'q': safe_float(it.get('quantidade')),
                'u': it.get('unidadeMedida', ''),
                'v_est': safe_float(it.get('valorUnitarioEstimado')),
                'benef': benef_id, # Salva o ID bruto para o limpeza.py tratar
                'sit': status_final,
                'res_forn': res_fornecedor,
                'res_val': res_valor
            })
        
        # Preserva os dados de cabeçalho antigos, atualiza apenas os itens e valores
        dados_novos = dados_antigos.copy()
        dados_novos['itens'] = itens_atualizados
        # Opcional: Atualizar valor total se disponível na API de compra, mas itens é o foco
        
        return dados_novos

    except Exception as e:
        # print(f"Erro ao atualizar {lic_id}: {e}")
        return None # Em caso de erro, não mexe no registro

# --- EXECUÇÃO PRINCIPAL ---

if not os.path.exists(ARQDADOS):
    print("❌ Banco de dados não encontrado.")
    exit()

print("🩺 Iniciando Auditoria de Atualização (Dr. Atualiza)...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    banco = json.load(f)

session = criar_sessao()
ids_para_atualizar = []

# 1. Triagem: Quem precisa de médico?
for lic_id, dados in enumerate(banco):
    # O banco é uma lista de dicts, mas precisamos iterar.
    # Se for dict (id -> dados), iteramos keys.
    # O formato salvo pelo app.py geralmente é LISTA.
    pass

# Ajuste para formato Lista vs Dict
if isinstance(banco, list):
    # Converte para dict temporário para fácil acesso
    banco_dict = {item['id']: item for item in banco}
else:
    banco_dict = banco

print(f"📊 Total de registros no banco: {len(banco_dict)}")

# Identifica alvos
alvos = []
for lid, dados in banco_dict.items():
    if precisa_atualizar(dados):
        alvos.append(lid)

print(f"🎯 Alvos identificados para atualização: {len(alvos)}")

# 2. Atualização Concorrente
atualizados_count = 0
erros_count = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as executor:
    # Cria dicionário {future: lic_id}
    future_to_id = {executor.submit(atualizar_licitacao, lid, banco_dict[lid], session): lid for lid in alvos}
    
    for future in concurrent.futures.as_completed(future_to_id):
        lid = future_to_id[future]
        try:
            resultado = future.result()
            if resultado:
                banco_dict[lid] = resultado # Sobrescreve com dados novos
                atualizados_count += 1
            else:
                erros_count += 1 # Mantém o antigo
        except Exception:
            erros_count += 1

# 3. Salva o Banco Curado
print(f"💾 Salvando banco atualizado...")
print(f"   ✅ Atualizados com sucesso: {atualizados_count}")
print(f"   ⚠️ Falhas/Mantidos antigos: {erros_count}")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(list(banco_dict.values()), f, ensure_ascii=False)

print("🏁 Auditoria concluída.")
