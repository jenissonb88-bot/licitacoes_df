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

# --- GEOGRAFIA DE PRECISÃO (Sua estrutura original restaurada) ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
UFS_PERMITIDAS_MED = NE_ESTADOS + ['DF', 'ES', 'MG', 'RJ', 'SP', 'GO', 'MT', 'MS', 'AM', 'PA', 'TO', 'BR', '']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- DIRETRIZES REAVALIADAS (Vetos e Whitelists) ---
# Retirados "SISTEMA" e "MANUTENCAO" para não bloquear Registro de Preços
VETOS_ABSOLUTOS = [normalize(x) for x in [
    "SOFTWARE", "IMPLANTACAO", "LICENCA", "COMPUTADOR", "INFORMATICA", "IMPRESSAO",
    "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "LOCACAO", "ASSINATURA", "LIMPEZA",
    "EXAME", "RADIOLOGIA", "IMAGEM", "VIGILANCIA", "SEGURANCA", "OFICINA",
    "GASES MEDICINAIS", "OXIGENIO", "ODONTOLOGICO", "PROTESE", "ORTESE", "CILINDRO",
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "ALIMENTACAO ESCOLAR", "MERENDA"
]]

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA"]]
WL_NUTRI_MMH = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "DIETA ENTERAL", "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "GAZE", "SONDA", "LUVA"]]
WL_TERMOS_VAGOS = [normalize(x) for x in ["SAUDE", "HOSPITAL", "MATERNIDADE", "CLINICA", "FUNDO MUNICIPAL", "SECRETARIA DE"]]

def log_mensagem(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/24.5'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_todos_os_itens(cnpj, ano, seq, session):
    """Garante que todos os itens sejam baixados, mesmo que passem de 500"""
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

        # 1. Guilhotina Geográfica e de Vetos
        if uf in ESTADOS_BLOQUEADOS: return ('IGNORADO', f"Estado Bloqueado: {uf}")
        for v in VETOS_ABSOLUTOS:
            if v in obj_norm: return ('IGNORADO', f"Veto Absoluto encontrado: {v}")

        # 2. Radar de Interesse
        tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
        tem_mmh = any(t in obj_norm for t in WL_NUTRI_MMH)
        tem_vago = any(t in obj_norm for t in WL_TERMOS_VAGOS)

        # 3. Validação Geográfica Específica
        precisa_checar_itens = False
        if tem_med:
            if uf not in UFS_PERMITIDAS_MED: return ('IGNORADO', f"Medicamento fora da área: {uf}")
        elif tem_mmh:
            if uf not in UFS_PERMITIDAS_MMH: return ('IGNORADO', f"MMH/Nutrição fora da área: {uf}")
        elif tem_vago:
            precisa_checar_itens = True # Título muito aberto (ex: Fundo de Saúde). O Dicionário vai decidir.
        else:
            return ('IGNORADO', "Fora da temática farmacêutica/hospitalar")

        # 4. Busca de Itens Exaustiva
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        itens_brutos = buscar_todos_os_itens(cnpj, ano, seq, session)
        if not itens_brutos: return ('ERRO', "Falha ao baixar itens")

        teve_match = False
        itens_mapeados = []
        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            
            # Checa match com o Dicionário de Ouro
            if not teve_match and any(termo in desc_item for termo in termos_ouro):
                teve_match = True
            
            # Formato enxuto para o Index (1)
            itens_mapeados.append({
                'n': it.get('numeroItem'), 'd': it.get('descricao', ''),
                'q': it.get('quantidade'), 'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0), 'sit': 'EM ANDAMENTO'
            })

        # 5. Juiz Final (Dicionário de Ouro)
        if precisa_checar_itens and not teve_match:
            return ('IGNORADO', "Título vago e nenhum item do dicionário encontrado")

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': lic.get('dataEncerramentoProposta'),
            'uf': uf, 'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'),
            'obj': obj_raw, 'edit': edit, 'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'itens': itens_mapeados, 'sit_global': 'DIVULGADA'
        }
        return ('CAPTURADO', dados_finais)

    except Exception as e: return ('ERRO', str(e))

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")

    try:
        # Pega a data por argumento no YML ou usa Checkpoint/Hoje
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        if args.start:
            data_alvo = args.start
        elif os.path.exists(ARQ_CHECKPOINT):
            with open(ARQ_CHECKPOINT, 'r') as f: data_alvo = f.read().strip()
        else: 
            data_alvo = date.today().strftime('%Y-%m-%d')

        log_mensagem(f"🚀 Iniciando Varredura Estratégica: {data_alvo}")
        
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            termos_ouro = [normalize(t) for t in json.load(f)]

        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        dia_api = data_alvo.replace('-', '')
        pagina = 1
        total_capturado_hoje = 0
        
        while True:
            r = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao", 
                            params={'dataInicial': dia_api, 'dataFinal': dia_api, 'codigoModalidadeContratacao': 6, 'pagina': pagina, 'tamanhoPagina': 50})
            if r.status_code != 200: break
            lics = r.json().get('data', [])
            if not lics: break
            
            log_mensagem(f"📄 Pag {pagina}: Processando {len(lics)} editais no PNCP...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session, termos_ouro): l for l in lics}
                for f in concurrent.futures.as_completed(futuros):
                    l = futuros[f]
                    status, resultado = f.result()
                    
                    if status == 'CAPTURADO':
                        banco[resultado['id']] = resultado
                        total_capturado_hoje += 1
                        log_mensagem(f"   🎯 ALVO: {resultado['edit']} - {resultado['org'][:30]}")

            if len(lics) < 50: break
            pagina += 1

        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        
        # Se estiver usando o checkpoint, avança um dia
        if not args.start:
            proximo = (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
            
        log_mensagem(f"🏁 Dia {data_alvo} concluído. Total Capturado: {total_capturado_hoje}")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
