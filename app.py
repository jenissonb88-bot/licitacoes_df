import requests
import json
import os
import unicodedata
import gzip
import concurrent.futures
import sys
import time
import random
import re
from datetime import datetime, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES DE ARQUIVOS ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz' 
ARQ_DICIONARIO = 'dicionario_ouro.json'
ARQ_LOCK = 'execucao.lock'
ARQ_LOG = 'log_captura.txt'

# Limite de threads para não sobrecarregar a API do Governo
MAXWORKERS = 4 

# --- GEOGRAFIA DE PRECISÃO ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
UFS_PERMITIDAS_MED = NE_ESTADOS + ['DF', 'ES', 'MG', 'RJ', 'SP', 'GO', 'MT', 'MS', 'AM', 'PA', 'TO', 'BR', '']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# 🛑 MURALHA 1: VETOS ABSOLUTOS DO EDITAL (O Objeto Inteiro Morre Aqui)
VETOS_ABSOLUTOS = [normalize(x) for x in [
    # Serviços, TI e Administrativo
    "SOFTWARE", "IMPLANTACAO", "LICENCA", "COMPUTADOR", "INFORMATICA", "IMPRESSAO",
    "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "LOCACAO", "ASSINATURA", "LIMPEZA",
    "VIGILANCIA", "SEGURANCA", "OFICINA", "BUFFET", "EVENTOS", "HOSPEDAGEM", "PASSAGENS",
    "FARDAMENTO", "MARMITEX", "REFEICAO", "LAVANDERIA", "PUBLICIDADE",
    
    # Obras, Frota e Alimentação Geral
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "ALIMENTACAO ESCOLAR", "MERENDA", "CESTA BASICA",
    "AGRICULTURA", "VEICULOS", "FROTA", "MANUTENCAO PREVENTIVA", "PECAS AUTOMOTIVAS",
    
    # Saúde (Fora do escopo do seu projeto)
    "EXAME", "RADIOLOGIA", "IMAGEM", "GASES MEDICINAIS", "OXIGENIO", 
    "ODONTOLOGICO", "PROTESE", "ORTESE", "CILINDRO",
    
    # Vetos Administrativos (Adesão, IRP, etc)
    "ADESAO", "IRP", "INTENCAO DE REGISTRO", "CREDENCIAMENTO", "LEILAO", "CHAMAMENTO PUBLICO"
]]

# 🛑 MURALHA 2: BLACKLIST DE ITENS (O "Pão Doce" Morre Aqui)
BLACKLIST_ITENS = [
    "BISCOITO", "FARINHA", "ACUCAR", "DOCE", "BOLO", "MISTURA", "RACAO", "PAO", "ARROZ", 
    "MACARRAO", "LEITE", "SUCO", "ALIMENTO", "CESTA", "HORTIFRUTI", "CARNE", "FRANGO", 
    "PEIXE", "FEIJAO", "CAFE", "ACHOCOLATADO", "SOPA", "POLPA", "IOGURTE", "BEBIDA", "FRUTA",
    "AMBULANCIA", "PNEU", "PAPEL", "CANETA", "TONER", "CARTUCHO", "IMPRESSORA", "MOTO", 
    "TRATOR", "ESCOLA", "DETERGENTE", "SABAO", "CIMENTO", "ASFALTO", "TINTA", "LIXO", 
    "FUNERARI", "URNA", "COPO", "DESCARTAVEL", "CADEIRA", "MESA", "GRAMPEADOR", "MOCHILA"
]
REGEX_BLACKLIST_ITENS = re.compile(r'\b(?:' + '|'.join(BLACKLIST_ITENS) + r')\b')

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
    s.headers.update({
        'Accept': 'application/json', 
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9'
    })
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_todos_os_itens(cnpj, ano, seq, session):
    todos_itens = []
    pagina_item = 1
    erro_msg = None
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            time.sleep(random.uniform(0.6, 1.2)) # Pausa para evitar bloqueio de IP
            r = session.get(url, params={'pagina': pagina_item, 'tamanhoPagina': 500}, timeout=30)
            if r.status_code != 200: 
                erro_msg = f"HTTP {r.status_code}"
                break
            json_resp = r.json()
            dados = json_resp if isinstance(json_resp, list) else json_resp.get('data', [])
            if not dados: break
            todos_itens.extend(dados)
            if len(dados) < 500: break
            pagina_item += 1
        except Exception as e:
            erro_msg = str(e)[:50]
            break
    return todos_itens, erro_msg

def processar_licitacao(lic, session, termos_ouro):
    try:
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or 'BR').upper().strip()
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)
        edit = f"{lic.get('numeroCompra')}/{lic.get('anoCompra')}"

        # 1. Filtros Iniciais (Geo e Muralha 1 do Objeto)
        if uf in ESTADOS_BLOQUEADOS: return ('VETO_GEO', None)
        for v in VETOS_ABSOLUTOS:
            if v in obj_norm: return ('VETO_TITULO', None)

        tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
        tem_mmh = any(t in obj_norm for t in WL_NUTRI_MMH)
        tem_vago = any(t in obj_norm for t in WL_TERMOS_VAGOS)

        precisa_checar_itens = False
        if tem_med:
            if uf not in UFS_PERMITIDAS_MED: return ('VETO_GEO', None)
        elif tem_mmh:
            if uf not in UFS_PERMITIDAS_MMH: return ('VETO_GEO', None)
        elif tem_vago:
            precisa_checar_itens = True
        else:
            return ('FORA_TEMATICA', None)

        # 2. Busca de Itens Detalhada
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        itens_brutos, erro_api = buscar_todos_os_itens(cnpj, ano, seq, session)
        
        if erro_api: return ('ERRO_API', f"{edit} -> {erro_api}")

        teve_match = False
        itens_mapeados = []
        
        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            
            # 🛑 MURALHA 2: Elimina comida, veículos e material de escritório
            if REGEX_BLACKLIST_ITENS.search(desc_item):
                continue
            
            # Verifica Match com o Dicionário de Fármacos
            if not teve_match and any(termo in desc_item for termo in termos_ouro):
                teve_match = True
            
            # Captura precisa do Tipo de Benefício ME/EPP
            try:
                cod_benef = int(it.get('tipoBeneficio', 5))
            except:
                cod_benef = 5

            itens_mapeados.append({
                'n': it.get('numeroItem'), 
                'd': it.get('descricao', ''),
                'q': it.get('quantidade'), 
                'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0), 
                'benef': cod_benef
            })

        if precisa_checar_itens and not teve_match:
            return ('VETO_DICIONARIO', None)
            
        # Se todos os itens foram eliminados pela Blacklist (ex: edital só de merenda)
        if not itens_mapeados:
            return ('FORA_TEMATICA', None)

        # 3. Dados Finais para o Banco de Dados
        cid = uo.get('municipioNome', '---')
        uasg = uo.get('codigoUnidade', 'N/A')
        unid_nome = uo.get('nomeUnidade', '---')

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 
            'dt_enc': lic.get('dataEncerramentoProposta'),
            'uf': uf, 
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'),
            'cid': cid, 'uasg': uasg, 'unid_nome': unid_nome,
            'obj': obj_raw, 'edit': edit, 
            'sit_global': lic.get('situacaoCompraNome', 'EM ANDAMENTO'), # <--- NOVA LINHA ADICIONADA AQUI
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'itens': itens_mapeados
        }
        return ('CAPTURADO', dados_finais)

    except Exception: return ('ERRO_API', "Falha Interna")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")

    try:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        data_alvo = args.start if args.start else date.today().strftime('%Y-%m-%d')
        log_mensagem(f"🚀 Sniper Iniciado: {data_alvo}")
        
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            termos_ouro = [normalize(t) for t in json.load(f)]

        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        dia_api = data_alvo.replace('-', '')
        pagina = 1
        stats = {'CAPTURADO': 0, 'VETO_GEO': 0, 'VETO_TITULO': 0, 'VETO_DICIONARIO': 0, 'FORA_TEMATICA': 0, 'ERRO_API': 0}
        
        while True:
            r = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao", 
                            params={'dataInicial': dia_api, 'dataFinal': dia_api, 'codigoModalidadeContratacao': 6, 'pagina': pagina, 'tamanhoPagina': 50})
            if r.status_code != 200: break
            lics = r.json().get('data', [])
            if not lics: break
            
            log_mensagem(f"📄 Pag {pagina}: Analisando {len(lics)} editais...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session, termos_ouro): l for l in lics}
                for f in concurrent.futures.as_completed(futuros):
                    status, resultado = f.result()
                    stats[status] += 1
                    
                    if status == 'CAPTURADO':
                        # Detetor de Alteração de Data
                        if resultado['id'] in banco:
                            antiga = banco[resultado['id']].get('dt_enc')
                            if antiga and resultado['dt_enc'] and antiga != resultado['dt_enc']:
                                resultado['alerta_data'] = True
                                resultado['dt_enc_antiga'] = antiga
                        banco[resultado['id']] = resultado

            if len(lics) < 50: break
            pagina += 1

        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
            
        log_mensagem(f"✅ Finalizado: {stats['CAPTURADO']} capturados de {sum(stats.values())} analisados.")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
