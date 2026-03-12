import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import csv
import re
import concurrent.futures
import time
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_TEMP = ARQDADOS + '.tmp'
ARQ_CHECKPOINT = 'checkpoint.json'
ARQ_LOCK = 'execucao.lock'
ARQ_CATALOGO = 'Exportar Dados.csv'
ARQ_LOG = 'log_captura.txt'
MAXWORKERS = 15
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- API OFICIAL PNCP ---
API_BASE = "https://pncp.gov.br/api/pncp/v1"
API_CONSULTA = "https://pncp.gov.br/api/consulta/v1"

# --- GEOGRAFIA E REGRAS DE NEGÓCIO ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- CATÁLOGOS DE VETO E ACEITE ---
VETOS_ALIMENTACAO = [normalize(x) for x in ["ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE", "PERECIVEIS", "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS", "LANCHE", "REFEICOES", "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE", "POLPA DE FRUTA", "ESTIAGEM"]]
VETOS_EDUCACAO = [normalize(x) for x in ["MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS", "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO", "SECRETARIA DE EDUCACAO"]]
VETOS_OPERACIONAL = [normalize(x) for x in ["OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO", "CORRETIVA", "VEICULO", "AMBULANCIA", "MOTOCICLETA", "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO", "EQUIPAMENTO E MATERIA PERMANENTE", "RECARGA", "ASFATIC", "CONFECCAO"]]
VETOS_ADM = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "CREDENCIAMENTO", "LEILAO", "ALIENACAO"]]
TODOS_VETOS = VETOS_ALIMENTACAO + VETOS_EDUCACAO + VETOS_OPERACIONAL + VETOS_ADM

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO", "ACETILCISTEINA", "ACETILSALICILICO", "ACICLOVIR", "ADENOSINA", "ADRENALINA", "ALBENDAZOL", "ALENDRONATO", "ALFAEPOETINA", "ALFAINTERFERONA", "ALFAST", "ALOPURINOL", "ALPRAZOLAM", "AMBROXOL", "AMBROXOL XPE", "AMINOFILINA", "AMIODARONA", "AMITRIPTILINA", "AMOXICILINA", "AMPICILINA", "ANASTROZOL", "ANFOTERICINA", "ANLODIPINO", "ARIPIPRAZOL", "ATENOLOL", "ATORVASTATINA", "ATORVASTATINA CALCICA", "ATRACURIO", "ATROPINA", "AZITROMICINA", "AZTREONAM", "BACLOFENO", "BAMIFILINA", "BENZILPENICILINA", "BENZOATO", "BETAMETASONA", "BEZAFIBRATO", "BIMATOPROSTA", "BISACODIL", "BISSULFATO", "BOPRIV", "BROMOPRIDA", "BUDESONIDA", "BUPROPIONA", "BUTILBROMETO", "CABERGOLINA", "CALCITRIOL", "CANDESARTANA", "CAPTOPRIL", "CARBAMAZEPINA", "CARBONATO", "CARVEDILOL", "CAVERDILOL", "CEFALEXINA", "CEFALOTINA", "CEFAZOLINA", "CEFEPIMA", "CEFOTAXIMA", "CEFOXITINA", "CEFTAZIDIMA", "CEFTRIAXONA", "CEFUROXIMA", "CETOCONAZOL", "CETOPROFENO", "CETOROLACO", "CICLOBENZAPRINA", "CICLOSPORINA", "CILOSTAZOL", "CIMETIDINA", "CIPROFLOXACINO", "CIPROFLOXACINA", "CITALOPRAM", "CLARITROMICINA", "CLINDAMICINA", "CLOBETASOL", "CLOMIPRAMINA", "CLONAZEPAM", "CLONIDINA", "CLOPIDOGREL", "CLORETO", "CLORIDRATO", "CLORPROMAZINA", "CLORTALIDONA", "CLOTRIMAZOL", "CLOZAPINA", "CODEINA", "COLCHICINA", "COLECALCIFEROL", "COLISTIMETATO", "COMPLEXO B", "DACARBZINA", "DAPAGLIFLOZINA", "DAPSONA", "DAPTOMICINA", "DARBEPOETINA", "DESLANOSIDEO", "DESLORATADINA", "DEXAMETASONA", "DEXCLORFENIRAMINA", "DEXPANTENOL", "DIAZEPAM", "DIETILAMONIO", "DICLOFENACO", "DIGOXINA", "DILTIAZEM", "DIMETICONA", "DIOSMINA", "DIPIRONA", "DOBUTAMINA", "DOMPERIDONA", "DONEPEZILA", "DOPAMINA", "DOXAZOSINA", "DOXICICLINA", "DROPERIDOL", "DULAGLUTIDA", "DULOXETINA", "DUTASTERIDA", "ECONAZOL", "EMULSAO", "ENALAPRIL", "ENOXAPARINA", "ENTACAPONA", "EPINEFRINA", "ERITROMICINA", "ESCITALOPRAM", "ESOMEPRAZOL", "ESPIRONOLACTONA", "ESTRADIOL", "ESTRIOL", "ESTROGENIOS", "ETANERCEPTE", "ETILEFRINA", "ETOMIDATO", "ETOPOSIDEO", "EZETIMIBA", "FAMOTIDINA", "FENITOINA", "FENOBARBITAL", "FENOTEROL", "FENTANILA", "FERRO", "FIBRINOGENIO", "FILGRASTIM", "FINASTERIDA", "FITOMENADIONA", "FLUCONAZOL", "FLUDROCORTISONA", "FLUMAZENIL", "FLUNARIZINA", "FLUOXETINA", "FLUTICASONA", "FOLATO", "FONDAPARINUX", "FORMOTEROL", "FOSFATO", "FUROSEMIDA", "GABAPENTINA", "GANCICLOVIR", "GELADEIRA", "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICEROL", "GLICLAZIDA", "GLICOSE", "GLIMEPIRIDA", "GLUCAGON", "HALOPERIDOL", "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA", "HIDROTALCITA", "HIDROXIDOPROGESTERONA", "HIDROXIDO", "HIDROXIPROGESTERONA", "HIDROXIUREIA", "HIOSCINA", "HIPROMELOSE", "IBUPROFENO", "IMIPENEM", "IMIPRAMINA", "INDAPAMIDA", "INSULINA", "IOIMBINA", "IPRATROPIO", "IRBESARTANA", "IRINOTECANO", "ISOSSORBIDA", "ISOTRETINOINA", "ITRACONAZOL", "IVERMECTINA", "LACTULOSE", "LAMOTRIGINA", "LANSOPRAZOL", "LATANOPROSTA", "LEFLUNOMIDA", "LERCANIDIPINO", "LETROZOL", "LEVODOPA", "LEVOFLOXACINO", "LEVOMEPROMAZINA", "LEVONORGESTREL", "LEVOTIROXINA", "LIDOCAINA", "LINEZOLIDA", "LINOGLIPTINA", "LIPIDICA", "LISINOPRIL", "LITIO", "LOPERAMIDA", "LORATADINA", "LORAZEPAM", "LOSARTANA", "LOVASTATINA", "MAGNESIO", "MANITOL", "MEBENDAZOL", "MEDROXIPROGESTERONA", "MEMANTINA", "MEROPENEM", "MESALAZINA", "METILDOPA", "METILPREDNISOLONA", "METOCLOPRAMIDA", "METOPROLOL", "METOTREXATO", "METRONIDAZOL", "MICOFENOLATO", "MIDAZOLAM", "MIRTAZAPINA", "MISOPROSTOL", "MORFINA", "MUPIROCINA", "NARATRIPTANA", "NEOMICINA", "NEOSTIGMINA", "NIFEDIPINO", "NIMESULIDA", "NIMODIPINO", "NISTATINA", "NITROFURANTOINA", "NITROGLICERINA", "NITROPRUSSIATO", "NORETISTERONA", "NORFLOXACINO", "NORTRIPTILINA", "OCTREOTIDA", "OLANZAPINA", "OLMESARTANA", "OMEPRAZOL", "ONDANSETRONA", "OXALIPLATINA", "OXCARBAZEPINA", "OXIBUTININA", "PACLITAXEL", "PALONOSETRONA", "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", "PENICILINA", "PERICIAZINA", "PERMETRINA", "PETIDINA", "PIRAZINAMIDA", "PIRIDOSTIGMINA", "PIRIDOXINA", "POLIMIXINA", "POLIVITAMINICO", "POTASSIO", "PRAMIPEXOL", "PRAVASTATINA", "PREDNISOLONA", "PREDNISONA", "PREGABALINA", "PROMETAZINA", "PROPATILNITRATO", "PROPOFOL", "PROPRANOLOL", "PROSTIGMINA", "QUETIAPINA", "RAMIPRIL", "RANITIDINA", "RESERPINA", "RIFAMPICINA", "RISPERIDONA", "RITONAVIR", "RIVAROXABANA", "ROCURONIO", "ROSUVASTATINA", "SACARATO", "SALBUTAMOL", "SECAM", "SERTRALINA", "SEVELAMER", "SINVASTATINA", "SODIO", "SUCCINILCOLINA", "SUCRALFATO", "SULFADIAZINA", "SULFAMETOXAZOL", "SULFATO", "SULPIRIDA", "SUXAMETONIO", "TAMOXIFENO", "TANSULOSINA", "TEMOZOLAMIDA", "TEMOZOLOMIDA", "TENOXICAN", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIOPENTAL", "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL", "TRAVOPROSTA", "TRIMETOPRIMA", "TROMETAMOL", "TROPICAMIDA", "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASELINA","AQUISICAO DE MEDICAMENTO", "HEMODIALISE", "DIALISE", "TERAPIA RENAL", "AMINOACIDO", "AMINOACIDOS", "FOSFORO"]]
WL_NUTRI_CLINICA = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL", "PARENTERA","ENTERA"]]
WL_MATERIAIS_NE = [normalize(x) for x in ["MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO", "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE", "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS", "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL", "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO", "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO", "TOUCA DESCARTAVEL", "TUBO ASPIRACAO", "CORRELATO", "AGULHAS", "SERINGAS"]]

# --- FUNÇÕES DE APOIO ---
def log(msg, console=True, arquivo=True):
    timestamp = datetime.now().strftime('%H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    if console:
        print(linha)
    if arquivo:
        with open(ARQ_LOG, 'a', encoding='utf-8') as f:
            f.write(linha + '\n')

def carregar_termos_portfolio():
    """Lê o Exportar Dados.csv e cria um motor de pesquisa na memória (Opção A)"""
    termos = set()
    if not os.path.exists(ARQ_CATALOGO):
        log(f"⚠️ AVISO: {ARQ_CATALOGO} não encontrado. Captura de editais vagos será limitada.")
        return termos

    encodings = ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']
    for enc in encodings:
        try:
            with open(ARQ_CATALOGO, mode='r', encoding=enc) as f:
                amostra = f.read(1024)
                f.seek(0)
                delimitador = ';' if ';' in amostra else ','
                reader = csv.DictReader(f, delimiter=delimitador)
                headers = {h.strip().upper(): h for h in reader.fieldnames if h}
                col_desc = next((h_real for h_upper, h_real in headers.items() if 'DESCRI' in h_upper), None)
                
                if not col_desc: continue
                
                for row in reader:
                    desc = str(row.get(col_desc, '')).strip().upper()
                    if desc:
                        palavra_chave = desc.split()[0].replace(',', '').replace('.', '')
                        if len(palavra_chave) > 2 and not palavra_chave.isdigit():
                            termos.add(palavra_chave)
            if termos: break
        except Exception:
            continue
            
    # Adiciona sinônimos básicos para garantir
    termos.update(["GLICOSE", "DEXTROSE", "AMINOACIDO", "HEMODIALISE", "DIALISE", "FOSFORO", "VITAMINA", "AAS"])
    return termos

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/23.0', 'Accept-Encoding': 'gzip, deflate, br'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    palavras_magicas = ["MEDICAMENTO", "MEDICAMENTOS", "AQUISICAO DE MEDICAMENTOS"]
    if any(p in obj for p in palavras_magicas): return False
    for v in TODOS_VETOS:
        if v in obj:
            if "NUTRICAO" in v or "ALIMENT" in v:
                if any(bom in obj for bom in WL_NUTRI_CLINICA) and "ESCOLAR" not in obj: return False
            return True
    return False

def safe_float(val):
    if val is None: return 0.0
    try:
        if isinstance(val, (int, float)):
            if isinstance(val, int) and val > 1000: return val / 100.0
            return float(val)
        val_str = str(val).strip().replace('R$', '').replace(' ', '')
        if ',' in val_str and '.' in val_str: val_str = val_str.replace('.', '').replace(',', '.')
        elif ',' in val_str: val_str = val_str.replace(',', '.')
        resultado = float(val_str)
        if resultado > 1000 and resultado == int(resultado):
            if resultado > 100000: resultado = resultado / 100.0
        return resultado
    except Exception:
        try:
            numeros = re.findall(r'[\d.,]+', str(val))
            if numeros:
                maior = max(numeros, key=lambda x: len(x))
                return float(maior.replace('.', '').replace(',', '.'))
        except: pass
        return 0.0

def salvar_checkpoint(dia, pagina):
    with open(ARQ_CHECKPOINT, 'w') as f: json.dump({'dia': dia, 'pagina': pagina}, f)

def carregar_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f: return json.load(f)
    return None

def buscar_itens_oficial(cnpj, ano, seq, session):
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens_totais = []
    pagina = 1
    
    while pagina <= 100:
        try:
            r = session.get(url, params={'pagina': pagina, 'tamanhoPagina': 100}, timeout=20)
            if r.status_code in [404, 301]: return None, str(r.status_code)
            elif r.status_code != 200:
                if pagina == 1: return None, f'http_{r.status_code}'
                break
            
            data = r.json()
            itens_pagina = data.get('data', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            if not itens_pagina: break
            
            itens_totais.extend(itens_pagina)
            if len(itens_pagina) < 100: break
            pagina += 1
        except Exception as e:
            if pagina == 1: return None, f'erro_{str(e)[:50]}'
            break
    return itens_totais, 'ok'

def processar_licitacao(lic, session, termos_portfolio, forcado=False):
    id_ref = "DESCONHECIDO"
    try:
        if not isinstance(lic, dict): return ('ERRO', None, 0, 'json_invalido')
        
        cnpj = lic.get('orgaoEntidade', {}).get('cnpj', '0000')
        ano = str(lic.get('anoCompra', '0000'))
        seq = str(lic.get('sequencialCompra', '0000'))
        id_ref = f"{cnpj}/{ano}/{seq}"
        
        sit_global_id = lic.get('situacaoCompraId') or 1
        sit_global_nome = MAPA_SITUACAO_GLOBAL.get(sit_global_id, "DIVULGADA")
        
        uo = lic.get('unidadeOrgao', {})
        
        # ✅ REGRA 3: O "Sem Estado" (Alarme BR)
        uf = str(uo.get('ufSigla') or '').upper().strip()
        if not uf:
            uf = 'BR'
            log(f"   ⚠️ AVISO: Licitação {id_ref} ({lic.get('orgaoEntidade', {}).get('razaoSocial', '---')}) sem UF na API. Assumindo 'BR'.")
            
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        obj_norm = normalize(obj_raw)
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()
        
        precisa_checar_portfolio = False
        
        if not forcado:
            try:
                dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
                if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO_DATA', None, 0, 'data_antiga')
            except: pass
                
            if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 'palavra_veto')
            
            tem_super_passe = any(p in obj_norm for p in ["MEDICAMENTO", "MEDICAMENTOS", "AQUISICAO DE MEDICAMENTOS"])
            tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
            tem_mmh_nutri = any(t in obj_norm for t in WL_MATERIAIS_NE + WL_NUTRI_CLINICA)
            tem_termo_amplo = any(x in obj_norm for x in ["SAUDE", "HOSPITAL", "MATERNIDADE", "CLINICA", "EBSERH", "FUNDO MUNICIPAL", "SECRETARIA DE"])
            
            # ✅ REGRAS 1, 2 e 4: Funil Geográfico e Temático
            if tem_super_passe or tem_med: 
                # Regra 1: Medicamentos Brasil todo, EXCETO Bloqueados
                if uf in ESTADOS_BLOQUEADOS: return ('IGNORADO_GEO', None, 0, 'estado_bloqueado_med')
                tem_interesse = True
            elif tem_mmh_nutri:
                # Regra 2: MMH e Dietas EXCLUSIVAMENTE Nordeste + DF + BR
                if uf not in UFS_PERMITIDAS_MMH: return ('IGNORADO_GEO', None, 0, 'fora_regiao_mmh')
                tem_interesse = True
            elif tem_termo_amplo:
                # Regra 4: Edital Vago. Bloqueia SUL/Norte e aciona a "Opção A" (Checar Portfólio)
                if uf in ESTADOS_BLOQUEADOS: return ('IGNORADO_GEO', None, 0, 'estado_bloqueado_vago')
                tem_interesse = True
                precisa_checar_portfolio = True
            else:
                return ('IGNORADO_TEMATICA', None, 0, 'sem_interesse')

        # Baixa os itens
        itens_brutos, fonte = buscar_itens_oficial(cnpj, ano, seq, session)
        if not itens_brutos: return ('ERRO_ITENS', None, 0, fonte)
        
        itens_mapeados = []
        for it in itens_brutos:
            if not isinstance(it, dict): continue
            benef_id = it.get('tipoBeneficioId')
            benef_nome_api = str(it.get('tipoBeneficioNome', '')).upper()
            benef_final = benef_id if benef_id in [1, 2, 3] else (1 if "EXCLUSIVA" in benef_nome_api else (3 if "COTA" in benef_nome_api else 4))
            
            itens_mapeados.append({
                'n': it.get('numeroItem'), 'd': it.get('descricao', ''),
                'q': safe_float(it.get('quantidade')), 'u': it.get('unidadeMedida', 'UN'),
                'v_est': safe_float(it.get('valorUnitarioEstimado')), 'benef': benef_final,
                'sit': MAPA_SITUACAO_ITEM.get(int(it.get('situacaoCompraItem') or 1), "EM ANDAMENTO"),
                'res_forn': None, 'res_val': 0.0
            })
            
        # ✅ REGRA 4 (Opção A): O Sniper cruza os itens vagos com a memória RAM
        if precisa_checar_portfolio and termos_portfolio and not forcado:
            teve_match = False
            for it in itens_mapeados:
                desc_item = normalize(it['d'])
                if any(termo in desc_item for termo in termos_portfolio):
                    teve_match = True
                    break
            
            if not teve_match:
                # Se baixou os itens e nenhum bateu com o Portfólio, descarta para poupar o BD!
                return ('IGNORADO_PORTFOLIO', None, 0, 'sem_match_itens')
        
        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf,
            'uasg': lic.get('unidadeOrgao', {}).get('codigoUnidade', '---'),
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'),
            'unid_nome': lic.get('unidadeOrgao', {}).get('nomeUnidade', '---'),
            'cid': lic.get('unidadeOrgao', {}).get('municipioNome', '---'),
            'obj': obj_raw, 'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'val_tot': safe_float(lic.get('valorTotalEstimado')),
            'itens': itens_mapeados, 'sit_global': sit_global_nome,
            'fonte': lic.get('nomeEntidadeIntegradora', 'PNCP Direto'), 'api_fonte': fonte
        }
        return ('CAPTURADO', dados_finais, len(itens_mapeados), 'ok')
        
    except Exception as e:
        return ('ERRO', None, 0, f'excecao_{str(e)[:50]}')

def buscar_periodo(session, banco, d_ini, d_fim):
    stats = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados_geo': 0, 'ignorados_data': 0, 'ignorados_tematica': 0, 'ignorados_portfolio': 0, 'erros_itens': 0, 'erros_outros': 0}
    checkpoint = carregar_checkpoint()
    
    if os.path.exists(ARQ_LOG): os.remove(ARQ_LOG)
    
    log("🚀 Iniciando captura Sniper Pharma (Opção A - Banco de Dados Blindado)")
    
    # CARREGA A INTELIGÊNCIA NA MEMÓRIA UMA ÚNICA VEZ
    termos_portfolio = carregar_termos_portfolio()
    log(f"🧠 Memória carregada com {len(termos_portfolio)} princípios ativos do Portfólio.")
    
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia_obj = d_ini + timedelta(days=i)
        dia = dia_obj.strftime('%Y%m%d')
        
        if checkpoint and dia < checkpoint['dia']: continue

        log(f"\n📅 DATA: {dia_obj.strftime('%d/%m/%Y')} ({dia})")
        pag = checkpoint['pagina'] if checkpoint and dia == checkpoint['dia'] else 1
        
        while True:
            inicio_pag = time.time()
            try:
                r = session.get(f"{API_CONSULTA}/contratacoes/publicacao", params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
                if r.status_code != 200: break
                
                dados = r.json()
                lics = dados.get('data', [])
                if not lics: break
                
                tot_pag = dados.get('totalPaginas', 1)
                s_pag = {k: 0 for k in stats}

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                    # Passa o portfólio para dentro do processador
                    futuros = [exe.submit(processar_licitacao, l, session, termos_portfolio) for l in lics]
                    for f in concurrent.futures.as_completed(futuros):
                        st, d, i_qtd, info = f.result()
                        if st == 'CAPTURADO' and d:
                            s_pag['capturados'] += 1; s_pag['itens'] += i_qtd
                            banco[f"{d['id'][:14]}_{d['edit']}"] = d
                        elif st == 'VETADO': s_pag['vetados'] += 1
                        elif st == 'IGNORADO_GEO': s_pag['ignorados_geo'] += 1
                        elif st == 'IGNORADO_DATA': s_pag['ignorados_data'] += 1
                        elif st == 'IGNORADO_TEMATICA': s_pag['ignorados_tematica'] += 1
                        elif st == 'IGNORADO_PORTFOLIO': s_pag['ignorados_portfolio'] += 1
                        elif st == 'ERRO_ITENS': s_pag['erros_itens'] += 1
                        else: s_pag['erros_outros'] += 1

                for k in stats: stats[k] += s_pag.get(k, 0)
                salvar_checkpoint(dia, pag + 1)
                
                log(f"   📄 Pág {pag}/{tot_pag} | ⏱️ {time.time() - inicio_pag:.1f}s | 🎯 {s_pag['capturados']} Capt | 🗑️ {s_pag['ignorados_portfolio']} Descartados S/ Match")
                
                if pag >= tot_pag:
                    salvar_checkpoint((dia_obj + timedelta(days=1)).strftime('%Y%m%d'), 1)
                    break
                pag += 1
            except Exception as e:
                log(f"   ❌ EXCEÇÃO na página {pag}: {str(e)[:100]}")
                break
    
    log(f"\n📊 RESUMO: {stats['capturados']} Capturados | {stats['ignorados_portfolio']} Filtrados pelo Portfólio (Limpeza Ativa) | Total no Banco: {len(banco)}")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=15)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        
        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    for x in json.load(f): banco[f"{x.get('id', '')[:14]}_{x.get('edit', '')}"] = x
            except Exception: pass

        buscar_periodo(session, banco, dt_start, dt_end)
        
        with gzip.open(ARQ_TEMP, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        if os.path.exists(ARQ_TEMP):
            os.replace(ARQ_TEMP, ARQDADOS)
            if os.path.exists(ARQ_CHECKPOINT): os.remove(ARQ_CHECKPOINT)
            print("✅ Concluído!")
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
