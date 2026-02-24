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
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
ARQ_CATALOGO = 'Exportar Dados.csv'
ARQ_REGRAS_CSV = 'regras_materiais.csv' # Seu novo arquivo de regras de contexto
ARQ_MANUAL = 'links_manuais.txt' 
MAXWORKERS = 15 
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- GEOGRAFIA E MAPAS ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR'] 

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

# Cache de normalização
CACHE_NORM = {}
def normalize(t): 
    if not t: return ""
    if t not in CACHE_NORM:
        CACHE_NORM[t] = ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')
    return CACHE_NORM[t]

# Função Motor de Busca Flexível com Escudo Global (\b)
def busca_flexivel(lista_regex, texto):
    for padrao in lista_regex:
        # O \b garante que a palavra seja exata. Ex: \bFERRO\b ignora FERROVIARIO
        if re.search(rf"\b{padrao}\b", texto):
            return True
    return False

# --- CARREGAMENTO DAS REGRAS DO CSV (O NOVO CÉREBRO) ---
REGRAS_CONTEXTUAIS = []
if os.path.exists(ARQ_REGRAS_CSV):
    try:
        with open(ARQ_REGRAS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                pc = normalize(row.get('palavra_chave', ''))
                if not pc: continue
                af = [normalize(x.strip()) for x in row.get('afirmacao', '').split(';') if x.strip()]
                neg = [normalize(x.strip()) for x in row.get('negacao', '').split(';') if x.strip()]
                REGRAS_CONTEXTUAIS.append({'pc': pc, 'af': af, 'neg': neg})
        print(f"🧠 Motor Semântico ativado: {len(REGRAS_CONTEXTUAIS)} regras carregadas.")
    except Exception as e: 
        print(f"⚠️ Erro ao ler {ARQ_REGRAS_CSV}: {e}")

def avalia_regras_contextuais(texto):
    if not REGRAS_CONTEXTUAIS: return False
    for regra in REGRAS_CONTEXTUAIS:
        if re.search(rf"\b{regra['pc']}\b", texto):
            passou_afirmacao = True
            if regra['af']: passou_afirmacao = any(re.search(rf"\b{a}\b", texto) for a in regra['af'])
            
            if passou_afirmacao:
                passou_negacao = True
                if regra['neg']: 
                    if any(re.search(rf"\b{n}\b", texto) for n in regra['neg']): passou_negacao = False
                if passou_negacao: return True
    return False

# --- CARREGAMENTO DO CATÁLOGO ---
CATALOGO_TERMOS = set()
if os.path.exists(ARQ_CATALOGO):
    try:
        for enc in ['utf-8', 'latin-1', 'cp1252']:
            try:
                with open(ARQ_CATALOGO, 'r', encoding=enc) as f:
                    leitor = csv.reader(f, delimiter=';') 
                    next(leitor, None)
                    for row in leitor:
                        if len(row) > 1:
                            termos = [row[0], row[1]] if len(row) > 1 else [row[0]]
                            for t in termos:
                                norm = normalize(t)
                                if len(norm) > 4: CATALOGO_TERMOS.add(norm)
                print(f"📚 Catálogo carregado: {len(CATALOGO_TERMOS)} termos.")
                break
            except: continue
    except: pass

# --- LISTAS FLEXÍVEIS COM REGEX ---
VETOS_ALIMENTACAO = [
    r"ALIMENTACAO ESCOLAR", r"GENEROS ALIMENTICIOS", r"MERENDA", r"PNAE", r"PERECIVEIS", 
    r"HORTIFRUTI", r"CARNES", r"PANIFICACAO", r"CESTAS BASICAS", r"LANCHE", r"REFEICOES", 
    r"COFFEE BREAK", r"BUFFET", r"COZINHA", r"ACOUGUE", r"POLPA DE FRUTA", r"ESTIAGEM"
]
VETOS_EDUCACAO = [
    r"MATERIAL ESCOLAR", r"PEDAGOGICO", r"DIDATICO", r"BRINQUEDOS", r"LIVROS", 
    r"TRANSPORTE ESCOLAR", r"KIT ALUNO", r"REDE MUNICIPAL DE ENSINO", r"SECRETARIA DE EDUCACAO"
]
VETOS_OPERACIONAL = [
    r"OBRAS", r"CONSTRUCAO", r"PAVIMENTACAO", r"REFORMA", r"MANUTENCAO PREDIAL", 
    r"LIMPEZA URBANA", r"RESIDUOS SOLIDOS", r"LOCACAO DE VEICULOS", r"TRANSPORTE", 
    r"COMBUSTIVEL", r"DIESEL", r"GASOLINA", r"PNEUS", r"PECAS AUTOMOTIVAS", 
    r"OFICINA", r"VIGILANCIA", r"SEGURANCA", r"BOMBEIRO", r"SALVAMENTO", r"RESGATE", 
    r"VIATURA", r"FARDAMENTO", r"VESTUARIO", r"INFORMATICA", r"COMPUTADORES", r"IMPRESSAO", r"EVENTOS",
    r"VEICULO(S)?", r"ASFALTO", r"TAPA[\s\-]*BURACO", r"FERROVIA(RIO)?", r"AUTOMOTIVO" # Reforços Adicionados
]
VETOS_ADM = [r"ADESAO", r"INTENCAO", r"IRP", r"CREDENCIAMENTO", r"LEILAO", r"ALIENACAO"]
TODOS_VETOS = VETOS_ALIMENTACAO + VETOS_EDUCACAO + VETOS_OPERACIONAL + VETOS_ADM

WL_MEDICAMENTOS = [
    r"MEDICAMENT", r"FARMAC", r"REMEDIO", r"SORO", r"FARMACO", r"AMPOAL", r"COMPRIMIDO", r"INJETAVEL", r"VACINA", 
    r"INSULINA", r"ANTIBIOTICO", r"ACETILCISTEINA", r"ACETILSALICILICO", r"ACICLOVIR", r"ADENOSINA", r"ADRENALINA", 
    r"ALBENDAZOL", r"ALENDRONATO", r"ALFAEPOETINA", r"ALFAINTERFERONA", r"ALFAST", r"ALOPURINOL", r"ALPRAZOLAM", 
    r"AMBROXOL", r"AMBROXOL XPE", r"AMINOFILINA", r"AMIODARONA", r"AMITRIPTILINA", r"AMOXICILINA", r"AMPICILINA", 
    r"ANASTROZOL", r"ANFOTERICINA", r"ANLODIPINO", r"ARIPIPRAZOL", r"ARIPIPRAZOL\.", r"ATENOLOL", r"ATORVASTANTINA", 
    r"ATORVASTATINA", r"ATORVASTATINA CALCICA", r"ATRACURIO", r"ATROPINA", r"AZITROMICINA", r"AZTREONAM", r"BACLOFENO", 
    r"BAMIFILINA", r"BENZILPENICILINA", r"BENZOATO", r"BETAMETASONA", r"BEZAFIBRATO", r"BIMATOPROSTA", r"BISACODIL", 
    r"BISSULFATO", r"BOPRIV", r"BROMOPRIDA", r"BUDESONIDA", r"BUPROPIONA", r"BUTILBROMETO", r"CABERGOLINA", r"CALCITRIOL", 
    r"CANDESARTANA", r"CAPTOPRIL", r"CARBAMAZEPINA", r"CARBONATO", r"CARVEDILOL", r"CAVERDILOL", r"CEFALEXINA", 
    r"CEFALOTINA", r"CEFAZOLINA", r"CEFEPIMA", r"CEFOTAXIMA", r"CEFOXITINA", r"CEFTAZIDIMA", r"CEFTRIAXONA", r"CEFUROXIMA", 
    r"CETOCONAZOL", r"CETOPROFENO", r"CETOROLACO", r"CICLOBENZAPRINA", r"CICLOSPORINA", r"CILOSTAZOL", r"CIMETIDINA", 
    r"CIPROFLOXACINO", r"CIPROFLOXACINA", r"CITALOPRAM", r"CLARITROMICINA", r"CLINDAMICINA", r"CLOBETASOL", r"CLOMIPRAMINA", 
    r"CLONAZEPAM", r"CLONIDINA", r"CLOPIDOGREL", r"CLORETO", r"CLORIDRATO", r"CLORIDRATO DE CIPROFLOXACINO", r"CLORPROMAZINA", 
    r"CLORTALIDONA", r"CLOTRIMAZOL", r"CLOZAPINA", r"CODEINA", r"COLCHICINA", r"COLECALCIFEROL", r"COLISTIMETATO", 
    r"COMPLEXO B", r"DACARBZINA", r"DAPAGLIFLOZINA", r"DAPAGLIFLOZINA\.", r"DAPSONA", r"DAPTOMICINA", r"DARBEPOETINA", 
    r"DESLANOSIDEO", r"DESLORATADINA", r"DEXAMETASONA", r"DEXCLORFENIRAMINA", r"DEXPANTENOL", r"DIAZEPAM", r"DIETILAMONIO", 
    r"DICLOFENACO", r"DIGOXINA", r"DILTIAZEM", r"DIMETICONA", r"DIOSMINA", r"DIPIRONA", r"DOBUTAMINA", r"DOMPERIDONA", 
    r"DONEPEZILA", r"DOPAMINA", r"DOXAZOSINA", r"DOXICICLINA", r"DROPERIDOL", r"DULAGLUTIDA", r"DULOXETINA", r"DUTASTERIDA", 
    r"ECONAZOL", r"EMULSAO", r"ENALAPRIL", r"ENOXAPARINA", r"ENTACAPONA", r"EPINEFRINA", r"ERITROMICINA", r"ESCITALOPRAM", 
    r"ESOMEPRAZOL", r"ESPIRONOLACTONA", r"ESTRADIOL", r"ESTRIOL", r"ESTROGENIOS", r"ETANERCEPTE", r"ETILEFRINA", r"ETOMIDATO", 
    r"ETOPOSIDEO", r"EZETIMIBA", r"FAMOTIDINA", r"FENITOINA", r"FENOBARBITAL", r"FENOTEROL", r"FENTANILA", r"FERRO(SO|SA)?(S)?", 
    r"FIBRINOGENIO", r"FILGRASTIM", r"FINASTERIDA", r"FITOMENADIONA", r"FLUCONAZOL", r"FLUDROCORTISONA", r"FLUMAZENIL", 
    r"FLUNARIZINA", r"FLUOXETINA", r"FLUTICASONA", r"FOLATO", r"FONDAPARINUX", r"FORMOTEROL", r"FOSFATO", r"FUROSEMIDA", 
    r"GABAPENTINA", r"GANCICLOVIR", r"GELADEIRA", r"GENCITABINA", r"GENTAMICINA", r"GLIBENCLAMIDA", r"GLICEROL", r"GLICLAZIDA", 
    r"GLICOSE", r"GLIMEPIRIDA", r"GLUCAGON", r"HALOPERIDOL", r"HEPARINA", r"HIDRALAZINA", r"HIDROCLOROTIAZIDA", 
    r"HIDROCORTISONA", r"HIDROTALCITA", r"HIDROXIDOPROGESTERONA", r"HIDROXIDO", r"HIDROXIPROGESTERONA", r"HIDROXIUREIA", 
    r"HIOSCINA", r"HIPROMELOSE", r"IBUPROFENO", r"IMIPENEM", r"IMIPRAMINA", r"INDAPAMIDA", r"INSULINA", r"IOIMBINA", 
    r"IPRATROPIO", r"IRBESARTANA", r"IRINOTECANO", r"ISOSSORBIDA", r"ISOTRETINOINA", r"ITRACONAZOL", r"IVERMECTINA", 
    r"LACTULOSE", r"LAMOTRIGINA", r"LANSOPRAZOL", r"LATANOPROSTA", r"LEFLUNOMIDA", r"LERCANIDIPINO", r"LETROZOL", r"LEVODOPA", 
    r"LEVOFLOXACINO", r"LEVOMEPROMAZINA", r"LEVONORGESTREL", r"LEVOTIROXINA", r"LIDOCAINA", r"LINEZOLIDA", r"LINOGLIPTINA", 
    r"LIPIDICA", r"LISINOPRIL", r"LITIO", r"LOPERAMIDA", r"LORATADINA", r"LORAZEPAM", r"LOSARTANA", r"LOVASTATINA", r"MAGNESIO", 
    r"MANITOL", r"MEBENDAZOL", r"MEDROXIPROGESTERONA", r"MEMANTINA", r"MEROPENEM", r"MESALAZINA", r"METILDOPA", 
    r"METILPREDNISOLONA", r"METOCLOPRAMIDA", r"METOPROLOL", r"METOTREXATO", r"METRONIDAZOL", r"MICOFENOLATO", r"MIDAZOLAM", 
    r"MIRTAZAPINA", r"MISOPROSTOL", r"MORFINA", r"MUPIROCINA", r"NARATRIPTANA", r"NEOMICINA", r"NEOSTIGMINA", r"NIFEDIPINO", 
    r"NIMESULIDA", r"NIMODIPINO", r"NISTATINA", r"NITROFURANTOINA", r"NITROGLICERINA", r"NITROPRUSSIATO", r"NORETISTERONA", 
    r"NORFLOXACINO", r"NORTRIPTILINA", r"OCTREOTIDA", r"OLANZAPINA", r"OLMESARTANA", r"OMEPRAZOL", r"ONDANSETRONA", 
    r"OXALIPLATINA", r"OXCARBAZEPINA", r"OXIBUTININA", r"PACLITAXEL", r"PALONOSETRONA", r"PANTOPRAZOL", 
    r"PARACETAMOL", r"PAROXETINA", r"PENICILINA", r"PERICIAZINA", r"PERMETRINA", r"PETIDINA", r"PIRAZINAMIDA", r"PIRIDOSTIGMINA", 
    r"PIRIDOXINA", r"POLIMIXINA", r"POLIVITAMINICO", r"POTASSIO", r"PRAMIPEXOL", r"PRAVASTATINA", r"PREDNISOLONA", r"PREDNISONA", 
    r"PREGABALINA", r"PROMETAZINA", r"PROPATILNITRATO", r"PROPOFOL", r"PROPRANOLOL", r"PROSTIGMINA", r"QUETIAPINA", r"RAMIPRIL", 
    r"RANITIDINA", r"RESERPINA", r"RIFAMPICINA", r"RISPERIDONA", r"RITONAVIR", r"RIVAROXABANA", r"ROCURONIO", r"ROSUVASTATINA", 
    r"SACARATO", r"SALBUTAMOL", r"SECAM", r"SERTRALINA", r"SEVELAMER", r"SINVASTATINA", r"SODIO", r"SUCCINILCOLINA", 
    r"SUCRALFATO", r"SULFADIAZINA", r"SULFAMETOXAZOL", r"SULFATO", r"SULPIRIDA", r"SUXAMETONIO", r"TAMOXIFENO", r"TANSULOSINA", 
    r"TEMOZOLAMIDA", r"TEMOZOLOMIDA", r"TENOXICAN", r"TERBUTALINA", r"TIAMINA", r"TIGECICLINA", r"TIOPENTAL", r"TIORIDAZINA", 
    r"TOBRAMICINA", r"TOPIRAMATO", r"TRAMADOL", r"TRAVOPROSTA", r"TRIMETOPRIMA", r"TROMETAMOL", r"TROPICAMIDA", r"VALSARTANA", 
    r"VANCOMICINA", r"VARFARINA", r"VASELINA"
]

WL_NUTRI_CLINICA = [
    r"NUTRICAO ENTERAL", r"FORMULA INFANTIL", r"SUPLEMENTO ALIMENTAR", r"DIETA ENTERAL", r"DIETA PARENTERAL", r"NUTRICAO CLINICA"
]

# As palavras simples (como Material, Luva, Mascara) saíram daqui para serem tratadas pelo CSV!
WL_MATERIAIS_NE = [
    r"INSUMO(S)? HOSPITALAR(ES)?", r"MMH", r"SERINGA(S)?", r"SONDA(S)?", r"CATETER(ES)?", 
    r"MEDIC(O|A)?(S)?[\s\-]*HOSPITALAR(ES)?", r"LABORATORI(O|AL|AIS)", r"PRODUTO(S)? PARA SAUDE", 
    r"ANTISSEPTIC(O|A)?(S)?", r"CLOREXIDINA", r"PVPI",
    r"CURATIVO(S)?", r"COBERTURA(S)? (ESPECIAL|ESPECIAIS|PARA LESO(AO|ES)|ESTERIL)"
]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/24.0 AI Edition'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    for v in TODOS_VETOS:
        if re.search(rf"\b{v}\b", obj):
            if re.search(r"\b(NUTRICAO|ALIMENT)\b", v):
                if busca_flexivel(WL_NUTRI_CLINICA, obj) and not re.search(r"\bESCOLAR\b", obj): 
                    continue
            return True
    return False

def safe_float(val):
    try: return float(val) if val is not None else 0.0
    except: return 0.0

def processar_licitacao(lic, session, forcado=False):
    id_ref = "DESCONHECIDO"
    try:
        if not isinstance(lic, dict): return ('ERRO', {'msg': 'Formato JSON inválido'}, 0, 0)
        
        cnpj = lic.get('orgaoEntidade', {}).get('cnpj', '0000')
        ano = lic.get('anoCompra', '0000')
        seq = lic.get('sequencialCompra', '0000')
        id_ref = f"{cnpj}/{ano}/{seq}"
        
        sit_global_id = lic.get('situacaoCompraId') or 1
        sit_global_nome = MAPA_SITUACAO_GLOBAL.get(sit_global_id, "DIVULGADA")
        
        uo = lic.get('unidadeOrgao', {})
        uf = uo.get('ufSigla', '').upper()
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        obj_norm = normalize(obj_raw)
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()
        
        if not forcado:
            if uf in ESTADOS_BLOQUEADOS: return ('VETADO', None, 0, 0)
            dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)
            if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 0)

            # Nova Inteligência: Verifica as Listas Blindadas OU o Motor de Regras do CSV
            tem_interesse = busca_flexivel(WL_MEDICAMENTOS, obj_norm) or \
                            (uf in NE_ESTADOS and busca_flexivel(WL_MATERIAIS_NE + WL_NUTRI_CLINICA, obj_norm)) or \
                            busca_flexivel([r"SAUDE", r"HOSPITAL"], obj_norm) or \
                            avalia_regras_contextuais(obj_norm)

            if not tem_interesse: return ('IGNORADO', None, 0, 0)

        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        itens_brutos = []
        tem_item_catalogo = forcado 
        pagina_atual = 1
        
        while True:
            r_itens = session.get(url_itens, params={'pagina': pagina_atual, 'tamanhoPagina': 100}, timeout=20)
            if r_itens.status_code != 200: 
                return ('ERRO', {'msg': f"HTTP {r_itens.status_code} na pág {pagina_atual}. Cancelado para não truncar."}, 0, 0)
            
            resp_json = r_itens.json()
            if isinstance(resp_json, dict): itens_raw = resp_json.get('data', [])
            elif isinstance(resp_json, list): itens_raw = resp_json
            else: break

            if not itens_raw: break

            for it in itens_raw:
                if not isinstance(it, dict): continue
                desc = it.get('descricao', '')
                desc_norm = normalize(desc)
                
                if str(it.get('ncmNbsCodigo', '')).startswith('30') or any(term in desc_norm for term in CATALOGO_TERMOS):
                    tem_item_catalogo = True
                
                sit_id = int(it.get('situacaoCompraItem') or 1)
                sit_nome = MAPA_SITUACAO_ITEM.get(sit_id, "EM ANDAMENTO")
                
                benef_id = it.get('tipoBeneficioId')
                benef_nome_api = str(it.get('tipoBeneficioNome', '')).upper()
                benef_final = benef_id if benef_id in [1, 2, 3] else (1 if "EXCLUSIVA" in benef_nome_api else (3 if "COTA" in benef_nome_api else 4))

                itens_brutos.append({
                    'n': it.get('numeroItem'), 'd': desc, 'q': safe_float(it.get('quantidade')),
                    'u': it.get('unidadeMedida', 'UN'), 'v_est': safe_float(it.get('valorUnitarioEstimado')),
                    'benef': benef_final, 'sit': sit_nome, 'res_forn': None, 'res_val': 0.0
                })
            
            if len(itens_raw) < 100: break
            pagina_atual += 1

        if not itens_brutos: return ('IGNORADO', None, 0, 0)
        
        # Filtro final rigoroso unificando o CSV
        if not forcado and uf not in NE_ESTADOS and not tem_item_catalogo and not (busca_flexivel(WL_MEDICAMENTOS, obj_norm) or avalia_regras_contextuais(obj_norm)):
            return ('IGNORADO', None, 0, 0)

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': lic.get('unidadeOrgao', {}).get('codigoUnidade', '---'),
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'), 
            'unid_nome': lic.get('unidadeOrgao', {}).get('nomeUnidade', '---'),
            'cid': lic.get('unidadeOrgao', {}).get('municipioNome', '---'), 
            'obj': obj_raw, 'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': safe_float(lic.get('valorTotalEstimado')), 
            'itens': itens_brutos, 'sit_global': sit_global_nome
        }
        return ('CAPTURADO', dados_finais, len(itens_brutos), 0)
    except Exception as e: 
        return ('ERRO', {'msg': f"Erro interno em {id_ref}: {str(e)}"}, 0, 0)

def processar_inclusoes_manuais(session, banco):
    if not os.path.exists(ARQ_MANUAL): return
    print("\n⚙️ Processando Inclusões Manuais...")
    try:
        with open(ARQ_MANUAL, 'r', encoding='utf-8') as f: links = f.read().splitlines()
        padrao = re.compile(r'/editais/(\d+)/(\d+)/(\d+)')
        for link in links:
            match = padrao.search(link)
            if match:
                cnpj, ano, seq = match.groups()
                r = session.get(f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}')
                if r.status_code == 200:
                    st, d, i, h = processar_licitacao(r.json(), session, forcado=True)
                    if st == 'CAPTURADO' and d:
                        banco[f"{d['id'][:14]}_{d['edit']}"] = d
                        print(f"   ✅ Captura Manual Sucesso: {cnpj}/{ano}/{seq}")
                    elif st == 'ERRO':
                        print(f"   ❌ Falha Manual: {d['msg']}")
        open(ARQ_MANUAL, 'w').close() 
    except Exception as e: print(f"Erro Inclusão Manual: {e}")

def buscar_periodo(session, banco, d_ini, d_fim):
    stats = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0}
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        print(f"\n📅 DATA: {dia}")
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
        pag = 1
        while True:
            try:
                r = session.get(url, params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
                if r.status_code != 200: break
                dados = r.json(); lics = dados.get('data', [])
                if not lics: break
            except: break
            
            tot_pag = dados.get('totalPaginas', 1)
            s_pag = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0}

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    st, d, itn, _ = f.result()
                    if st == 'CAPTURADO' and d:
                        s_pag['capturados'] += 1; s_pag['itens'] += itn
                        banco[f"{d['id'][:14]}_{d['edit']}"] = d
                    elif st == 'VETADO': s_pag['vetados'] += 1
                    elif st == 'IGNORADO': s_pag['ignorados'] += 1
                    elif st == 'ERRO': s_pag['erros'] += 1
            
            for k in stats: stats[k] += s_pag[k]
            print(f"   📄 Pág {pag}/{tot_pag}: 🎯 {s_pag['capturados']} Caps | 📦 {s_pag['itens']} Itens | 🔥 {s_pag['erros']} Erros")
            if pag >= tot_pag: break
            pag += 1

    print("\n" + "="*50)
    print("📊 RESUMO GERAL DA OPERAÇÃO DE CAPTURA")
    print("="*50)
    print(f"✅ EDITAIS CAPTURADOS: {stats['capturados']}")
    print(f"📦 ITENS TOTALIZADOS:  {stats['itens']}")
    print(f"🚫 EDITAIS VETADOS:    {stats['vetados']}")
    print(f"👁️ EDITAIS IGNORADOS:  {stats['ignorados']}")
    print("="*50)

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=2)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        
        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[f"{x.get('id', '')[:14]}_{x.get('edit', '')}"] = x
            
        processar_inclusoes_manuais(session, banco)    
        buscar_periodo(session, banco, dt_start, dt_end)
        
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
