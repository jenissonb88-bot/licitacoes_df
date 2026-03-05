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
ARQ_TEMP = ARQDADOS + '.tmp'
ARQ_CHECKPOINT = 'checkpoint.json'
ARQ_LOCK = 'execucao.lock'
ARQ_CATALOGO = 'Exportar Dados.csv'
ARQ_MANUAL = 'links_manuais.txt'
MAXWORKERS = 15
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- GEOGRAFIA E MAPAS ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
UFS_PERMITIDAS_MMH = NE_ESTADOS  # Apenas Nordeste, sem DF

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def normalize(t):
    if not t:
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- VETOS ABSOLUTOS (Sempre vetam, mesmo com medicamentos) ---
VETOS_ABSOLUTOS = [normalize(x) for x in [
    "INTENCAO DE REGISTRO DE PRECO",
    "INTENCAO REGISTRO DE PRECO",
    "CREDENCIAMENTO",
    "ADESAO",
    "IRP",
    "LEILAO",
    "ALIENACAO"
]]

# --- SUPER PASSE (Libera para qualquer UF, exceto ESTADOS_BLOQUEADOS) ---
PALAVRAS_MEDICAMENTOS = [normalize(x) for x in [
    "MEDICAMENTO", "MEDICAMENTOS",
    "AQUISICAO DE MEDICAMENTO", "AQUISICAO DE MEDICAMENTOS",
    "AQUISICAO MEDICAMENTO", "AQUISICAO MEDICAMENTOS",
    "COMPRA DE MEDICAMENTO", "COMPRA DE MEDICAMENTOS",
    "FORNECIMENTO DE MEDICAMENTO", "FORNECIMENTO DE MEDICAMENTOS"
]]

# --- VETOS OPERACIONAIS (Com variações SING/PLURAL) ---
VETOS_OPERACIONAIS_BASE = [
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO",
    "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO", "TRANSPORTE",
    "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS",
    "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO",
    "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", "UNIFORME", "TEXTIL",
    "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO",
    "CORRETIVA", "PREVENTIVA", "VEICULO", "AMBULANCIA", "MOTOCICLETA",
    "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO",
    "EQUIPAMENTO E MATERIA PERMANENTE", "MATERIAIS PERMANENTES",
    "EQUIPAMENTOS PERMANENTES", "INSTALACAO", "ASFALTICO", "ASFALTO",
    "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS",
    "MANUTENCAO PREVENTIVA", "MANUTENCAO CORRETIVA",
    "GASES MEDICINAIS", "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA",
    "GERADOR", "RECARGA", "CONFECCAO", "PRESTACAO DE SERVICO",
    "SERVICO ESPECIALIZADO"
]

VETOS_ALIMENTACAO = [normalize(x) for x in [
    "ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE",
    "PERECIVEIS", "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS",
    "LANCHE", "REFEICOES", "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE",
    "POLPA DE FRUTA", "ESTIAGEM"
]]

VETOS_EDUCACAO = [normalize(x) for x in [
    "MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS",
    "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO",
    "SECRETARIA DE EDUCACAO"
]]

# Gerar variações sing/plural para vetos operacionais
VETOS_OPERACIONAL = []
for termo in VETOS_OPERACIONAIS_BASE:
    n = normalize(termo)
    VETOS_OPERACIONAL.append(n)
    if not n.endswith('S') and not n.endswith('ES'):
        VETOS_OPERACIONAL.append(n + 'S')
    elif n.endswith('L'):
        VETOS_OPERACIONAL.append(n + 'ES')

VETOS_OPERACIONAL = list(set(VETOS_OPERACIONAL))
TODOS_VETOS = VETOS_OPERACIONAL + VETOS_ALIMENTACAO + VETOS_EDUCACAO

# --- WHITELISTS ---
WL_MEDICAMENTOS = [normalize(x) for x in [
    "MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL",
    "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO",
    "ACETILCISTEINA", "ACETILSALICILICO", "ACICLOVIR", "ADENOSINA",
    "ADRENALINA", "ALBENDAZOL", "ALENDRONATO", "ALFAEPOETINA",
    "ALFAINTERFERONA", "ALFAST", "ALOPURINOL", "ALPRAZOLAM", "AMBROXOL",
    "AMINOFILINA", "AMIODARONA", "AMITRIPTILINA", "AMOXICILINA",
    "AMPICILINA", "ANASTROZOL", "ANFOTERICINA", "ANLODIPINO",
    "ARIPIPRAZOL", "ATENOLOL", "ATORVASTANTINA", "ATORVASTATINA",
    "ATRACURIO", "ATROPINA", "AZITROMICINA", "AZTREONAM", "BACLOFENO",
    "BAMIFILINA", "BENZILPENICILINA", "BENZOATO", "BETAMETASONA",
    "BEZAFIBRATO", "BIMATOPROSTA", "BISACODIL", "BISSULFATO", "BOPRIV",
    "BROMOPRIDA", "BUDESONIDA", "BUPROPIONA", "BUTILBROMETO",
    "CABERGOLINA", "CALCITRIOL", "CANDESARTANA", "CAPTOPRIL",
    "CARBAMAZEPINA", "CARBONATO", "CARVEDILOL", "CAVERDILOL",
    "CEFALEXINA", "CEFALOTINA", "CEFAZOLINA", "CEFEPIMA", "CEFOTAXIMA",
    "CEFOXITINA", "CEFTAZIDIMA", "CEFTRIAXONA", "CEFUROXIMA",
    "CETOCONAZOL", "CETOPROFENO", "CETOROLACO", "CICLOBENZAPRINA",
    "CICLOSPORINA", "CILOSTAZOL", "CIMETIDINA", "CIPROFLOXACINO",
    "CIPROFLOXACINA", "CITALOPRAM", "CLARITROMICINA", "CLINDAMICINA",
    "CLOBETASOL", "CLOMIPRAMINA", "CLONAZEPAM", "CLONIDINA",
    "CLOPIDOGREL", "CLORETO", "CLORIDRATO", "CLORPROMAZINA",
    "CLORTALIDONA", "CLOTRIMAZOL", "CLOZAPINA", "CODEINA", "COLCHICINA",
    "COLECALCIFEROL", "COLISTIMETATO", "COMPLEXO B", "DACARBZINA",
    "DAPAGLIFLOZINA", "DAPSONA", "DAPTOMICINA", "DARBEPOETINA",
    "DESLANOSIDEO", "DESLORATADINA", "DEXAMETASONA", "DEXCLORFENIRAMINA",
    "DEXPANTENOL", "DIAZEPAM", "DIETILAMONIO", "DICLOFENACO", "DIGOXINA",
    "DILTIAZEM", "DIMETICONA", "DIOSMINA", "DIPIRONA", "DOBUTAMINA",
    "DOMPERIDONA", "DONEPEZILA", "DOPAMINA", "DOXAZOSINA", "DOXICICLINA",
    "DROPERIDOL", "DULAGLUTIDA", "DULOXETINA", "DUTASTERIDA", "ECONAZOL",
    "EMULSAO", "ENALAPRIL", "ENOXAPARINA", "ENTACAPONA", "EPINEFRINA",
    "ERITROMICINA", "ESCITALOPRAM", "ESOMEPRAZOL", "ESPIRONOLACTONA",
    "ESTRADIOL", "ESTRIOL", "ESTROGENIOS", "ETANERCEPTE", "ETILEFRINA",
    "ETOMIDATO", "ETOPOSIDEO", "EZETIMIBA", "FAMOTIDINA", "FENITOINA",
    "FENOBARBITAL", "FENOTEROL", "FENTANILA", "FERRO", "FIBRINOGENIO",
    "FILGRASTIM", "FINASTERIDA", "FITOMENADIONA", "FLUCONAZOL",
    "FLUDROCORTISONA", "FLUMAZENIL", "FLUNARIZINA", "FLUOXETINA",
    "FLUTICASONA", "FOLATO", "FONDAPARINUX", "FORMOTEROL", "FOSFATO",
    "FUROSEMIDA", "GABAPENTINA", "GANCICLOVIR", "GELADEIRA",
    "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICEROL",
    "GLICLAZIDA", "GLICOSE", "GLIMEPIRIDA", "GLUCAGON", "HALOPERIDOL",
    "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA",
    "HIDROTALCITA", "HIDROXIDOPROGESTERONA", "HIDROXIDO",
    "HIDROXIUREIA", "HIOSCINA", "HIPROMELOSE", "IBUPROFENO", "IMIPENEM",
    "IMIPRAMINA", "INDAPAMIDA", "IOIMBINA", "IPRATROPIO", "IRBESARTANA",
    "IRINOTECANO", "ISOSSORBIDA", "ISOTRETINOINA", "ITRACONAZOL",
    "IVERMECTINA", "LACTULOSE", "LAMOTRIGINA", "LANSOPRAZOL",
    "LATANOPROSTA", "LEFLUNOMIDA", "LERCANIDIPINO", "LETROZOL",
    "LEVODOPA", "LEVOFLOXACINO", "LEVOMEPROMAZINA", "LEVONORGESTREL",
    "LEVOTIROXINA", "LIDOCAINA", "LINEZOLIDA", "LINOGLIPTINA",
    "LIPIDICA", "LISINOPRIL", "LITIO", "LOPERAMIDA", "LORATADINA",
    "LORAZEPAM", "LOSARTANA", "LOVASTATINA", "MAGNESIO", "MANITOL",
    "MEBENDAZOL", "MEDROXIPROGESTERONA", "MEMANTINA", "MEROPENEM",
    "MESALAZINA", "METILDOPA", "METILPREDNISOLONA", "METOCLOPRAMIDA",
    "METOPROLOL", "METOTREXATO", "METRONIDAZOL", "MICOFENOLATO",
    "MIDAZOLAM", "MIRTAZAPINA", "MISOPROSTOL", "MORFINA", "MUPIROCINA",
    "NARATRIPTANA", "NEOMICINA", "NEOSTIGMINA", "NIFEDIPINO",
    "NIMESULIDA", "NIMODIPINO", "NISTATINA", "NITROFURANTOINA",
    "NITROGLICERINA", "NITROPRUSSIATO", "NORETISTERONA",
    "NORFLOXACINO", "NORTRIPTILINA", "OCTREOTIDA", "OLANZAPINA",
    "OLMESARTANA", "OMEPRAZOL", "ONDANSETRONA", "OXALIPLATINA",
    "OXCARBAZEPINA", "OXIBUTININA", "PACLITAXEL", "PALONOSETRONA",
    "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", "PERICIAZINA",
    "PERMETRINA", "PETIDINA", "PIRAZINAMIDA", "PIRIDOSTIGMINA",
    "PIRIDOXINA", "POLIMIXINA", "POLIVITAMINICO", "POTASSIO",
    "PRAMIPEXOL", "PRAVASTATINA", "PREDNISOLONA", "PREDNISONA",
    "PREGABALINA", "PROMETAZINA", "PROPATILNITRATO", "PROPOFOL",
    "PROPRANOLOL", "PROSTIGMINA", "QUETIAPINA", "RAMIPRIL", "RANITIDINA",
    "RESERPINA", "RIFAMPICINA", "RISPERIDONA", "RITONAVIR",
    "RIVAROXABANA", "ROCURONIO", "ROSUVASTATINA", "SACARATO",
    "SALBUTAMOL", "SECAM", "SERTRALINA", "SEVELAMER", "SINVASTATINA",
    "SODIO", "SUCCINILCOLINA", "SUCRALFATO", "SULFADIAZINA",
    "SULFAMETOXAZOL", "SULFATO", "SULPIRIDA", "SUXAMETONIO",
    "TAMOXIFENO", "TANSULOSINA", "TEMOZOLAMIDA", "TEMOZOLOMIDA",
    "TENOXICAN", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIOPENTAL",
    "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL",
    "TRAVOPROSTA", "TRIMETOPRIMA", "TROMETAMOL", "TROPICAMIDA",
    "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASELINA"
]]

WL_NUTRI_CLINICA = [normalize(x) for x in [
    "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR",
    "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA",
    "ENTERAL", "PARENTERA", "ENTERA"
]]

WL_MATERIAIS_NE = [normalize(x) for x in [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA",
    "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO",
    "LUVAS DE PROCEDIMENTO", "MASCARA", "MASCARA CIRURGICA", "PENSO",
    "MATERIAL PENSO", "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA",
    "ABSORVENTE", "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO",
    "CURATIVOS", "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE",
    "AVENTAL DESCARTAVEL", "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA",
    "COLETOR PERFURO", "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO",
    "SONDA NASO", "TOUCA DESCARTAVEL", "TUBO ASPIRACAO", "CORRELATO",
    "AGULHAS", "SERINGAS"
]]

WL_TERMOS_AMPLos = [normalize(x) for x in ["SAUDE", "HOSPITAL", "HOSPITALAR"]]

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
                                if len(norm) > 4:
                                    CATALOGO_TERMOS.add(norm)
                print(f"📚 Catálogo carregado: {len(CATALOGO_TERMOS)} termos.")
                break
            except:
                continue
    except:
        pass

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/22.1'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def tem_medicamento_no_texto(texto):
    """Verifica se há termos de medicamentos no texto"""
    if not texto:
        return False
    texto_norm = normalize(texto)
    return any(p in texto_norm for p in PALAVRAS_MEDICAMENTOS)

def tem_medicamento_nos_itens(itens):
    """Fallback: verifica se algum item contém medicamentos"""
    if not itens:
        return False
    for item in itens:
        desc = item.get('descricao', '')
        if tem_medicamento_no_texto(desc):
            return True
        desc_norm = normalize(desc)
        if any(med in desc_norm for med in WL_MEDICAMENTOS):
            return True
    return False

def veta_edital(obj_raw, uf, itens=None):
    """
    Retorna: (status, motivo)
    status: 'CAPTURAR', 'VETAR', 'IGNORAR'
    """
    obj_norm = normalize(obj_raw)

    # 1. VETOS ABSOLUTOS (sempo vetam, independente de medicamentos)
    for v in VETOS_ABSOLUTOS:
        if v in obj_norm:
            return ('VETAR', f'Veto absoluto: {v}')

    # 2. SUPER PASSE NACIONAL (medicamentos)
    tem_med_objeto = tem_medicamento_no_texto(obj_raw)
    tem_med_itens = False if tem_med_objeto else tem_medicamento_nos_itens(itens)

    if tem_med_objeto or tem_med_itens:
        if uf in ESTADOS_BLOQUEADOS:
            return ('IGNORAR', 'Medicamento em estado bloqueado')
        return ('CAPTURAR', 'Super passe: medicamentos')

    # 3. VETOS OPERACIONAIS/ALIMENTAÇÃO/EDUCAÇÃO
    for v in TODOS_VETOS:
        if v in obj_norm:
            if "NUTRICAO" in v or "ALIMENT" in v:
                if any(bom in obj_norm for bom in WL_NUTRI_CLINICA) and "ESCOLAR" not in obj_norm:
                    pass
                else:
                    return ('VETAR', f'Veto alimentação: {v}')
            else:
                return ('VETAR', f'Veto operacional: {v}')

    # 4. MMH/NUTRIÇÃO - Apenas Nordeste
    tem_mmh = any(t in obj_norm for t in WL_MATERIAIS_NE)
    tem_nutri = any(t in obj_norm for t in WL_NUTRI_CLINICA)

    if tem_mmh or tem_nutri:
        if uf in UFS_PERMITIDAS_MMH:
            return ('CAPTURAR', 'MMH/Nutrição no NE')
        else:
            return ('IGNORAR', 'MMH/Nutrição fora do NE')

    # 5. TERMOS AMPLOS (SAUDE/HOSPITAL) - Apenas Nordeste
    tem_termo_amplo = any(t in obj_norm for t in WL_TERMOS_AMPLos)
    if tem_termo_amplo:
        if uf in UFS_PERMITIDAS_MMH:
            return ('CAPTURAR', 'Termo amplo no NE')
        else:
            return ('IGNORAR', 'Termo amplo fora do NE')

    return ('IGNORAR', 'Não atende critérios')

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0

def salvar_checkpoint(dia, pagina):
    with open(ARQ_CHECKPOINT, 'w') as f:
        json.dump({'dia': dia, 'pagina': pagina}, f)

def carregar_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return json.load(f)
    return None

def extrair_dados_url_pncp(url):
    """Extrai CNPJ, Ano, Sequencial da URL do PNCP"""
    url = url.strip()
    padrao = r'pncp\.gov\.br/app/editais/(\d+)/(\d+)/(\d+)'
    match = re.search(padrao, url)
    if match:
        return match.group(1), match.group(2), match.group(3)
    return None, None, None

def processar_licitacao(lic, session, forcado=False):
    id_ref = "DESCONHECIDO"
    try:
        if not isinstance(lic, dict):
            return ('ERRO', {'msg': 'Formato JSON inválido da API principal'}, 0, 0)

        cnpj = lic.get('orgaoEntidade', {}).get('cnpj', '0000')
        ano = lic.get('anoCompra', '0000')
        seq = lic.get('sequencialCompra', '0000')
        id_ref = f"{cnpj}/{ano}/{seq}"

        sit_global_id = lic.get('situacaoCompraId') or 1
        sit_global_nome = MAPA_SITUACAO_GLOBAL.get(sit_global_id, "DIVULGADA")

        uo = lic.get('unidadeOrgao', {})
        uf = uo.get('ufSigla', '').upper()

        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()

        if not forcado:
            try:
                dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
                if dt_enc < DATA_CORTE_FIXA:
                    return ('IGNORADO', None, 0, 0)
            except:
                pass

        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        itens_brutos = []
        pagina_atual = 1
        max_paginas = 50

        while pagina_atual <= max_paginas:
            try:
                r_itens = session.get(url_itens, params={'pagina': pagina_atual, 'tamanhoPagina': 100}, timeout=20)
                if r_itens.status_code != 200:
                    if pagina_atual == 1:
                        return ('ERRO', {'msg': f"HTTP {r_itens.status_code} em {url_itens}"}, 0, 0)
                    else:
                        break

                resp_json = r_itens.json()
                itens_raw = resp_json.get('data', []) if isinstance(resp_json, dict) else (resp_json if isinstance(resp_json, list) else [])

                if not itens_raw:
                    break

                for it in itens_raw:
                    if not isinstance(it, dict):
                        continue
                    desc = it.get('descricao', '')
                    sit_id = int(it.get('situacaoCompraItem') or 1)
                    sit_nome = MAPA_SITUACAO_ITEM.get(sit_id, "EM ANDAMENTO")
                    benef_id = it.get('tipoBeneficioId')
                    benef_nome_api = str(it.get('tipoBeneficioNome', '')).upper()
                    benef_final = benef_id if benef_id in [1, 2, 3] else (1 if "EXCLUSIVA" in benef_nome_api else (3 if "COTA" in benef_nome_api else 4))

                    itens_brutos.append({
                        'n': it.get('numeroItem'),
                        'd': desc,
                        'q': safe_float(it.get('quantidade')),
                        'u': it.get('unidadeMedida', 'UN'),
                        'v_est': safe_float(it.get('valorUnitarioEstimado')),
                        'benef': benef_final,
                        'sit': sit_nome,
                        'res_forn': None,
                        'res_val': 0.0
                    })

                if len(itens_raw) < 100:
                    break
                pagina_atual += 1
            except Exception as e:
                print(f"   ⚠️ Erro ao buscar itens página {pagina_atual}: {e}")
                break

        if not itens_brutos and not forcado:
            return ('IGNORADO', None, 0, 0)

        status, motivo = veta_edital(obj_raw, uf, itens_brutos if not forcado else None)

        if status == 'VETAR':
            return ('VETADO', {'motivo': motivo}, 0, 0)
        elif status == 'IGNORAR':
            return ('IGNORADO', {'motivo': motivo}, 0, 0)

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}",
            'dt_enc': dt_enc_str,
            'uf': uf,
            'uasg': lic.get('unidadeOrgao', {}).get('codigoUnidade', '---'),
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'),
            'unid_nome': lic.get('unidadeOrgao', {}).get('nomeUnidade', '---'),
            'cid': lic.get('unidadeOrgao', {}).get('municipioNome', '---'),
            'obj': obj_raw,
            'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'val_tot': safe_float(lic.get('valorTotalEstimado')),
            'itens': itens_brutos,
            'sit_global': sit_global_nome,
            'fonte': lic.get('nomeEntidadeIntegradora', 'PNCP Direto')
        }
        return ('CAPTURADO', dados_finais, len(itens_brutos), 0)

    except Exception as e:
        return ('ERRO', {'msg': f"Erro interno em {id_ref}: {str(e)}"}, 0, 0)

def processar_links_manuais(session, banco):
    """Processa links manuais do arquivo links_manuais.txt"""
    if not os.path.exists(ARQ_MANUAL):
        return {'processados': 0, 'erros': 0}

    stats = {'processados': 0, 'erros': 0, 'adicionados': 0}

    try:
        with open(ARQ_MANUAL, 'r', encoding='utf-8') as f:
            links = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except Exception as e:
        print(f"   ⚠️ Erro ao ler links_manuais.txt: {e}")
        return stats

    if not links:
        return stats

    print(f"
📎 Processando {len(links)} links manuais...")

    for url in links:
        cnpj, ano, seq = extrair_dados_url_pncp(url)
        if not cnpj:
            print(f"   ❌ URL inválida: {url}")
            stats['erros'] += 1
            continue

        chave = f"{cnpj}{ano}_{str(seq).zfill(5)}/{ano}"
        if chave in banco:
            print(f"   ℹ️ Já existe: {chave}")
            continue

        url_api = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}'
        try:
            r = session.get(url_api, timeout=30)
            if r.status_code != 200:
                print(f"   ❌ Erro HTTP {r.status_code} ao buscar {url}")
                stats['erros'] += 1
                continue

            lic = r.json()
            st, d, i_qtd, _ = processar_licitacao(lic, session, forcado=True)

            if st == 'CAPTURADO' and d:
                banco[f"{d['id'][:14]}_{d['edit']}"] = d
                stats['adicionados'] += 1
                print(f"   ✅ Adicionado: {d['edit']} - {d['obj'][:50]}...")
            elif st == 'VETADO':
                print(f"   🚫 Vetado: {url} - {d.get('motivo', '')}")
            elif st == 'IGNORAR':
                print(f"   ⚪ Ignorado: {url} - {d.get('motivo', '')}")
            else:
                print(f"   ❌ Erro ao processar: {url}")
                stats['erros'] += 1

        except Exception as e:
            print(f"   ❌ Exceção ao processar {url}: {e}")
            stats['erros'] += 1

    print(f"   📊 Links manuais: {stats['adicionados']} adicionados, {stats['erros']} erros")
    return stats

def buscar_periodo(session, banco, d_ini, d_fim):
    stats = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0}
    checkpoint = carregar_checkpoint()

    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia_obj = d_ini + timedelta(days=i)
        dia = dia_obj.strftime('%Y%m%d')

        if checkpoint and dia < checkpoint['dia']:
            continue

        print(f"
📅 DATA: {dia}")
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'

        pag = checkpoint['pagina'] if checkpoint and dia == checkpoint['dia'] else 1

        while True:
            try:
                r = session.get(url, params={
                    'dataInicial': dia,
                    'dataFinal': dia,
                    'codigoModalidadeContratacao': 6,
                    'pagina': pag,
                    'tamanhoPagina': 50
                }, timeout=30)

                if r.status_code != 200:
                    print(f"   ⚠️ Erro crítico HTTP {r.status_code}. Salvando checkpoint.")
                    salvar_checkpoint(dia, pag)
                    break

                dados = r.json()
                lics = dados.get('data', [])
                if not lics:
                    break

                tot_pag = dados.get('totalPaginas', 1)
                s_pag = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0}

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                    futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                    for f in concurrent.futures.as_completed(futuros):
                        st, d, i_qtd, _ = f.result()
                        if st == 'CAPTURADO' and d:
                            s_pag['capturados'] += 1
                            s_pag['itens'] += i_qtd
                            banco[f"{d['id'][:14]}_{d['edit']}"] = d
                        elif st == 'VETADO':
                            s_pag['vetados'] += 1
                        elif st == 'IGNORADO':
                            s_pag['ignorados'] += 1
                        elif st == 'ERRO':
                            s_pag['erros'] += 1

                for k in stats:
                    stats[k] += s_pag[k]

                print(f"   📄 Pág {pag}/{tot_pag}: 🎯 {s_pag['capturados']} Caps | 🚫 {s_pag['vetados']} Vets | ⚪ {s_pag['ignorados']} Ign | 🔥 {s_pag['erros']} Erros")

                salvar_checkpoint(dia, pag + 1)

                if pag >= tot_pag:
                    salvar_checkpoint((dia_obj + timedelta(days=1)).strftime('%Y%m%d'), 1)
                    break
                pag += 1

            except Exception as e:
                print(f"   ⚠️ Falha na página {pag}: {e}. Salvando checkpoint.")
                salvar_checkpoint(dia, pag)
                break

    return stats

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK):
        print("🔒 Execução já em andamento. Saindo.")
        sys.exit(0)

    with open(ARQ_LOCK, 'w') as f:
        f.write("lock")

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str)
        parser.add_argument('--end', type=str)
        args = parser.parse_args()

        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=15)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()

        print(f"🚀 Sniper Pharma v22.2 - Período: {dt_start} a {dt_end}")

        session = criar_sessao()
        banco = {}

        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    dados_existentes = json.load(f)
                    for x in dados_existentes:
                        chave = f"{x.get('id', '')[:14]}_{x.get('edit', '')}"
                        banco[chave] = x
                print(f"📦 Banco carregado: {len(banco)} licitações")
            except Exception as e:
                print(f"⚠️ Erro ao carregar banco: {e}")

        stats = buscar_periodo(session, banco, dt_start, dt_end)
        print(f"
📊 Resumo busca: 🎯 {stats['capturados']} | 🚫 {stats['vetados']} | ⚪ {stats['ignorados']} | 🔥 {stats['erros']}")

        stats_manual = processar_links_manuais(session, banco)

        print("
💾 Salvando banco de dados...")
        with gzip.open(ARQ_TEMP, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)

        if os.path.exists(ARQ_TEMP):
            os.replace(ARQ_TEMP, ARQDADOS)
            if os.path.exists(ARQ_CHECKPOINT):
                os.remove(ARQ_CHECKPOINT)
            print(f"✅ Banco atualizado: {len(banco)} licitações totais")

    finally:
        if os.path.exists(ARQ_LOCK):
            os.remove(ARQ_LOCK)
