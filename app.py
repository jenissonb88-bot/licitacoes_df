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
ARQ_MANUAL = 'links_manuais.txt'
MAXWORKERS = 15
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- ENDPOINTS ---
# API de Consulta Pública (mantida - funciona)
URL_CONSULTA_PUBLICA = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'

# API de Dados Abertos (nova)
URL_DADOS_ABERTOS = 'https://dadosabertos.compras.gov.br/modulo-contratacoes'
ENDPOINT_ITENS_DA = f"{URL_DADOS_ABERTOS}/2_consultarItensContratacoes_PNCP_14133"

# API Antiga (fallback para quando dados abertos der 404)
URL_API_ANTIGA = 'https://pncp.gov.br/api/pncp/v1'

# --- GEOGRAFIA E MAPAS ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', '']

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- CATÁLOGOS (mantidos) ---
VETOS_ALIMENTACAO = [normalize(x) for x in ["ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE", "PERECIVEIS", "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS", "LANCHE", "REFEICOES", "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE", "POLPA DE FRUTA", "ESTIAGEM"]]
VETOS_EDUCACAO = [normalize(x) for x in ["MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS", "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO", "SECRETARIA DE EDUCACAO"]]
VETOS_OPERACIONAL = [normalize(x) for x in ["OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO", "CORRETIVA", "VEICULO", "AMBULANCIA", "MOTOCICLETA", "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO", "EQUIPAMENTO E MATERIA PERMANENTE", "RECARGA", "ASFATIC", "CONFECCAO"]]
VETOS_ADM = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "CREDENCIAMENTO", "LEILAO", "ALIENACAO"]]
TODOS_VETOS = VETOS_ALIMENTACAO + VETOS_EDUCACAO + VETOS_OPERACIONAL + VETOS_ADM

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO", "ACETILCISTEINA", "ACETILSALICILICO", "ACICLOVIR", "ADENOSINA", "ADRENALINA", "ALBENDAZOL", "ALENDRONATO", "ALFAEPOETINA", "ALFAINTERFERONA", "ALFAST", "ALOPURINOL", "ALPRAZOLAM", "AMBROXOL", "AMBROXOL XPE", "AMINOFILINA", "AMIODARONA", "AMITRIPTILINA", "AMOXICILINA", "AMPICILINA", "ANASTROZOL", "ANFOTERICINA", "ANLODIPINO", "ARIPIPRAZOL", "ARIPIPRAZOL.", "ATENOLOL", "ATORVASTANTINA", "ATORVASTATINA", "ATORVASTATINA CALCICA", "ATRACURIO", "ATROPINA", "AZITROMICINA", "AZTREONAM", "BACLOFENO", "BAMIFILINA", "BENZILPENICILINA", "BENZOATO", "BETAMETASONA", "BEZAFIBRATO", "BIMATOPROSTA", "BISACODIL", "BISSULFATO", "BOPRIV", "BROMOPRIDA", "BUDESONIDA", "BUPROPIONA", "BUTILBROMETO", "CABERGOLINA", "CALCITRIOL", "CANDESARTANA", "CAPTOPRIL", "CARBAMAZEPINA", "CARBONATO", "CARVEDILOL", "CAVERDILOL", "CEFALEXINA", "CEFALOTINA", "CEFAZOLINA", "CEFEPIMA", "CEFOTAXIMA", "CEFOXITINA", "CEFTAZIDIMA", "CEFTRIAXONA", "CEFUROXIMA", "CETOCONAZOL", "CETOPROFENO", "CETOROLACO", "CICLOBENZAPRINA", "CICLOSPORINA", "CILOSTAZOL", "CIMETIDINA", "CIPROFLOXACINO", "CIPROFLOXACINA", "CITALOPRAM", "CLARITROMICINA", "CLINDAMICINA", "CLOBETASOL", "CLOMIPRAMINA", "CLONAZEPAM", "CLONIDINA", "CLOPIDOGREL", "CLORETO", "CLORIDRATO", "CLORIDRATO DE CIPROFLOXACINO", "CLORPROMAZINA", "CLORTALIDONA", "CLOTRIMAZOL", "CLOZAPINA", "CODEINA", "COLCHICINA", "COLECALCIFEROL", "COLISTIMETATO", "COMPLEXO B", "DACARBZINA", "DAPAGLIFLOZINA", "DAPAGLIFLOZINA.", "DAPSONA", "DAPTOMICINA", "DARBEPOETINA", "DESLANOSIDEO", "DESLORATADINA", "DEXAMETASONA", "DEXCLORFENIRAMINA", "DEXPANTENOL", "DIAZEPAM", "DIETILAMONIO", "DICLOFENACO", "DIGOXINA", "DILTIAZEM", "DIMETICONA", "DIOSMINA", "DIPIRONA", "DOBUTAMINA", "DOMPERIDONA", "DONEPEZILA", "DOPAMINA", "DOXAZOSINA", "DOXICICLINA", "DROPERIDOL", "DULAGLUTIDA", "DULOXETINA", "DUTASTERIDA", "ECONAZOL", "EMULSAO", "ENALAPRIL", "ENOXAPARINA", "ENTACAPONA", "EPINEFRINA", "ERITROMICINA", "ESCITALOPRAM", "ESOMEPRAZOL", "ESPIRONOLACTONA", "ESTRADIOL", "ESTRIOL", "ESTROGENIOS", "ETANERCEPTE", "ETILEFRINA", "ETOMIDATO", "ETOPOSIDEO", "EZETIMIBA", "FAMOTIDINA", "FENITOINA", "FENOBARBITAL", "FENOTEROL", "FENTANILA", "FERRO", "FIBRINOGENIO", "FILGRASTIM", "FINASTERIDA", "FITOMENADIONA", "FLUCONAZOL", "FLUDROCORTISONA", "FLUMAZENIL", "FLUNARIZINA", "FLUOXETINA", "FLUTICASONA", "FOLATO", "FONDAPARINUX", "FORMOTEROL", "FOSFATO", "FUROSEMIDA", "GABAPENTINA", "GANCICLOVIR", "GELADEIRA", "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICEROL", "GLICLAZIDA", "GLICOSE", "GLIMEPIRIDA", "GLUCAGON", "HALOPERIDOL", "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA", "HIDROTALCITA", "HIDROXIDOPROGESTERONA", "HIDROXIDO", "HIDROXIPROGESTERONA", "HIDROXIUREIA", "HIOSCINA", "HIPROMELOSE", "IBUPROFENO", "IMIPENEM", "IMIPRAMINA", "INDAPAMIDA", "INSULINA", "IOIMBINA", "IPRATROPIO", "IRBESARTANA", "IRINOTECANO", "ISOSSORBIDA", "ISOTRETINOINA", "ITRACONAZOL", "IVERMECTINA", "LACTULOSE", "LAMOTRIGINA", "LANSOPRAZOL", "LATANOPROSTA", "LEFLUNOMIDA", "LERCANIDIPINO", "LETROZOL", "LEVODOPA", "LEVOFLOXACINO", "LEVOMEPROMAZINA", "LEVONORGESTREL", "LEVOTIROXINA", "LIDOCAINA", "LINEZOLIDA", "LINOGLIPTINA", "LIPIDICA", "LISINOPRIL", "LITIO", "LOPERAMIDA", "LORATADINA", "LORAZEPAM", "LOSARTANA", "LOVASTATINA", "MAGNESIO", "MANITOL", "MEBENDAZOL", "MEDROXIPROGESTERONA", "MEMANTINA", "MEROPENEM", "MESALAZINA", "METILDOPA", "METILPREDNISOLONA", "METOCLOPRAMIDA", "METOPROLOL", "METOTREXATO", "METRONIDAZOL", "MICOFENOLATO", "MIDAZOLAM", "MIRTAZAPINA", "MISOPROSTOL", "MORFINA", "MUPIROCINA", "NARATRIPTANA", "NEOMICINA", "NEOSTIGMINA", "NIFEDIPINO", "NIMESULIDA", "NIMODIPINO", "NISTATINA", "NITROFURANTOINA", "NITROGLICERINA", "NITROPRUSSIATO", "NORETISTERONA", "NORFLOXACINO", "NORTRIPTILINA", "OCTREOTIDA", "OLANZAPINA", "OLMESARTANA", "OMEPRAZOL", "ONDANSETRONA", "OXALIPLATINA", "OXCARBAZEPINA", "OXIBUTININA", "PACLITAXEL", "PALONOSETRONA", "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", "PENICILINA", "PERICIAZINA", "PERMETRINA", "PETIDINA", "PIRAZINAMIDA", "PIRIDOSTIGMINA", "PIRIDOXINA", "POLIMIXINA", "POLIVITAMINICO", "POTASSIO", "PRAMIPEXOL", "PRAVASTATINA", "PREDNISOLONA", "PREDNISONA", "PREGABALINA", "PROMETAZINA", "PROPATILNITRATO", "PROPOFOL", "PROPRANOLOL", "PROSTIGMINA", "QUETIAPINA", "RAMIPRIL", "RANITIDINA", "RESERPINA", "RIFAMPICINA", "RISPERIDONA", "RITONAVIR", "RIVAROXABANA", "ROCURONIO", "ROSUVASTATINA", "SACARATO", "SALBUTAMOL", "SECAM", "SERTRALINA", "SEVELAMER", "SINVASTATINA", "SODIO", "SUCCINILCOLINA", "SUCRALFATO", "SULFADIAZINA", "SULFAMETOXAZOL", "SULFATO", "SULPIRIDA", "SUXAMETONIO", "TAMOXIFENO", "TANSULOSINA", "TEMOZOLAMIDA", "TEMOZOLOMIDA", "TENOXICAN", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIOPENTAL", "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL", "TRAVOPROSTA", "TRIMETOPRIMA", "TROMETAMOL", "TROPICAMIDA", "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASELINA","AQUISICAO DE MEDICAMENTO"]]
WL_NUTRI_CLINICA = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL", "PARENTERA","ENTERA"]]
WL_MATERIAIS_NE = [normalize(x) for x in ["MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO", "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE", "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS", "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL", "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO", "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO", "TOUCA DESCARTAVEL", "TUBO ASPIRACAO", "CORRELATO", "AGULHAS", "SERINGAS"]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'Sniper Pharma/22.1',
        'Accept-Encoding': 'gzip, deflate, br'
    })
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
    try: return float(val) if val is not None else 0.0
    except: return 0.0

def salvar_checkpoint(dia, pagina):
    with open(ARQ_CHECKPOINT, 'w') as f:
        json.dump({'dia': dia, 'pagina': pagina}, f)

def carregar_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return json.load(f)
    return None

# ✅ NOVO: Busca itens com fallback para API antiga
def buscar_itens_com_fallback(cnpj, ano, seq, session):
    """
    Tenta buscar itens na API de dados abertos.
    Se der 404, faz fallback para API antiga.
    """
    # Tentativa 1: API de Dados Abertos (nova)
    params = {
        'orgaoEntidadeCnpj': cnpj,
        'anoCompraPncp': int(ano),  # Converter para int
        'sequencialCompraPncp': int(seq),  # Converter para int
        'pagina': 1,
        'tamanhoPagina': 500
    }
    
    try:
        r = session.get(ENDPOINT_ITENS_DA, params=params, timeout=30)
        
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                itens = data.get('data', []) or data.get('itens', []) or data.get('resultado', [])
            elif isinstance(data, list):
                itens = data
            else:
                itens = []
            
            if itens:
                print(f"   ✅ Dados Abertos: {len(itens)} itens")
                return itens, 'dados_abertos'
        
        elif r.status_code == 404:
            print(f"   ⚠️ Dados Abertos 404, tentando API antiga...")
        else:
            print(f"   ⚠️ Dados Abertos HTTP {r.status_code}")
            
    except Exception as e:
        print(f"   ⚠️ Erro Dados Abertos: {e}")
    
    # Tentativa 2: API Antiga (fallback)
    url_antiga = f'{URL_API_ANTIGA}/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
    try:
        pagina = 1
        itens_totais = []
        
        while True:
            r = session.get(
                url_antiga, 
                params={'pagina': pagina, 'tamanhoPagina': 100}, 
                timeout=20
            )
            
            if r.status_code != 200:
                if pagina == 1:
                    print(f"   ❌ API Antiga HTTP {r.status_code}")
                    return None, 'falha'
                break
            
            data = r.json()
            itens_pagina = data.get('data', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            
            if not itens_pagina:
                break
            
            itens_totais.extend(itens_pagina)
            
            if len(itens_pagina) < 100:
                break
            
            pagina += 1
        
        if itens_totais:
            print(f"   ✅ API Antiga: {len(itens_totais)} itens")
            return itens_totais, 'api_antiga'
            
    except Exception as e:
        print(f"   ❌ Erro API Antiga: {e}")
    
    return None, 'falha'

def processar_licitacao(lic, session, forcado=False):
    id_ref = "DESCONHECIDO"
    try:
        if not isinstance(lic, dict): return ('ERRO', {'msg': 'Formato JSON inválido'}, 0, 0)
        
        cnpj = lic.get('orgaoEntidade', {}).get('cnpj', '0000')
        ano = str(lic.get('anoCompra', '0000'))
        seq = str(lic.get('sequencialCompra', '0000'))
        id_ref = f"{cnpj}/{ano}/{seq}"
        
        sit_global_id = lic.get('situacaoCompraId') or 1
        sit_global_nome = MAPA_SITUACAO_GLOBAL.get(sit_global_id, "DIVULGADA")
        
        uo = lic.get('unidadeOrgao', {})
        uf = uo.get('ufSigla', '').upper()
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        obj_norm = normalize(obj_raw)
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()
        
        if not forcado:
            if uf and uf in ESTADOS_BLOQUEADOS: return ('IGNORADO', None, 0, 0)
            try:
                dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
                if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)
            except:
                pass
            if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 0)
            
            palavras_magicas = ["MEDICAMENTO", "MEDICAMENTOS", "AQUISICAO DE MEDICAMENTOS"]
            tem_super_passe = any(p in obj_norm for p in palavras_magicas)
            tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
            tem_mmh_nutri = any(t in obj_norm for t in WL_MATERIAIS_NE + WL_NUTRI_CLINICA)
            tem_termo_amplo = any(x in obj_norm for x in ["SAUDE", "HOSPITAL"])
            
            if tem_super_passe or tem_med: tem_interesse = True
            elif tem_mmh_nutri and (uf in UFS_PERMITIDAS_MMH): tem_interesse = True
            elif tem_termo_amplo and (uf in UFS_PERMITIDAS_MMH): tem_interesse = True
            else: tem_interesse = False
            
            if not tem_interesse: return ('IGNORADO', None, 0, 0)

        # ✅ USA FUNÇÃO COM FALLBACK
        itens_brutos, fonte = buscar_itens_com_fallback(cnpj, ano, seq, session)
        
        if not itens_brutos:
            return ('IGNORADO', None, 0, 0)
        
        # Mapear itens
        itens_mapeados = []
        for it in itens_brutos:
            if not isinstance(it, dict):
                continue
            
            desc = it.get('descricao', '')
            sit_id = int(it.get('situacaoCompraItem') or 1)
            sit_nome = MAPA_SITUACAO_ITEM.get(sit_id, "EM ANDAMENTO")
            
            benef_id = it.get('tipoBeneficioId')
            benef_nome_api = str(it.get('tipoBeneficioNome', '')).upper()
            if benef_id in [1, 2, 3]:
                benef_final = benef_id
            elif "EXCLUSIVA" in benef_nome_api:
                benef_final = 1
            elif "COTA" in benef_nome_api:
                benef_final = 3
            else:
                benef_final = 4
            
            itens_mapeados.append({
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
            'itens': itens_mapeados,
            'sit_global': sit_global_nome,
            'fonte': lic.get('nomeEntidadeIntegradora', 'PNCP Direto'),
            'api_fonte': fonte  # Registra qual API funcionou
        }
        return ('CAPTURADO', dados_finais, len(itens_mapeados), 0)
        
    except Exception as e:
        return ('ERRO', {'msg': f"Erro em {id_ref}: {str(e)}"}, 0, 0)

def buscar_periodo(session, banco, d_ini, d_fim):
    stats = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0, 'fallbacks': 0}
    checkpoint = carregar_checkpoint()
    
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia_obj = d_ini + timedelta(days=i)
        dia = dia_obj.strftime('%Y%m%d')
        
        if checkpoint and dia < checkpoint['dia']: continue

        print(f"\n📅 DATA: {dia}")
        pag = checkpoint['pagina'] if checkpoint and dia == checkpoint['dia'] else 1
        
        while True:
            try:
                r = session.get(
                    URL_CONSULTA_PUBLICA,
                    params={
                        'dataInicial': dia,
                        'dataFinal': dia,
                        'codigoModalidadeContratacao': 6,
                        'pagina': pag,
                        'tamanhoPagina': 50
                    },
                    timeout=30
                )
                
                if r.status_code != 200:
                    print(f"   ⚠️ Erro crítico HTTP {r.status_code}. Abortando.")
                    break
                
                dados = r.json()
                lics = dados.get('data', [])
                if not lics: break
                
                tot_pag = dados.get('totalPaginas', 1)
                s_pag = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0, 'fallbacks': 0}

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                    futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                    for f in concurrent.futures.as_completed(futuros):
                        st, d, i_qtd, h = f.result()
                        if st == 'CAPTURADO' and d:
                            s_pag['capturados'] += 1
                            s_pag['itens'] += i_qtd
                            if d.get('api_fonte') == 'api_antiga':
                                s_pag['fallbacks'] += 1
                            banco[f"{d['id'][:14]}_{d['edit']}"] = d
                        elif st == 'VETADO': s_pag['vetados'] += 1
                        elif st == 'IGNORADO': s_pag['ignorados'] += 1
                        elif st == 'ERRO': s_pag['erros'] += 1

                for k in stats: stats[k] += s_pag.get(k, 0)
                print(f"   📄 Pág {pag}/{tot_pag}: 🎯 {s_pag['capturados']} Caps | 🔄 {s_pag['fallbacks']} FB | 🔥 {s_pag['erros']} Erros")
                
                salvar_checkpoint(dia, pag + 1)
                
                if pag >= tot_pag:
                    salvar_checkpoint((dia_obj + timedelta(days=1)).strftime('%Y%m%d'), 1)
                    break
                pag += 1
                
            except Exception as e:
                print(f"   ⚠️ Falha na página {pag}: {e}")
                break
    
    print(f"\n📊 Resumo: {stats['capturados']} capturados, {stats['fallbacks']} fallbacks, {stats['erros']} erros")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str)
        parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=15)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        
        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    for x in json.load(f): banco[f"{x.get('id', '')[:14]}_{x.get('edit', '')}"] = x
            except Exception as e: print(f"Erro ao carregar banco: {e}")

        buscar_periodo(session, banco, dt_start, dt_end)
        
        print("\n💾 Salvando...")
        with gzip.open(ARQ_TEMP, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        
        if os.path.exists(ARQ_TEMP):
            os.replace(ARQ_TEMP, ARQDADOS)
            if os.path.exists(ARQ_CHECKPOINT): os.remove(ARQ_CHECKPOINT)
            print("✅ Concluído!")

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
