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
ARQ_MANUAL = 'links_manuais.txt' 
MAXWORKERS = 15 
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- GEOGRAFIA E MAPAS ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
# Limite estrito para MMH e Dietas (inclui tolerância API/Órgãos Federais no DF)
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', ''] 

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

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

VETOS_ALIMENTACAO = [normalize(x) for x in ["ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE", "PERECIVEIS", "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS", "LANCHE", "REFEICOES", "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE", "POLPA DE FRUTA", "ESTIAGEM"]]
VETOS_EDUCACAO = [normalize(x) for x in ["MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS", "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO", "SECRETARIA DE EDUCACAO"]]
VETOS_OPERACIONAL = [normalize(x) for x in ["OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", 
                                            "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO"]]
VETOS_ADM = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "CREDENCIAMENTO", "LEILAO", "ALIENACAO", "CORRETIVA"]]
TODOS_VETOS = VETOS_ALIMENTACAO + VETOS_EDUCACAO + VETOS_OPERACIONAL + VETOS_ADM

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO", "ACETILCISTEINA", "ACETILSALICILICO", "ACICLOVIR", "ADENOSINA", "ADRENALINA", "ALBENDAZOL", "ALENDRONATO", 
                                          "ALFAEPOETINA", "ALFAINTERFERONA", "ALFAST", "ALOPURINOL", "ALPRAZOLAM", "AMBROXOL", "AMBROXOL XPE", "AMINOFILINA", "AMIODARONA", "AMITRIPTILINA", "AMOXICILINA", "AMPICILINA", "ANASTROZOL", "ANFOTERICINA", "ANLODIPINO", "ARIPIPRAZOL", 
                                          "ARIPIPRAZOL.", "ATENOLOL", "ATORVASTANTINA", "ATORVASTATINA", "ATORVASTATINA CALCICA", "ATRACURIO", "ATROPINA", "AZITROMICINA", "AZTREONAM", "BACLOFENO", "BAMIFILINA", "BENZILPENICILINA", "BENZOATO", "BETAMETASONA", "BEZAFIBRATO", 
                                          "BIMATOPROSTA", "BISACODIL", "BISSULFATO", "BOPRIV", "BROMOPRIDA", "BUDESONIDA", "BUPROPIONA", "BUTILBROMETO", "CABERGOLINA", "CALCITRIOL", "CANDESARTANA", "CAPTOPRIL", "CARBAMAZEPINA", "CARBONATO", "CARVEDILOL", "CAVERDILOL", "CEFALEXINA", 
                                          "CEFALOTINA", "CEFAZOLINA", "CEFEPIMA", "CEFOTAXIMA", "CEFOXITINA", "CEFTAZIDIMA", "CEFTRIAXONA", "CEFUROXIMA", "CETOCONAZOL", "CETOPROFENO", "CETOROLACO", "CICLOBENZAPRINA", "CICLOSPORINA", "CILOSTAZOL", "CIMETIDINA", "CIPROFLOXACINO", 
                                          "CIPROFLOXACINA", "CITALOPRAM", "CLARITROMICINA", "CLINDAMICINA", "CLOBETASOL", "CLOMIPRAMINA", "CLONAZEPAM", "CLONIDINA", "CLOPIDOGREL", "CLORETO", "CLORIDRATO", "CLORIDRATO DE CIPROFLOXACINO", "CLORPROMAZINA", "CLORTALIDONA", "CLOTRIMAZOL", 
                                          "CLOZAPINA", "CODEINA", "COLCHICINA", "COLECALCIFEROL", "COLISTIMETATO", "COMPLEXO B", "DACARBZINA", "DAPAGLIFLOZINA", "DAPAGLIFLOZINA.", "DAPSONA", "DAPTOMICINA", "DARBEPOETINA", "DESLANOSIDEO", "DESLORATADINA", "DEXAMETASONA", "DEXCLORFENIRAMINA", 
                                          "DEXPANTENOL", "DIAZEPAM", "DIETILAMONIO", "DICLOFENACO", "DIGOXINA", "DILTIAZEM", "DIMETICONA", "DIOSMINA", "DIPIRONA", "DOBUTAMINA", "DOMPERIDONA", "DONEPEZILA", "DOPAMINA", "DOXAZOSINA", "DOXICICLINA", "DROPERIDOL", "DULAGLUTIDA", "DULOXETINA", 
                                          "DUTASTERIDA", "ECONAZOL", "EMULSAO", "ENALAPRIL", "ENOXAPARINA", "ENTACAPONA", "EPINEFRINA", "ERITROMICINA", "ESCITALOPRAM", "ESOMEPRAZOL", "ESPIRONOLACTONA", "ESTRADIOL", "ESTRIOL", "ESTROGENIOS", "ETANERCEPTE", "ETANERCEPTE", "ETILEFRINA", 
                                          "ETOMIDATO", "ETOPOSIDEO", "EZETIMIBA", "FAMOTIDINA", "FENITOINA", "FENOBARBITAL", "FENOTEROL", "FENTANILA", "FERRO", "FIBRINOGENIO", "FILGRASTIM", "FINASTERIDA", "FITOMENADIONA", "FLUCONAZOL", "FLUDROCORTISONA", "FLUMAZENIL", "FLUNARIZINA", 
                                          "FLUOXETINA", "FLUTICASONA", "FOLATO", "FONDAPARINUX", "FORMOTEROL", "FOSFATO", "FUROSEMIDA", "GABAPENTINA", "GANCICLOVIR", "GELADEIRA", "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICEROL", "GLICLAZIDA", "GLICOSE", "GLIMEPIRIDA", "GLUCAGON", 
                                          "HALOPERIDOL", "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA", "HIDROTALCITA", "HIDROXIDOPROGESTERONA", "HIDROXIDO", "HIDROXIPROGESTERONA", "HIDROXIUREIA", "HIOSCINA", "HIPROMELOSE", "IBUPROFENO", "IMIPENEM", "IMIPRAMINA", "INDAPAMIDA", 
                                          "INSULINA", "IOIMBINA", "IPRATROPIO", "IRBESARTANA", "IRINOTECANO", "ISOSSORBIDA", "ISOTRETINOINA", "ITRACONAZOL", "IVERMECTINA", "LACTULOSE", "LAMOTRIGINA", "LANSOPRAZOL", "LATANOPROSTA", "LEFLUNOMIDA", "LERCANIDIPINO", "LETROZOL", "LEVODOPA", "LEVOFLOXACINO", 
                                          "LEVOMEPROMAZINA", "LEVONORGESTREL", "LEVOTIROXINA", "LIDOCAINA", "LINEZOLIDA", "LINOGLIPTINA", "LIPIDICA", "LISINOPRIL", "LITIO", "LOPERAMIDA", "LORATADINA", "LORAZEPAM", "LOSARTANA", "LOVASTATINA", "MAGNESIO", "MANITOL", "MEBENDAZOL", "MEDROXIPROGESTERONA", 
                                          "MEMANTINA", "MEROPENEM", "MESALAZINA", "METILDOPA", "METILPREDNISOLONA", "METOCLOPRAMIDA", "METOPROLOL", "METOTREXATO", "METRONIDAZOL", "MICOFENOLATO", "MIDAZOLAM", "MIRTAZAPINA", "MISOPROSTOL", "MORFINA", "MUPIROCINA", "NARATRIPTANA", "NEOMICINA", "NEOSTIGMINA", 
                                          "NIFEDIPINO", "NIMESULIDA", "NIMODIPINO", "NISTATINA", "NITROFURANTOINA", "NITROGLICERINA", "NITROPRUSSIATO", "NORETISTERONA", "NORFLOXACINO", "NORTRIPTILINA", "OCTREOTIDA", "OLANZAPINA", "OLMESARTANA", "OMEPRAZOL", "ONDANSETRONA", "OXALIPLATINA", "OXCARBAZEPINA", 
                                          "OXIBUTININA", "PACLITAXEL", "PALONOSETRONA", "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", "PENICILINA", "PERICIAZINA", "PERMETRINA", "PETIDINA", "PIRAZINAMIDA", "PIRIDOSTIGMINA", "PIRIDOXINA", "POLIMIXINA", "POLIVITAMINICO", "POTASSIO", "PRAMIPEXOL", 
                                          "PRAVASTATINA", "PREDNISOLONA", "PREDNISONA", "PREGABALINA", "PROMETAZINA", "PROPATILNITRATO", "PROPOFOL", "PROPRANOLOL", "PROSTIGMINA", "QUETIAPINA", "RAMIPRIL", "RANITIDINA", "RESERPINA", "RIFAMPICINA", "RISPERIDONA", "RITONAVIR", "RIVAROXABANA", "ROCURONIO", 
                                          "ROSUVASTATINA", "SACARATO", "SALBUTAMOL", "SECAM", "SERTRALINA", "SEVELAMER", "SINVASTATINA", "SODIO", "SUCCINILCOLINA", "SUCRALFATO", "SULFADIAZINA", "SULFAMETOXAZOL", "SULFATO", "SULPIRIDA", "SUXAMETONIO", "TAMOXIFENO", "TANSULOSINA", "TEMOZOLAMIDA", "TEMOZOLOMIDA", 
                                          "TENOXICAN", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIOPENTAL", "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL", "TRAVOPROSTA", "TRIMETOPRIMA", "TROMETAMOL", "TROPICAMIDA", "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASELINA"]]
WL_NUTRI_CLINICA = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL"]]
WL_MATERIAIS_NE = [normalize(x) for x in ["MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO", "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE", 
                                          "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS", "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL", "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO", "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO", "TOUCA DESCARTAVEL", 
                                         "TUBO ASPIRACAO", "CORRELATO", "AGULHAS", "SERINGAS"]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/22.1'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    for v in TODOS_VETOS:
        if v in obj:
            if "NUTRICAO" in v or "ALIMENT" in v:
                if any(bom in obj for bom in WL_NUTRI_CLINICA) and "ESCOLAR" not in obj: return False
            return True
    return False

def safe_float(val):
    try: return float(val) if val is not None else 0.0
    except: return 0.0

def processar_licitacao(lic, session, forcado=False):
    id_ref = "DESCONHECIDO"
    try:
        if not isinstance(lic, dict): return ('ERRO', {'msg': 'Formato JSON inválido da API principal'}, 0, 0)
        
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
            # 1. BARREIRA GEOGRÁFICA GLOBAL: Corta sumariamente os estados que nunca atua
            if uf and uf in ESTADOS_BLOQUEADOS: 
                return ('IGNORADO', None, 0, 0)
            
            # 2. BARREIRA TEMPORAL E DE VETOS
            dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)
            if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 0)

            # 3. ROTEAMENTO DE INTERESSE POR CATEGORIA E GEOGRAFIA
            tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS) or any(x in obj_norm for x in ["SAUDE", "HOSPITAL"])
            tem_mmh_nutri = any(t in obj_norm for t in WL_MATERIAIS_NE + WL_NUTRI_CLINICA)

            # Regra de Negócio Dinâmica:
            # - Medicamentos: Passam livremente (os bloqueados já foram barrados no passo 1).
            # - MMH/Dietas: Passam APENAS se a UF for do Nordeste, for DF, ou for vazia.
            tem_interesse = tem_med or (tem_mmh_nutri and (uf in UFS_PERMITIDAS_MMH))

            if not tem_interesse: return ('IGNORADO', None, 0, 0)

        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        itens_brutos = []
        tem_item_catalogo = forcado 
        pagina_atual = 1
        
        while True:
            r_itens = session.get(url_itens, params={'pagina': pagina_atual, 'tamanhoPagina': 100}, timeout=20)
            if r_itens.status_code != 200: 
                if pagina_atual == 1: return ('ERRO', {'msg': f"HTTP {r_itens.status_code} ao aceder a {url_itens}"}, 0, 0)
                else: break
            
            resp_json = r_itens.json()
            
            if isinstance(resp_json, dict):
                itens_raw = resp_json.get('data', [])
            elif isinstance(resp_json, list):
                itens_raw = resp_json
            else:
                break

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

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': lic.get('unidadeOrgao', {}).get('codigoUnidade', '---'),
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'), 
            'unid_nome': lic.get('unidadeOrgao', {}).get('nomeUnidade', '---'),
            'cid': lic.get('unidadeOrgao', {}).get('municipioNome', '---'), 
            'obj': obj_raw, 'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': safe_float(lic.get('valorTotalEstimado')), 
            'itens': itens_brutos,
            'sit_global': sit_global_nome
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
                        chave_negocio = f"{d['id'][:14]}_{d['edit']}"
                        banco[chave_negocio] = d
                        print(f"   ✅ Captura Manual Sucesso: {cnpj}/{ano}/{seq}")
                    elif st == 'ERRO':
                        print(f"   ❌ Falha Manual em {cnpj}/{ano}/{seq}: {d['msg']}")
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
                if r.status_code != 200: 
                    print(f"   ⚠️ Erro crítico da API (Página Inicial): HTTP {r.status_code}")
                    break
                dados = r.json(); lics = dados.get('data', [])
                if not lics: break
            except Exception as e: 
                print(f"   ⚠️ Falha de conexão com PNCP ao buscar dia {dia}: {e}")
                break
            
            tot_pag = dados.get('totalPaginas', 1)
            s_pag = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0}

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    st, d, i, h = f.result()
                    
                    if st == 'CAPTURADO' and d:
                        s_pag['capturados'] += 1
                        s_pag['itens'] += i
                        banco[f"{d['id'][:14]}_{d['edit']}"] = d
                        
                    elif st == 'VETADO': s_pag['vetados'] += 1
                    elif st == 'IGNORADO': s_pag['ignorados'] += 1
                    elif st == 'ERRO': 
                        s_pag['erros'] += 1
                        print(f"      [!] LOG ERRO: {d['msg']}")
            
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
    print(f"🔥 ERROS DA API:       {stats['erros']}")
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
