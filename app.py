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

# --- GEOGRAFIA ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', '']

MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- CATÁLOGOS ---
VETOS_ALIMENTACAO = [normalize(x) for x in ["ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE", "PERECIVEIS", "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS", "LANCHE", "REFEICOES", "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE", "POLPA DE FRUTA", "ESTIAGEM"]]
VETOS_EDUCACAO = [normalize(x) for x in ["MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS", "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO", "SECRETARIA DE EDUCACAO"]]
VETOS_OPERACIONAL = [normalize(x) for x in ["OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO", "CORRETIVA", "VEICULO", "AMBULANCIA", "MOTOCICLETA", "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO", "EQUIPAMENTO E MATERIA PERMANENTE", "RECARGA", "ASFATIC", "CONFECCAO"]]
VETOS_ADM = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "CREDENCIAMENTO", "LEILAO", "ALIENACAO"]]
TODOS_VETOS = VETOS_ALIMENTACAO + VETOS_EDUCACAO + VETOS_OPERACIONAL + VETOS_ADM

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO", "ACETILCISTEINA", "ACETILSALICILICO", "ACICLOVIR", "ADENOSINA", "ADRENALINA", "ALBENDAZOL", "ALENDRONATO", "ALFAEPOETINA", "ALFAINTERFERONA", "ALFAST", "ALOPURINOL", "ALPRAZOLAM", "AMBROXOL", "AMBROXOL XPE", "AMINOFILINA", "AMIODARONA", "AMITRIPTILINA", "AMOXICILINA", "AMPICILINA", "ANASTROZOL", "ANFOTERICINA", "ANLODIPINO", "ARIPIPRAZOL", "ATENOLOL", "ATORVASTATINA", "ATORVASTATINA CALCICA", "ATRACURIO", "ATROPINA", "AZITROMICINA", "AZTREONAM", "BACLOFENO", "BAMIFILINA", "BENZILPENICILINA", "BENZOATO", "BETAMETASONA", "BEZAFIBRATO", "BIMATOPROSTA", "BISACODIL", "BISSULFATO", "BOPRIV", "BROMOPRIDA", "BUDESONIDA", "BUPROPIONA", "BUTILBROMETO", "CABERGOLINA", "CALCITRIOL", "CANDESARTANA", "CAPTOPRIL", "CARBAMAZEPINA", "CARBONATO", "CARVEDILOL", "CAVERDILOL", "CEFALEXINA", "CEFALOTINA", "CEFAZOLINA", "CEFEPIMA", "CEFOTAXIMA", "CEFOXITINA", "CEFTAZIDIMA", "CEFTRIAXONA", "CEFUROXIMA", "CETOCONAZOL", "CETOPROFENO", "CETOROLACO", "CICLOBENZAPRINA", "CICLOSPORINA", "CILOSTAZOL", "CIMETIDINA", "CIPROFLOXACINO", "CIPROFLOXACINA", "CITALOPRAM", "CLARITROMICINA", "CLINDAMICINA", "CLOBETASOL", "CLOMIPRAMINA", "CLONAZEPAM", "CLONIDINA", "CLOPIDOGREL", "CLORETO", "CLORIDRATO", "CLORPROMAZINA", "CLORTALIDONA", "CLOTRIMAZOL", "CLOZAPINA", "CODEINA", "COLCHICINA", "COLECALCIFEROL", "COLISTIMETATO", "COMPLEXO B", "DACARBZINA", "DAPAGLIFLOZINA", "DAPSONA", "DAPTOMICINA", "DARBEPOETINA", "DESLANOSIDEO", "DESLORATADINA", "DEXAMETASONA", "DEXCLORFENIRAMINA", "DEXPANTENOL", "DIAZEPAM", "DIETILAMONIO", "DICLOFENACO", "DIGOXINA", "DILTIAZEM", "DIMETICONA", "DIOSMINA", "DIPIRONA", "DOBUTAMINA", "DOMPERIDONA", "DONEPEZILA", "DOPAMINA", "DOXAZOSINA", "DOXICICLINA", "DROPERIDOL", "DULAGLUTIDA", "DULOXETINA", "DUTASTERIDA", "ECONAZOL", "EMULSAO", "ENALAPRIL", "ENOXAPARINA", "ENTACAPONA", "EPINEFRINA", "ERITROMICINA", "ESCITALOPRAM", "ESOMEPRAZOL", "ESPIRONOLACTONA", "ESTRADIOL", "ESTRIOL", "ESTROGENIOS", "ETANERCEPTE", "ETILEFRINA", "ETOMIDATO", "ETOPOSIDEO", "EZETIMIBA", "FAMOTIDINA", "FENITOINA", "FENOBARBITAL", "FENOTEROL", "FENTANILA", "FERRO", "FIBRINOGENIO", "FILGRASTIM", "FINASTERIDA", "FITOMENADIONA", "FLUCONAZOL", "FLUDROCORTISONA", "FLUMAZENIL", "FLUNARIZINA", "FLUOXETINA", "FLUTICASONA", "FOLATO", "FONDAPARINUX", "FORMOTEROL", "FOSFATO", "FUROSEMIDA", "GABAPENTINA", "GANCICLOVIR", "GELADEIRA", "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICEROL", "GLICLAZIDA", "GLICOSE", "GLIMEPIRIDA", "GLUCAGON", "HALOPERIDOL", "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA", "HIDROTALCITA", "HIDROXIDOPROGESTERONA", "HIDROXIDO", "HIDROXIPROGESTERONA", "HIDROXIUREIA", "HIOSCINA", "HIPROMELOSE", "IBUPROFENO", "IMIPENEM", "IMIPRAMINA", "INDAPAMIDA", "INSULINA", "IOIMBINA", "IPRATROPIO", "IRBESARTANA", "IRINOTECANO", "ISOSSORBIDA", "ISOTRETINOINA", "ITRACONAZOL", "IVERMECTINA", "LACTULOSE", "LAMOTRIGINA", "LANSOPRAZOL", "LATANOPROSTA", "LEFLUNOMIDA", "LERCANIDIPINO", "LETROZOL", "LEVODOPA", "LEVOFLOXACINO", "LEVOMEPROMAZINA", "LEVONORGESTREL", "LEVOTIROXINA", "LIDOCAINA", "LINEZOLIDA", "LINOGLIPTINA", "LIPIDICA", "LISINOPRIL", "LITIO", "LOPERAMIDA", "LORATADINA", "LORAZEPAM", "LOSARTANA", "LOVASTATINA", "MAGNESIO", "MANITOL", "MEBENDAZOL", "MEDROXIPROGESTERONA", "MEMANTINA", "MEROPENEM", "MESALAZINA", "METILDOPA", "METILPREDNISOLONA", "METOCLOPRAMIDA", "METOPROLOL", "METOTREXATO", "METRONIDAZOL", "MICOFENOLATO", "MIDAZOLAM", "MIRTAZAPINA", "MISOPROSTOL", "MORFINA", "MUPIROCINA", "NARATRIPTANA", "NEOMICINA", "NEOSTIGMINA", "NIFEDIPINO", "NIMESULIDA", "NIMODIPINO", "NISTATINA", "NITROFURANTOINA", "NITROGLICERINA", "NITROPRUSSIATO", "NORETISTERONA", "NORFLOXACINO", "NORTRIPTILINA", "OCTREOTIDA", "OLANZAPINA", "OLMESARTANA", "OMEPRAZOL", "ONDANSETRONA", "OXALIPLATINA", "OXCARBAZEPINA", "OXIBUTININA", "PACLITAXEL", "PALONOSETRONA", "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", "PENICILINA", "PERICIAZINA", "PERMETRINA", "PETIDINA", "PIRAZINAMIDA", "PIRIDOSTIGMINA", "PIRIDOXINA", "POLIMIXINA", "POLIVITAMINICO", "POTASSIO", "PRAMIPEXOL", "PRAVASTATINA", "PREDNISOLONA", "PREDNISONA", "PREGABALINA", "PROMETAZINA", "PROPATILNITRATO", "PROPOFOL", "PROPRANOLOL", "PROSTIGMINA", "QUETIAPINA", "RAMIPRIL", "RANITIDINA", "RESERPINA", "RIFAMPICINA", "RISPERIDONA", "RITONAVIR", "RIVAROXABANA", "ROCURONIO", "ROSUVASTATINA", "SACARATO", "SALBUTAMOL", "SECAM", "SERTRALINA", "SEVELAMER", "SINVASTATINA", "SODIO", "SUCCINILCOLINA", "SUCRALFATO", "SULFADIAZINA", "SULFAMETOXAZOL", "SULFATO", "SULPIRIDA", "SUXAMETONIO", "TAMOXIFENO", "TANSULOSINA", "TEMOZOLAMIDA", "TEMOZOLOMIDA", "TENOXICAN", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIOPENTAL", "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL", "TRAVOPROSTA", "TRIMETOPRIMA", "TROMETAMOL", "TROPICAMIDA", "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASELINA","AQUISICAO DE MEDICAMENTO"]]
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

# ✅ CORRIGIDO: Conversão de valores monetários
def safe_float(val):
    """
    Converte valor monetário do PNCP para float.
    Lida com: inteiros (centavos), strings formatadas, e formatos brasileiros.
    """
    if val is None:
        return 0.0
    
    try:
        # Se já é número, retornar direto
        if isinstance(val, (int, float)):
            # Se for inteiro grande (> 1000), provavelmente é centavos
            if isinstance(val, int) and val > 1000:
                return val / 100.0
            return float(val)
        
        # Se é string, limpar e converter
        val_str = str(val).strip()
        
        # Remover símbolo de moeda e espaços
        val_str = val_str.replace('R$', '').replace(' ', '')
        
        # Detectar formato brasileiro (vírgula como decimal)
        if ',' in val_str and '.' in val_str:
            # Formato: 1.234.567,89 → remover pontos, vírgula vira ponto
            val_str = val_str.replace('.', '').replace(',', '.')
        elif ',' in val_str:
            # Formato: 1234567,89 → vírgula vira ponto
            val_str = val_str.replace(',', '.')
        # Se só tem ponto, assumir que é separador decimal americano
        # ou separador de milhar se houver mais de um ponto
        
        # Converter para float
        resultado = float(val_str)
        
        # Heurística: se valor > 1000 e não tem casas decimais significativas,
        # provavelmente veio em centavos (API do PNCP às vezes faz isso)
        if resultado > 1000 and resultado == int(resultado):
            # Verificar se parece ser centavos (valor muito grande para ser reais)
            if resultado > 100000:  # R$ 100.000,00 em centavos = 10.000.000
                resultado = resultado / 100.0
        
        return resultado
        
    except Exception as e:
        # Fallback: tentar extrair qualquer número
        try:
            numeros = re.findall(r'[\d.,]+', str(val))
            if numeros:
                # Pegar o maior número encontrado
                maior = max(numeros, key=lambda x: len(x))
                # Tentar converter assumindo formato brasileiro
                limpo = maior.replace('.', '').replace(',', '.')
                return float(limpo)
        except:
            pass
        return 0.0

def salvar_checkpoint(dia, pagina):
    with open(ARQ_CHECKPOINT, 'w') as f:
        json.dump({'dia': dia, 'pagina': pagina}, f)

def carregar_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return json.load(f)
    return None

def log(msg, console=True, arquivo=True):
    timestamp = datetime.now().strftime('%H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    if console:
        print(linha)
    if arquivo:
        with open(ARQ_LOG, 'a', encoding='utf-8') as f:
            f.write(linha + '\n')

def buscar_itens_oficial(cnpj, ano, seq, session):
    """Busca itens usando a API oficial de integração PNCP."""
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens_totais = []
    pagina = 1
    max_paginas = 100
    
    while pagina <= max_paginas:
        try:
            r = session.get(
                url, 
                params={'pagina': pagina, 'tamanhoPagina': 100}, 
                timeout=20
            )
            
            if r.status_code == 404:
                return None, '404'
            elif r.status_code == 301:
                return None, '301'
            elif r.status_code != 200:
                if pagina == 1:
                    return None, f'http_{r.status_code}'
                break
            
            data = r.json()
            if isinstance(data, dict):
                itens_pagina = data.get('data', [])
            elif isinstance(data, list):
                itens_pagina = data
            else:
                break
            
            if not itens_pagina:
                break
            
            itens_totais.extend(itens_pagina)
            
            if len(itens_pagina) < 100:
                break
            
            pagina += 1
            
        except Exception as e:
            if pagina == 1:
                return None, f'erro_{str(e)[:50]}'
            break
    
    return itens_totais, 'ok'

def processar_licitacao(lic, session, forcado=False):
    id_ref = "DESCONHECIDO"
    try:
        if not isinstance(lic, dict): 
            return ('ERRO', None, 0, 'json_invalido')
        
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
            if uf and uf in ESTADOS_BLOQUEADOS: 
                return ('IGNORADO_GEO', None, 0, 'estado_bloqueado')
            try:
                dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
                if dt_enc < DATA_CORTE_FIXA: 
                    return ('IGNORADO_DATA', None, 0, 'data_antiga')
            except:
                pass
            if veta_edital(obj_raw, uf): 
                return ('VETADO', None, 0, 'palavra_veto')
            
            palavras_magicas = ["MEDICAMENTO", "MEDICAMENTOS", "AQUISICAO DE MEDICAMENTOS"]
            tem_super_passe = any(p in obj_norm for p in palavras_magicas)
            tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
            tem_mmh_nutri = any(t in obj_norm for t in WL_MATERIAIS_NE + WL_NUTRI_CLINICA)
            tem_termo_amplo = any(x in obj_norm for x in ["SAUDE", "HOSPITAL"])
            
            if tem_super_passe or tem_med: 
                tem_interesse = True
            elif tem_mmh_nutri and (uf in UFS_PERMITIDAS_MMH): 
                tem_interesse = True
            elif tem_termo_amplo and (uf in UFS_PERMITIDAS_MMH): 
                tem_interesse = True
            else: 
                tem_interesse = False
            
            if not tem_interesse: 
                return ('IGNORADO_TEMATICA', None, 0, 'sem_interesse')

        itens_brutos, fonte = buscar_itens_oficial(cnpj, ano, seq, session)
        
        if not itens_brutos:
            return ('ERRO_ITENS', None, 0, fonte)
        
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
            'api_fonte': fonte
        }
        return ('CAPTURADO', dados_finais, len(itens_mapeados), 'ok')
        
    except Exception as e:
        return ('ERRO', None, 0, f'excecao_{str(e)[:50]}')

def buscar_periodo(session, banco, d_ini, d_fim):
    stats_geral = {'vetados': 0, 'capturados': 0, 'itens': 0, 'ignorados': 0, 'erros': 0}
    checkpoint = carregar_checkpoint()
    
    if os.path.exists(ARQ_LOG):
        os.remove(ARQ_LOG)
    
    log("🚀 Iniciando captura Sniper Pharma", console=True, arquivo=True)
    log(f"📅 Período: {d_ini} até {d_fim}", console=True, arquivo=True)
    log("=" * 60, console=True, arquivo=True)
    
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia_obj = d_ini + timedelta(days=i)
        dia = dia_obj.strftime('%Y%m%d')
        dia_fmt = dia_obj.strftime('%d/%m/%Y')
        
        if checkpoint and dia < checkpoint['dia']: 
            continue

        log("", console=True, arquivo=False)
        log(f"📅 DATA: {dia_fmt} ({dia})", console=True, arquivo=True)
        log("-" * 60, console=True, arquivo=True)
        
        pag = checkpoint['pagina'] if checkpoint and dia == checkpoint['dia'] else 1
        
        while True:
            inicio_pag = time.time()
            
            try:
                r = session.get(
                    f"{API_CONSULTA}/contratacoes/publicacao",
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
                    log(f"   ❌ ERRO CRÍTICO: HTTP {r.status_code} na página {pag}", console=True, arquivo=True)
                    break
                
                dados = r.json()
                lics = dados.get('data', [])
                if not lics: 
                    log(f"   ℹ️ Nenhuma licitação encontrada nesta data", console=True, arquivo=True)
                    break
                
                tot_pag = dados.get('totalPaginas', 1)
                s_pag = {'capturados': 0, 'vetados': 0, 'ignorados_geo': 0, 'ignorados_data': 0, 'ignorados_tematica': 0, 'erros_itens': 0, 'erros_outros': 0, 'itens': 0}
                erros_detalhados = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                    futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                    for f in concurrent.futures.as_completed(futuros):
                        st, d, i_qtd, info = f.result()
                        if st == 'CAPTURADO' and d:
                            s_pag['capturados'] += 1
                            s_pag['itens'] += i_qtd
                            banco[f"{d['id'][:14]}_{d['edit']}"] = d
                        elif st == 'VETADO':
                            s_pag['vetados'] += 1
                        elif st == 'IGNORADO_GEO':
                            s_pag['ignorados_geo'] += 1
                        elif st == 'IGNORADO_DATA':
                            s_pag['ignorados_data'] += 1
                        elif st == 'IGNORADO_TEMATICA':
                            s_pag['ignorados_tematica'] += 1
                        elif st == 'ERRO_ITENS':
                            s_pag['erros_itens'] += 1
                            erros_detalhados.append(f"404:{info}" if info == '404' else info[:30])
                        else:
                            s_pag['erros_outros'] += 1
                            if info:
                                erros_detalhados.append(info[:30])

                tempo_pag = time.time() - inicio_pag
                
                log(f"   📄 Pág {pag}/{tot_pag} | ⏱️ {tempo_pag:.1f}s", console=True, arquivo=True)
                log(f"      🎯 Capturados: {s_pag['capturados']} ({s_pag['itens']} itens)", console=True, arquivo=True)
                log(f"      🔒 Vetados: {s_pag['vetados']} | 🌍 Geo: {s_pag['ignorados_geo']} | 📅 Data: {s_pag['ignorados_data']} | 📝 Temática: {s_pag['ignorados_tematica']}", console=True, arquivo=True)
                
                if s_pag['erros_itens'] > 0 or s_pag['erros_outros'] > 0:
                    total_erros = s_pag['erros_itens'] + s_pag['erros_outros']
                    erros_str = ', '.join(set(erros_detalhados[:3]))
                    log(f"      ⚠️ Erros: {total_erros} ({erros_str}{'...' if len(erros_detalhados) > 3 else ''})", console=True, arquivo=True)
                
                for k in stats_geral: stats_geral[k] += s_pag.get(k, 0)
                
                salvar_checkpoint(dia, pag + 1)
                
                if pag >= tot_pag:
                    log(f"   ✅ Fim do dia {dia_fmt}: {stats_geral['capturados']} total acumulado", console=True, arquivo=True)
                    salvar_checkpoint((dia_obj + timedelta(days=1)).strftime('%Y%m%d'), 1)
                    break
                
                pag += 1
                
            except Exception as e:
                log(f"   ❌ EXCEÇÃO na página {pag}: {str(e)[:100]}", console=True, arquivo=True)
                break
    
    log("", console=True, arquivo=True)
    log("=" * 60, console=True, arquivo=True)
    log("📊 RESUMO FINAL DA CAPTURA", console=True, arquivo=True)
    log("=" * 60, console=True, arquivo=True)
    log(f"   🎯 Capturados: {stats_geral['capturados']} licitações ({stats_geral['itens']} itens)", console=True, arquivo=True)
    log(f"   🔒 Vetados: {stats_geral['vetados']}", console=True, arquivo=True)
    log(f"   🚫 Ignorados: {stats_geral['ignorados']}", console=True, arquivo=True)
    log(f"   ⚠️ Erros: {stats_geral['erros']}", console=True, arquivo=True)
    log(f"   💾 Total no banco: {len(banco)} licitações", console=True, arquivo=True)
    log("=" * 60, console=True, arquivo=True)

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): 
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
        
        session = criar_sessao()
        banco = {}
        
        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    for x in json.load(f): 
                        banco[f"{x.get('id', '')[:14]}_{x.get('edit', '')}"] = x
            except Exception as e: 
                print(f"Erro ao carregar banco: {e}")

        buscar_periodo(session, banco, dt_start, dt_end)
        
        print("\n💾 Salvando...")
        with gzip.open(ARQ_TEMP, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
        
        if os.path.exists(ARQ_TEMP):
            os.replace(ARQ_TEMP, ARQDADOS)
            if os.path.exists(ARQ_CHECKPOINT): 
                os.remove(ARQ_CHECKPOINT)
            print("✅ Concluído!")

    finally:
        if os.path.exists(ARQ_LOCK): 
            os.remove(ARQ_LOCK)
