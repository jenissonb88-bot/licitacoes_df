import json, gzip, os, unicodedata, csv
from datetime import datetime
import concurrent.futures

ARQDADOS, ARQLIMPO, ARQ_CATALOGO = 'dadosoportunidades.json.gz', 'pregacoes_pharma_limpos.json.gz', 'Exportar Dados.csv'
DATA_CORTE_2026 = datetime(2026, 1, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

# CORRIGIDO: O termo genérico "MANUTENCAO" foi substituído por termos específicos
VETOS_IMEDIATOS = [
    "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO", 
    "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "MANUTENCAO PREVENTIVA", "MANUTENCAO CORRETIVA",
    "UNIFORME", "TEXTIL", "REFORMA", "GASES MEDICINAIS", 
    "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA", "IMPRESSAO", "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO", "CORRETIVA", "GERADOR"]]
VETOS_ADM = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "CREDENCIAMENTO", "LEILAO", "ALIENACAO",
]

TERMOS_NE_MMH_NUTRI = [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO", "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE", 
    "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS", "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL", "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO", "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO", "TOUCA DESCARTAVEL", 
    "TUBO ASPIRACAO", "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL", "FORMULA ESPECIA" 
]

TERMOS_SALVAMENTO = [
    "MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO", "ACETILCISTEINA", "ACETILSALICILICO", "ACICLOVIR", "ADENOSINA", "ADRENALINA", "ALBENDAZOL", "ALENDRONATO", 
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
    "TENOXICAN", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIOPENTAL", "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL", "TRAVOPROSTA", "TRIMETOPRIMA", "TROMETAMOL", "TROPICAMIDA", "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASELINA"
]

CONTEXTO_SAUDE = ["HOSPITALAR", "DIETA", "MEDICAMENTO", "SAUDE", "CLINICA", "PACIENTE"]

# Cache de normalização para evitar retrabalho no processador
CACHE_NORM = {}
def normalize(t): 
    if not t: return ""
    if t not in CACHE_NORM:
        CACHE_NORM[t] = ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')
    return CACHE_NORM[t]

def inferir_beneficio(desc_norm, benef_atual):
    if benef_atual in [1, 2, 3]: return benef_atual
    if any(x in desc_norm for x in ["EXCLUSIVA ME", "EXCLUSIVO ME", "COTA EXCLUSIVA", "SOMENTE ME", "EXCLUSIVIDADE ME", "ME/EPP"]): return 1
    if any(x in desc_norm for x in ["COTA RESERVADA", "RESERVADA ME", "RESERVADA PARA ME"]): return 3
    return benef_atual

CATALOGO = set()
if os.path.exists(ARQ_CATALOGO):
    try:
        with open(ARQ_CATALOGO, 'r', encoding='latin-1') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader, None)
            for row in reader:
                if row: CATALOGO.add(normalize(row[0]))
    except: pass

def analisar_pertinencia(obj_norm, uf, itens_brutos):
    if uf in ESTADOS_BLOQUEADOS: return False
    for veto in VETOS_IMEDIATOS:
        if veto in obj_norm: return False
    if "MEDICINA" in obj_norm or "MEDICO" in obj_norm:
        if "GASES" in obj_norm and not any(s in obj_norm for s in TERMOS_SALVAMENTO): return False
    if "FORMULA" in obj_norm or "LEITE" in obj_norm:
        if not any(ctx in obj_norm for ctx in CONTEXTO_SAUDE): return False
    if uf in NE_ESTADOS and any(t in obj_norm for t in TERMOS_NE_MMH_NUTRI): return True
    if any(t in obj_norm for t in TERMOS_SALVAMENTO): return True
    if CATALOGO:
        for it in itens_brutos:
            if any(prod in normalize(it.get('d', '')) for prod in CATALOGO): return True
    return False

# --- FUNÇÃO ISOLADA PARA PROCESSAMENTO PARALELO ---
def processar_licitacao_limpeza(p):
    try:
        dt_str = p.get('dt_enc', '')
        if not dt_str: return None
        dt_obj = datetime.fromisoformat(dt_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_obj < DATA_CORTE_2026: return None
    except: return None

    obj_norm = normalize(p.get('obj', ''))
    uf = p.get('uf', '').upper()
    
    itens_brutos = p.get('itens', [])
    if not analisar_pertinencia(obj_norm, uf, itens_brutos): return None

    itens_fmt = []
    for it in itens_brutos:
        desc_bruta = it.get('d', '')
        desc_norm = normalize(desc_bruta)
        itens_fmt.append({
            'n': it.get('n'), 'desc': desc_bruta, 'qtd': it.get('q', 0), 'un': it.get('u', ''), 
            'valUnit': it.get('v_est', 0), 'valHomologado': it.get('res_val', 0), 
            'fornecedor': it.get('res_forn'), 'situacao': it.get('sit', 'EM ANDAMENTO'), 
            'benef': inferir_beneficio(desc_norm, int(it.get('benef', 4)))
        })
    
    if not itens_fmt: return None

    todos_exclusivos = all(i['benef'] in [1, 2, 3] for i in itens_fmt)
    algum_exclusivo = any(i['benef'] in [1, 2, 3] for i in itens_fmt)
    tipo_lic = "EXCLUSIVO" if todos_exclusivos else ("PARCIAL" if algum_exclusivo else "AMPLO")

    card = {
        'id': p.get('id'), 'uf': uf, 'uasg': p.get('uasg'), 'orgao': p.get('org'), 
        'unidade': p.get('unid_nome'), 'edital': p.get('edit'), 'cidade': p.get('cid'), 
        'objeto': p.get('obj'), 'valor_estimado': p.get('val_tot', 0), 'data_enc': dt_str,
        'link': p.get('link'), 'tipo_licitacao': tipo_lic, 'itens': itens_fmt,
        'sit_global': p.get('sit_global', 'DIVULGADA') 
    }
    
    chave = f"{p.get('id', '')[:14]}_{p.get('edit', '')}"
    return (chave, card, dt_obj, len(itens_fmt))

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == '__main__':
    if not os.path.exists(ARQDADOS): exit()

    print("Descompactando base bruta...")
    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
        banco_bruto = json.load(f)

    banco_deduplicado = {}

    print(f"Processando {len(banco_bruto)} licitações com processamento paralelo...")
    
    # Utilizando ThreadPool para acelerar o processamento (15 trabalhadores igual o app.py)
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        resultados = executor.map(processar_licitacao_limpeza, banco_bruto)

    # Coleta e desempate (Deduplicação)
    for res in resultados:
        if res is None: continue
        chave, card, dt_novo, qtd_itens_novo = res
        
        if chave not in banco_deduplicado:
            banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
        else:
            qtd_itens_antigo = banco_deduplicado[chave]['qtd']
            if qtd_itens_novo > qtd_itens_antigo:
                banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
            elif qtd_itens_novo == qtd_itens_antigo:
                if dt_novo > banco_deduplicado[chave]['dt']:
                    banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}

    print("Ordenando e salvando arquivo limpo...")
    web_data = [v['card'] for v in sorted(banco_deduplicado.values(), key=lambda x: x['dt'], reverse=True)]
    
    with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: 
        json.dump(web_data, f, ensure_ascii=False)
    
    print("Limpeza concluída com sucesso!")
