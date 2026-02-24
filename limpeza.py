import json, gzip, os, unicodedata, csv, re
from datetime import datetime
import concurrent.futures

ARQDADOS, ARQLIMPO, ARQ_CATALOGO = 'dadosoportunidades.json.gz', 'pregacoes_pharma_limpos.json.gz', 'Exportar Dados.csv'
ARQ_REGRAS_CSV = 'regras_materiais.csv'
DATA_CORTE_2026 = datetime(2026, 1, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

VETOS_IMEDIATOS = [
    r"PRESTACAO DE SERVICO", r"SERVICO ESPECIALIZADO", r"LOCACAO", r"INSTALACAO", 
    r"MANUTENCAO PREDIAL", r"MANUTENCAO DE EQUIPAMENTO(S)?", r"MANUTENCAO PREVENTIVA", r"MANUTENCAO CORRETIVA",
    r"UNIFORME(S)?", r"TEXTI(L|IS)", r"REFORMA", r"GASE(S)? MEDICINAI(S)?", 
    r"OXIGENIO", r"CILINDRO", r"LIMPEZA PREDIAL", r"LAVANDERIA", r"IMPRESSAO"
]

TERMOS_NE_MMH_NUTRI = [
    r"INSUMO(S)? HOSPITALAR(ES)?", r"MMH", r"SERINGA(S)?", r"SONDA(S)?", r"CATETER(ES)?", 
    r"NUTRICAO ENTERAL", r"FORMULA INFANTIL", r"SUPLEMENTO", r"DIETA", r"NUTRICAO CLINICA",
    r"MEDIC(O|A)?(S)?[\s\-]*HOSPITALAR(ES)?", r"LABORATORI(O|AL|AIS)", r"PRODUTO(S)? PARA SAUDE", 
    r"ANTISSEPTIC(O|A)?(S)?", r"CLOREXIDINA", r"PVPI",
    r"CURATIVO(S)?", r"COBERTURA(S)? (ESPECIAL|ESPECIAIS|PARA LESO(AO|ES)|ESTERIL)"
]

TERMOS_SALVAMENTO = [
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

CONTEXTO_SAUDE = [r"HOSPITALAR", r"DIETA", r"MEDICAMENTO", r"SAUDE", r"CLINICA", r"PACIENTE"]

CACHE_NORM = {}
def normalize(t): 
    if not t: return ""
    if t not in CACHE_NORM:
        CACHE_NORM[t] = ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')
    return CACHE_NORM[t]

def busca_flexivel(lista_regex, texto):
    for padrao in lista_regex:
        if re.search(rf"\b{padrao}\b", texto):
            return True
    return False

# --- CARREGAMENTO DAS REGRAS DO CSV ---
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
    except Exception as e: pass

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

def inferir_beneficio(desc_norm, benef_atual):
    if benef_atual in [1, 2, 3]: return benef_atual
    if busca_flexivel([r"EXCLUSIVA ME", r"EXCLUSIVO ME", r"COTA EXCLUSIVA", r"SOMENTE ME", r"EXCLUSIVIDADE ME", r"ME/EPP"], desc_norm): return 1
    if busca_flexivel([r"COTA RESERVADA", r"RESERVADA ME", r"RESERVADA PARA ME"], desc_norm): return 3
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
    if busca_flexivel(VETOS_IMEDIATOS, obj_norm): return False
    
    if busca_flexivel([r"MEDICINA", r"MEDICO"], obj_norm):
        if busca_flexivel([r"GASE(S)?"], obj_norm) and not busca_flexivel(TERMOS_SALVAMENTO, obj_norm): return False
        
    if busca_flexivel([r"FORMULA", r"LEITE"], obj_norm):
        if not busca_flexivel(CONTEXTO_SAUDE, obj_norm): return False
        
    if uf in NE_ESTADOS and busca_flexivel(TERMOS_NE_MMH_NUTRI, obj_norm): return True
    if busca_flexivel(TERMOS_SALVAMENTO, obj_norm): return True
    if avalia_regras_contextuais(obj_norm): return True
    
    if CATALOGO:
        for it in itens_brutos:
            if any(prod in normalize(it.get('d', '')) for prod in CATALOGO): return True
    return False

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

if __name__ == '__main__':
    if not os.path.exists(ARQDADOS): exit()

    print("Descompactando base bruta...")
    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
        banco_bruto = json.load(f)
        
    # --- VARIÁVEIS DE ESTATÍSTICA ---
    total_editais_brutos = len(banco_bruto)
    total_itens_brutos = sum(len(p.get('itens', [])) for p in banco_bruto)
    editais_filtrados = 0

    banco_deduplicado = {}
    print(f"Processando {total_editais_brutos} licitações com Regex e CSV Contextual...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        resultados = executor.map(processar_licitacao_limpeza, banco_bruto)

    for res in resultados:
        if res is None: continue
        editais_filtrados += 1
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
        
    # --- CÁLCULO FINAL DE ESTATÍSTICAS ---
    total_editais_limpos = len(banco_deduplicado)
    total_itens_limpos = sum(v['qtd'] for v in banco_deduplicado.values())
    
    removidos_pelos_filtros = total_editais_brutos - editais_filtrados
    removidos_por_duplicidade = editais_filtrados - total_editais_limpos
    itens_descartados = total_itens_brutos - total_itens_limpos

    print("\n" + "="*50)
    print("📊 RESUMO GERAL DA OPERAÇÃO DE LIMPEZA")
    print("="*50)
    print(f"📥 EDITAIS BRUTOS AVALIADOS: {total_editais_brutos}")
    print(f"📦 ITENS BRUTOS AVALIADOS:   {total_itens_brutos}")
    print("-" * 50)
    print(f"🚫 EDITAIS VETADOS:          {removidos_pelos_filtros} (Filtros, Datas ou UF)")
    print(f"✂️ EDITAIS MESCLADOS:        {removidos_por_duplicidade} (Deduplicação de Versões)")
    print(f"🗑️ ITENS DESCARTADOS:        {itens_descartados}")
    print("-" * 50)
    print(f"✅ EDITAIS FINAIS:           {total_editais_limpos}")
    print(f"🎯 ITENS FINAIS:             {total_itens_limpos}")
    print("="*50)
