import json, gzip, os, unicodedata, csv
from datetime import datetime

ARQDADOS, ARQLIMPO, ARQ_CATALOGO = 'dadosoportunidades.json.gz', 'pregacoes_pharma_limpos.json.gz', 'Exportar Dados.csv'
DATA_CORTE_2026 = datetime(2026, 1, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

VETOS_IMEDIATOS = [
    "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO", 
    "MANUTENCAO", "UNIFORME", "TEXTIL", "REFORMA", "GASES MEDICINAIS", 
    "OXIGENIO", "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA", "IMPRESSAO"
]
TERMOS_NE_MMH_NUTRI = [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", 
    "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS", "MASCARA", 
    "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO", "DIETA", "NUTRICAO CLINICA"
]
TERMOS_SALVAMENTO = ["MEDICAMENT", "FARMAC", "REMEDIO", "FARMACO", "INJETAVEL"]
CONTEXTO_SAUDE = ["HOSPITALAR", "DIETA", "MEDICAMENTO", "SAUDE", "CLINICA", "PACIENTE"]

def normalize(t): return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn') if t else ""

def inferir_beneficio(desc, benef_atual):
    if benef_atual in [1, 2, 3]: return benef_atual
    d = normalize(desc)
    if any(x in d for x in ["EXCLUSIVA ME", "EXCLUSIVO ME", "COTA EXCLUSIVA", "SOMENTE ME", "EXCLUSIVIDADE ME", "ME/EPP"]): return 1
    if any(x in d for x in ["COTA RESERVADA", "RESERVADA ME", "RESERVADA PARA ME"]): return 3
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

if not os.path.exists(ARQDADOS): exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: banco_bruto = json.load(f)

banco_deduplicado = {}
for p in banco_bruto:
    try:
        dt = datetime.fromisoformat(p.get('dt_enc', '').replace('Z', '+00:00')).replace(tzinfo=None)
        if dt < DATA_CORTE_2026: continue
    except: continue

    obj_norm = normalize(p.get('obj', ''))
    uf = p.get('uf', '').upper()
    
    if analisar_pertinencia(obj_norm, uf, p.get('itens', [])):
        itens_fmt = []
        for it in p.get('itens', []):
            itens_fmt.append({
                'n': it.get('n'), 'desc': it.get('d'), 'qtd': it.get('q', 0), 'un': it.get('u', ''), 
                'valUnit': it.get('v_est', 0), 'valHomologado': it.get('res_val', 0), 
                'fornecedor': it.get('res_forn'), 'situacao': it.get('sit', 'EM ANDAMENTO'), 
                'benef': inferir_beneficio(it.get('d', ''), int(it.get('benef', 4)))
            })
        
        if not itens_fmt: continue

        todos_exclusivos = all(i['benef'] in [1, 2, 3] for i in itens_fmt)
        algum_exclusivo = any(i['benef'] in [1, 2, 3] for i in itens_fmt)
        tipo_lic = "EXCLUSIVO" if todos_exclusivos else ("PARCIAL" if algum_exclusivo else "AMPLO")

        card = {
            'id': p.get('id'), 'uf': uf, 'uasg': p.get('uasg'), 'orgao': p.get('org'), 
            'unidade': p.get('unid_nome'), 'edital': p.get('edit'), 'cidade': p.get('cid'), 
            'objeto': p.get('obj'), 'valor_estimado': p.get('val_tot', 0), 'data_enc': p.get('dt_enc'),
            'link': p.get('link'), 'tipo_licitacao': tipo_lic, 'itens': itens_fmt,
            'sit_global': p.get('sit_global', 'DIVULGADA') 
        }
        
        chave = f"{p.get('id', '')[:14]}_{p.get('edit', '')}"
        if chave not in banco_deduplicado or datetime.fromisoformat(card['data_enc'].replace('Z','+00:00')).replace(tzinfo=None) > datetime.fromisoformat(banco_deduplicado[chave]['data_enc'].replace('Z','+00:00')).replace(tzinfo=None):
            banco_deduplicado[chave] = card

web_data = sorted(list(banco_deduplicado.values()), key=lambda x: x['data_enc'], reverse=True)
with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: json.dump(web_data, f, ensure_ascii=False)
