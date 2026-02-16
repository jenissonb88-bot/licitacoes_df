import json, gzip, os, unicodedata, csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'
DATA_CORTE_FIXA = datetime(2025, 12, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# LEI DO VETO
VETOS = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "BUFFET", "PRESTACAO", "TERCEIRIZACAO", "EXAME LABORATORI"]]

catalogo = set()
if os.path.exists(ARQCSV):
    for enc in ['latin-1', 'utf-8', 'cp1252']:
        try:
            with open(ARQCSV, 'r', encoding=enc) as f:
                leitor = csv.reader(f); next(leitor, None)
                for l in leitor:
                    if l:
                        for i in [0,1,5]:
                            if len(l) > i:
                                t = normalize(l[i])
                                if len(t) > 4: catalogo.add(t)
            break
        except: continue

if not os.path.exists(ARQDADOS): exit()
with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: banco = json.load(f)

banco_limpo = []
for p in banco:
    uf = p.get('uf', '').upper()
    obj = normalize(p.get('obj', ''))
    
    # 1. Trava Data
    try:
        dt = datetime.fromisoformat(p.get('dt_enc', '').replace('Z', '+00:00')).replace(tzinfo=None)
        if dt < DATA_CORTE_FIXA: continue
    except: continue

    # 2. Veto Suprema
    if any(v in obj for v in VETOS) and not "DIETA" in obj: continue

    # 3. Filtro Geográfico de Especialista
    is_pharma = any(t in obj for t in ["MEDICAMENT", "FARMACO", "REMEDIO", "SORO", "AMPOAL"])
    is_material = any(t in obj for t in ["MMH", "INSUMO", "MATERIAL MEDIC", "EQUIPO", "SONDA", "LUVA"])
    
    aprovado = False
    if uf in NE_ESTADOS:
        if is_pharma or is_material or "DIETA" in obj: aprovado = True
    elif is_pharma:
        aprovado = True
    
    # 4. Cruzamento com CSV (Resgate)
    if not aprovado:
        for it in p.get('itens', []):
            if any(term in normalize(it.get('d', '')) for term in catalogo):
                aprovado = True; break
    
    if aprovado:
        p['itens'] = [i for i in p.get('itens', []) if not any(v in normalize(i.get('d', '')) for v in ["BUFFET", "LIMPEZA", "COFFEE"])]
        banco_limpo.append(p)

web_data = []
for p in banco_limpo:
    c_ex = 0; itens_fmt = []
    for it in p.get('itens', []):
        is_ex = int(it.get('benef') or 4) in [1, 2, 3]
        if is_ex: c_ex += 1
        itens_fmt.append({
            'n': it.get('n'), 'desc': it.get('d'), 'qtd': it.get('q', 0), 'un': it.get('u', ''),
            'valUnit': it.get('v_est', 0), 'valHomologado': it.get('res_val', 0),
            'fornecedor': it.get('res_forn'), 'situacao': it.get('sit', 'ABERTO'), 'me_epp': is_ex
        })
    web_data.append({
        'id': p.get('id'), 'uf': p.get('uf'), 'uasg': p.get('uasg'), 'orgao': p.get('org'),
        'unidade': p.get('unid_nome'), 'edital': p.get('edit'), 'cidade': p.get('cid'),
        'objeto': p.get('obj'), 'valor_estimado': p.get('val_tot', 0), 'data_enc': p.get('dt_enc'),
        'link': p.get('link'), 'tipo_licitacao': "EXCLUSIVO" if c_ex==len(itens_fmt) else "AMPLO",
        'itens': itens_fmt
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: json.dump(web_data, f, ensure_ascii=False)
with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f: json.dump(banco_limpo, f, ensure_ascii=False)
print(f"♻️ Limpeza Geográfica concluída: {len(banco_limpo)} registros.")
