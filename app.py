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

# LEI DO VETO SUPREMA
VETOS = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "BUFFET", "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "EXAME LABORATORI", "OBRAS", "PNEU", "VEICULO"]]

# CARREGAR CATÁLOGO DROGAFONTE
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

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: banco = json.load(f)

banco_limpo = []
for p in banco:
    uf = p['uf'].upper()
    obj = normalize(p['obj'])
    
    # 1. Trava Cronológica
    dt = datetime.fromisoformat(p['dt_enc'].replace('Z', '+00:00')).replace(tzinfo=None)
    if dt < DATA_CORTE_FIXA: continue

    # 2. Veto Supremo
    if any(v in obj for v in VETOS) and not "DIETA" in obj: continue

    # 3. Filtro Geográfico de Especialista
    is_pharma = any(t in obj for t in ["MEDICAMENT", "FARMACO", "REMEDIO", "SORO", "AMPOAL", "COMPRIMIDO"])
    is_material = any(t in obj for t in ["MMH", "INSUMO", "MATERIAL MEDIC", "EQUIPO", "SONDA", "LUVA", "SERINGA"])
    is_dieta = any(t in obj for t in ["DIETA", "FORMULA", "NUTRICIONAL"])

    aprovado = False
    if uf in NE_ESTADOS:
        if is_pharma or is_material or is_dieta: aprovado = True
    else:
        if is_pharma: aprovado = True
    
    # Validação Final via Catálogo (Resgate de itens genéricos)
    if not aprovado:
        for it in p['itens']:
            if any(term in normalize(it['d']) for term in catalogo):
                aprovado = True; break
    
    if aprovado:
        # Pente Fino nos Itens: Remove o que sobrou de lixo dentro de um edital aprovado
        p['itens'] = [i for i in p['itens'] if not any(v in normalize(i['d']) for v in ["BUFFET", "LIMPEZA", "COPA", "OBRAS", "PNEU"])]
        banco_limpo.append(p)

# GERAÇÃO DO WEB_DATA PARA O MONITOR
web_data = []
for p in banco_limpo:
    c_ex = 0; itens_fmt = []
    for it in p['itens']:
        is_ex = int(it.get('benef') or 4) in [1, 2, 3]
        if is_ex: c_ex += 1
        itens_fmt.append({
            'n': it['n'], 'desc': it['d'], 'qtd': it['q'], 'un': it['u'],
            'valUnit': it['v_est'], 'valHomologado': it['res_val'],
            'fornecedor': it['res_forn'], 'situacao': it['sit'], 'me_epp': is_ex
        })
    web_data.append({
        'id': p['id'], 'uf': p['uf'], 'uasg': p['uasg'], 'orgao': p['org'],
        'unidade': p['unid_nome'], 'edital': p['edit'], 'cidade': p['cid'],
        'objeto': p['obj'], 'valor_estimado': p['val_tot'], 'data_enc': p['dt_enc'],
        'link': p['link'], 'tipo_licitacao': "EXCLUSIVO" if c_ex==len(itens_fmt) else ("PARCIAL" if c_ex>0 else "AMPLO"),
        'itens': itens_fmt
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: json.dump(web_data, f, ensure_ascii=False)
with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f: json.dump(banco_limpo, f, ensure_ascii=False)
print(f"♻️ Limpeza finalizada: {len(banco_limpo)} registros mantidos.")
