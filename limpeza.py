import json, gzip, os, unicodedata, csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
DATA_CORTE_FIXA = datetime(2025, 12, 1)

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

if not os.path.exists(ARQDADOS): 
    print("❌ Arquivo de dados não encontrado.")
    exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: banco = json.load(f)
inicial = len(banco)

web_data = []
for p in banco:
    # Filtro de Data Final
    try:
        dt = datetime.fromisoformat(p.get('dt_enc', '').replace('Z', '+00:00')).replace(tzinfo=None)
        if dt < DATA_CORTE_FIXA: continue
    except: continue

    # Como a filtragem pesada já foi feita no app.py, aqui apenas formatamos
    # e removemos eventuais sobras vazias.
    c_ex = 0; itens_fmt = []
    itens_originais = p.get('itens', [])
    
    if not itens_originais: continue

    for it in itens_originais:
        is_ex = int(it.get('benef') or 4) in [1, 2, 3]
        if is_ex: c_ex += 1
        
        itens_fmt.append({
            'n': it.get('n'), 
            'desc': it.get('d'), 
            'qtd': it.get('q', 0), 
            'un': it.get('u', ''),
            'valUnit': it.get('v_est', 0), 
            'valHomologado': it.get('res_val', 0),
            'fornecedor': it.get('res_forn'), 
            'situacao': it.get('sit', 'ABERTO'), 
            'me_epp': is_ex
        })

    web_data.append({
        'id': p.get('id'), 
        'uf': p.get('uf'), 
        'uasg': p.get('uasg'), 
        'orgao': p.get('org'),
        'unidade': p.get('unid_nome'), 
        'edital': p.get('edit'), 
        'cidade': p.get('cid'),
        'objeto': p.get('obj'), 
        'valor_estimado': p.get('val_tot', 0), 
        'data_enc': p.get('dt_enc'),
        'link': p.get('link'), 
        'tipo_licitacao': "EXCLUSIVO" if c_ex==len(itens_fmt) and len(itens_fmt)>0 else "AMPLO",
        'itens': itens_fmt
    })

# Ordenação por data (mais novos primeiro)
web_data.sort(key=lambda x: x['data_enc'], reverse=True)

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: json.dump(web_data, f, ensure_ascii=False)

print(f"♻️ Limpeza e Formatação concluída!")
print(f"   📉 Registros Brutos: {inicial}")
print(f"   📈 Registros Web:    {len(web_data)}")
