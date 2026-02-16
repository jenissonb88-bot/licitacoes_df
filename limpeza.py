import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- CARREGAMENTO DO CSV ---
catalogo_produtos = set()
if os.path.exists(ARQCSV):
    try:
        with open(ARQCSV, 'r', encoding='latin-1') as f:
            leitor = csv.reader(f)
            next(leitor, None) 
            for linha in leitor:
                if linha:
                    for i in [0, 1, 5]:
                        if len(linha) > i: catalogo_produtos.add(normalize(linha[i]))
    except: pass

if not os.path.exists(ARQDADOS): exit()
with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: todos = json.load(f)

limpos = []
for preg in todos:
    p_itens = preg.get('itens', [])
    obj_norm = normalize(preg.get('obj'))
    uf = preg.get('uf', '').upper()
    
    # Filtro Básico
    aprovado = any(t in obj_norm for t in ["REMEDIO", "FARMACO", "HOSPITAL", "SAUDE", "MEDICAMENT", "MMH"])
    if not aprovado:
        for it in p_itens:
            if any(p in normalize(it.get('d')) for p in catalogo_produtos):
                aprovado = True; break
    
    if not aprovado: continue

    lista_final = []
    count_me = 0
    for it in p_itens:
        bid = it.get('benef')
        is_me = int(bid or 4) in [1, 3]
        if is_me: count_me += 1
        
        lista_final.append({
            'n': it['n'], 'desc': it['d'], 'qtd': it['q'], 'un': it['u'],
            'valUnit': it['v_est'], 'me_epp': is_me, 'situacao': it['sit'],
            'fornecedor': it.get('res_forn'), 
            'valHomologado': float(it.get('res_val') or 0)
        })

    limpos.append({
        'id': preg['id'], 'uf': uf, 'cidade': preg.get('cid'), 'orgao': preg.get('org'), 
        'unidade': preg.get('unid_nome'), 'uasg': preg.get('uasg'), 'edital': preg.get('edit'),
        'valor_estimado': preg.get('val_tot'), 'data_enc': preg.get('dt_enc'),
        'objeto': preg.get('obj'), 'link': preg.get('link'),
        'tipo_licitacao': "EXCLUSIVO" if count_me == len(lista_final) else ("PARCIAL" if count_me > 0 else "AMPLO"),
        'itens': lista_final
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)
