import json, gzip, os, unicodedata, csv

ARQDADOS = 'dadosoportunidades.json.gz'          
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'     

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def auditar_beneficio(desc, cod_api):
    """Verifica se há divergência entre o código do governo e o texto do item"""
    d = normalize(desc)
    # Se a API diz que é Amplo (4,5) ou falhou (0/None)
    if cod_api in [4, 5, 0, None]:
        if any(x in d for x in ["EXCLUSIVO ME", "EXCLUSIVA ME", "ME EPP", "PARA ME", "SOMENTE ME", "EXCLUSIVIDADE ME"]):
            return 1, True # Código corrigido, Houve Divergência
        if any(x in d for x in ["COTA RESERVADA", "RESERVADA ME"]):
            return 3, True
    return cod_api, False

if not os.path.exists(ARQDADOS): exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    dados_brutos = json.load(f)

web_data = []

for p in dados_brutos:
    itens_fmt = []
    c_exclusivos = 0
    
    for it in p.get('itens', []):
        benef_api = int(it.get('benef') or 4)
        
        # AUDITORIA SEMÂNTICA
        benef_corrigido, divergente = auditar_beneficio(it.get('d', ''), benef_api)
        is_me_epp = benef_corrigido in [1, 2, 3]
        
        if is_me_epp: c_exclusivos += 1
        
        itens_fmt.append({
            'n': it.get('n'), 'desc': it.get('d'), 'qtd': it.get('q'), 'un': it.get('u'),
            'valUnit': it.get('v_est'), 'valHomologado': it.get('res_val'), 'fornecedor': it.get('res_forn'),
            'situacao': it.get('sit'), 
            'benef': benef_corrigido, 'me_epp': is_me_epp, 'divergente': divergente # MARCA D'ÁGUA
        })
        
    if not itens_fmt: continue

    tipo_lic = "EXCLUSIVO" if c_exclusivos == len(itens_fmt) else ("PARCIAL" if c_exclusivos > 0 else "AMPLO")

    web_data.append({
        'id': p.get('id'), 'data_enc': p.get('dt_enc'), 'uf': p.get('uf'), 'uasg': p.get('uasg'),
        'orgao': p.get('org'), 'unidade': p.get('unid_nome'), 'cidade': p.get('cid'), 
        'objeto': p.get('obj'), 'edital': p.get('edit'), 'link': p.get('link'),
        'valor_estimado': p.get('val_tot'), 'tipo_licitacao': tipo_lic, 'itens': itens_fmt
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(web_data, f, ensure_ascii=False)
