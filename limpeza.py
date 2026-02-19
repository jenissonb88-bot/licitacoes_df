import json
import gzip
import os
import unicodedata
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'          
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'     
DATA_CORTE_2026 = datetime(2026, 1, 1)           

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def auditar_beneficio(desc, cod_api):
    """
    Verifica divergência: Se a API diz que é Amplo (4, 5 ou vazio), 
    mas o texto exige exclusividade.
    """
    d = normalize(desc)
    if cod_api in [4, 5, 0, None]:
        if any(x in d for x in ["EXCLUSIVO ME", "EXCLUSIVA ME", "ME EPP", "PARA ME", "SOMENTE ME", "EXCLUSIVIDADE ME"]):
            return 1, True # Retorna código 1 (Exclusivo) e flag de Divergência = True
        if any(x in d for x in ["COTA RESERVADA", "RESERVADA ME"]):
            return 3, True # Retorna código 3 (Cota) e flag de Divergência = True
    return cod_api, False # Sem divergência

if not os.path.exists(ARQDADOS): exit()

print("🧹 Iniciando processo de Limpeza e Lapidação...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    dados_brutos = json.load(f)

web_data = []

for p in dados_brutos:
    itens_fmt = []
    c_exclusivos = 0
    
    for it in p.get('itens', []):
        benef_api = int(it.get('benef') or 4)
        
        # AUDITORIA SEMÂNTICA DE ME/EPP
        benef_corrigido, divergente = auditar_beneficio(it.get('d', ''), benef_api)
        is_me_epp = benef_corrigido in [1, 2, 3]
        
        if is_me_epp: c_exclusivos += 1
        
        # Mapeamento Exato para o que o index.html espera
        itens_fmt.append({
            'n': it.get('n'), 
            'desc': it.get('d'), 
            'qtd': it.get('q'), 
            'un': it.get('u'),
            'valUnit': it.get('v_est'), 
            'valHomologado': it.get('res_val'), 
            'fornecedor': it.get('res_forn'),
            'situacao': it.get('sit', 'EM ANDAMENTO'), 
            'benef': benef_corrigido, 
            'me_epp': is_me_epp, 
            'divergente': divergente
        })
        
    if not itens_fmt: continue

    # Define classificação da Licitação para a Tag visual
    tipo_lic = "EXCLUSIVO" if c_exclusivos == len(itens_fmt) else ("PARCIAL" if c_exclusivos > 0 else "AMPLO")

    web_data.append({
        'id': p.get('id'), 
        'data_enc': p.get('dt_enc'), 
        'uf': p.get('uf'), 
        'uasg': p.get('uasg'),
        'orgao': p.get('org'), 
        'unidade': p.get('unid_nome'), 
        'cidade': p.get('cid'), 
        'objeto': p.get('obj'), 
        'edital': p.get('edit'), 
        'link': p.get('link'),
        'valor_estimado': p.get('val_tot'), 
        'tipo_licitacao': tipo_lic, 
        'itens': itens_fmt
    })

# Ordenação cronológica (os mais recentes primeiro)
web_data.sort(key=lambda x: x['data_enc'], reverse=True)

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(web_data, f, ensure_ascii=False)

print(f"✅ Limpeza concluída: {len(dados_brutos)} capturados -> {len(web_data)} lapidados.")
