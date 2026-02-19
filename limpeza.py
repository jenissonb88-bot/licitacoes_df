import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'          
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'     
ARQ_CATALOGO = 'Exportar Dados.csv'              
DATA_CORTE_2026 = datetime(2025, 12, 1)           

ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def inferir_beneficio(desc, benef_atual):
    """PLANO B: Corrige a omissão do PNCP lendo a descrição do item"""
    if benef_atual in [1, 2, 3]: return benef_atual
    d = normalize(desc)
    if any(x in d for x in ["EXCLUSIVA ME", "EXCLUSIVO ME", "COTA EXCLUSIVA", "SOMENTE ME", "EXCLUSIVIDADE ME", "ME/EPP"]):
        return 1
    if any(x in d for x in ["COTA RESERVADA", "RESERVADA ME", "RESERVADA PARA ME"]):
        return 3
    return benef_atual

CATALOGO = set()
try:
    with open(ARQ_CATALOGO, 'r', encoding='latin-1') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader, None)
        for row in reader:
            if len(row) > 2:
                for termo in [row[0], row[2]]:
                    n = normalize(termo)
                    if len(n) > 3: CATALOGO.add(n)
except Exception as e: print(f"Aviso catálogo: {e}")

if not os.path.exists(ARQDADOS): exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    dados_brutos = json.load(f)

web_data = []

for p in dados_brutos:
    uf = p.get('uf', '')
    if uf in ESTADOS_BLOQUEADOS: continue

    itens_brutos = p.get('itens', [])
    itens_fmt = []
    
    for it in itens_brutos:
        desc = it.get('d', '')
        
        # Correção do ME/EPP
        benef_bruto = int(it.get('benef') or 4)
        benef_corrigido = inferir_beneficio(desc, benef_bruto)
        
        itens_fmt.append({
            'n': it.get('n'), 
            'desc': desc, 
            'qtd': it.get('q', 0),
            'un': it.get('u', ''), 
            'valUnit': it.get('v_est', 0),
            'valHomologado': it.get('res_val', 0), 
            'fornecedor': it.get('res_forn'),
            'situacao': it.get('sit', 'EM ANDAMENTO'), 
            'benef': benef_corrigido
        })
        
    if not itens_fmt: continue

    # Calcula Benefício do Edital Inteiro pro HTML
    todos_exclusivos = all(i['benef'] in [1, 2, 3] for i in itens_fmt)
    algum_exclusivo = any(i['benef'] in [1, 2, 3] for i in itens_fmt)
    tipo_lic = "EXCLUSIVO" if todos_exclusivos else ("PARCIAL" if algum_exclusivo else "AMPLO")

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
        'valor_estimado': p.get('val_tot', 0), 
        'tipo_licitacao': tipo_lic,
        'itens': itens_fmt
    })

print(f"🧹 Limpeza concluída: {len(dados_brutos)} brutos -> {len(web_data)} limpos.")

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(web_data, f, ensure_ascii=False)
