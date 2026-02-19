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

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = []

# Termos que salvam tudo no NE 
TERMOS_UNIVERSAIS_NE = ["FRALDA", "ABSORVENTE", "ALCOOL 70", "ALCOOL ETILICO", "ALCOOL GEL", "ALCOOL EM GEL"]

# Vetos que matam o edital 
VETOS_IMEDIATOS = [
    "PRESTACAO DE SERVICO", "SERVICO DE ENGENHARIA", "LOCACAO", "INSTALACAO", 
    "MANUTENCAO", "UNIFORME", "TEXTIL", "REFORMA", "LIMPEZA PREDIAL", 
    "LAVANDERIA", "IMPRESSAO", "CONSULTORIA", "TREINAMENTO", "VIGILANCIA",
    "PORTARIA", "RECEPCAO", "EVENTOS", "BUFFET", "SONDAGEM", "GEOLOGIA"
]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

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

    dt_enc_str = p.get('dt_enc', '')
    if dt_enc_str:
        try:
            dt = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt < DATA_CORTE_2026: continue
        except: pass

    obj = normalize(p.get('obj', ''))
    
    # Check Veto
    vetado = False
    for v in VETOS_IMEDIATOS:
        if v in obj:
            if v == "SERVICO" and "FORNECIMENTO" in obj: continue
            vetado = True; break
    if vetado: continue

    # Check Interesse
    tem_interesse = any(c in obj for c in CATALOGO)
    if not tem_interesse and uf in NE_ESTADOS:
        tem_interesse = any(tu in obj for tu in TERMOS_UNIVERSAIS_NE)
    if not tem_interesse:
        if any(x in obj for x in ["MEDICAMENTO", "MATERIAL MED", "INSUMO HOSP"]): tem_interesse = True
    
    if not tem_interesse: continue

    # Itens Processados
    itens_brutos = p.get('itens', [])
    itens_fmt = []
    
    for it in itens_brutos:
        desc = normalize(it.get('d', ''))
        
        # Formato limpo e renomeado
        itens_fmt.append({
            'n': it.get('n'), 
            'desc': it.get('d'), 
            'qtd': it.get('q', 0),
            'un': it.get('u', ''), 
            'valUnit': it.get('v_est', 0),
            'valHomologado': it.get('res_val', 0), 
            'fornecedor': it.get('res_forn'),
            'situacao': it.get('sit', 'EM ANDAMENTO'), 
            'benef': int(it.get('benef') or 4) 
        })
        
    if not itens_fmt: continue

    # Calcula Benefício do Edital Inteiro
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
