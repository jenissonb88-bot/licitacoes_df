import json
import gzip
import os
import unicodedata
from datetime import datetime

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'

# 1. DATA DE CORTE RIGOROSA (Ano Novo, Vida Nova)
DATA_CORTE_2026 = datetime(2026, 1, 1)

# 2. BLOQUEIO GEOGRÁFICO (Estados onde não há atuação)
# Sul (RS, SC, PR) + Extremos Norte (AP, AC, RO, RR)
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

if not os.path.exists(ARQDADOS): 
    print("❌ Arquivo de dados não encontrado.")
    exit()

print(f"🔄 Iniciando Auditoria e Limpeza (Corte: {DATA_CORTE_2026.strftime('%d/%m/%Y')})...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
    banco_bruto = json.load(f)

inicial = len(banco_bruto)
banco_filtrado_final = [] # Substituirá o arquivo original (sem lixo)
web_data = [] # Irá para o site (formatado)

for p in banco_bruto:
    # --- FASE 1: TRIAGEM ELIMINATÓRIA ---
    
    # A. Validação de Data
    try:
        data_str = p.get('dt_enc', '').replace('Z', '+00:00')
        dt = datetime.fromisoformat(data_str).replace(tzinfo=None)
        if dt < DATA_CORTE_2026: continue # Lixo antigo
    except: continue # Data inválida

    # B. Validação Geográfica (O Muro Logístico)
    uf = p.get('uf', '').upper()
    if uf in ESTADOS_BLOQUEADOS:
        continue # Fora da área de atuação

    # --- FASE 2: ANÁLISE DE CONTEÚDO ---
    
    itens_originais = p.get('itens', [])
    if not itens_originais: continue # Edital vazio

    c_ex = 0
    itens_fmt = []
    
    # Processa itens
    for it in itens_originais:
        # Verifica se é ME/EPP (Benefício)
        is_ex = int(it.get('benef') or 4) in [1, 2, 3]
        if is_ex: c_ex += 1
        
        # Filtro extra de segurança (caso o app.py tenha deixado passar algo muito estranho)
        desc = normalize(it.get('d', ''))
        if any(x in desc for x in ["PNEU", "LUBRIFICANTE", "ALIMENTACAO", "MERENDA"]):
            continue

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

    # C. Validação Final: Sobrou algum item útil?
    if not itens_fmt: 
        continue # Se todos os itens foram filtrados, joga o edital fora

    # --- FASE 3: APROVAÇÃO ---
    
    # Se chegou aqui, o edital é bom.
    # 1. Salva no banco "bruto" (mas agora limpo de verdade)
    banco_filtrado_final.append(p)

    # 2. Formata para o Monitor Web
    web_data.append({
        'id': p.get('id'), 
        'uf': uf, 
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

# Ordenação (Mais recentes primeiro)
web_data.sort(key=lambda x: x['data_enc'], reverse=True)

# --- FASE 4: SOBRESCRITA DOS ARQUIVOS ---

# Salva o banco de dados mestre (Reduzido e Limpo)
with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f: 
    json.dump(banco_filtrado_final, f, ensure_ascii=False)

# Salva o arquivo do site
with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: 
    json.dump(web_data, f, ensure_ascii=False)

removidos = inicial - len(banco_filtrado_final)

print(f"✅ Auditoria Concluída!")
print(f"   📉 Registros Originais: {inicial}")
print(f"   🚫 Removidos (Data/Geo/Lixo): {removidos}")
print(f"   💾 Banco de Dados Atualizado: {len(banco_filtrado_final)}")
print(f"   🌐 Disponível no Monitor: {len(web_data)}")
