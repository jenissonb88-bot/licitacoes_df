import json
import gzip
import os
import unicodedata
from datetime import datetime

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
# DATA DE CORTE RIGOROSA: Tudo antes disso será APAGADO do banco
DATA_CORTE_2026 = datetime(2026, 1, 1)

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

if not os.path.exists(ARQDADOS): 
    print("❌ Arquivo de dados não encontrado.")
    exit()

print(f"🔄 Iniciando limpeza profunda (Corte: {DATA_CORTE_2026.strftime('%d/%m/%Y')})...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
    banco_bruto = json.load(f)

inicial = len(banco_bruto)
banco_filtrado_2026 = [] # Vai substituir o arquivo original
web_data = [] # Vai para o site

for p in banco_bruto:
    # 1. Filtro de Data (A Grande Faxina)
    try:
        # Usa a data de encerramento como referência principal
        data_str = p.get('dt_enc', '').replace('Z', '+00:00')
        dt = datetime.fromisoformat(data_str).replace(tzinfo=None)
        
        # SE FOR ANTES DE 2026, NÃO ENTRA NO NOVO BANCO (DELETA)
        if dt < DATA_CORTE_2026: 
            continue
            
    except: 
        # Se não tem data válida, deleta por segurança
        continue

    # Se passou pelo filtro de data, adiciona ao novo banco limpo
    banco_filtrado_2026.append(p)

    # 2. Formatação para o Web/Monitor
    itens_originais = p.get('itens', [])
    if not itens_originais: continue # Edital vazio não vai pro site

    c_ex = 0
    itens_fmt = []
    
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

# Ordenação
web_data.sort(key=lambda x: x['data_enc'], reverse=True)

# 3. SALVAMENTO CRÍTICO

# A: Sobrescreve o banco original apenas com dados de 2026+
with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f: 
    json.dump(banco_filtrado_2026, f, ensure_ascii=False)

# B: Salva o arquivo do site
with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: 
    json.dump(web_data, f, ensure_ascii=False)

removidos = inicial - len(banco_filtrado_2026)

print(f"✅ Processo Concluído!")
print(f"   📉 Registros Brutos (Antes): {inicial}")
print(f"   🗑️ Registros Antigos Deletados (<2026): {removidos}")
print(f"   💾 Novo Banco de Dados (Salvo): {len(banco_filtrado_2026)}")
print(f"   🌐 Registros para o Site: {len(web_data)}")
