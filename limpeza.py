import json
import gzip
import os
import unicodedata
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'

# --- 1. WHITELIST (SALVA SEMPRE) ---
# Prioridade Máxima: Passa mesmo se tiver termos proibidos.
WHITELIST_OBJETO = [
    "FRALDA", "ABSORVENTE"
]

# --- 2. BLACKLIST (DESCARTA SE TIVER) ---
BLACKLIST_OBJETO = [
    # Originais
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", 
    "AR CONDICIONADO", "OBRAS", "ENGENHARIA", "CONFECCAO", 
    "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "VETERINARIO", "ANIMAIS", "RACAO",
    "ODONTOLOGICO", "ODONTO",
    
    # Novos Solicitados
    "GENERO ALIMENTICIO", 
    "MATERIAL DE CONSTRUCAO", 
    "MATERIAL ELETRICO", 
    "MATERIAL ESPORTIVO", 
    "LOCACAO DE EQUIPAMENTO", 
    "AQUISICAO DE EQUIPAMENTO", 
    "EXAME LABORATORI", 
    "MERENDA" # Já cobre "MERENDA ESCOLAR"
]

def normalize(texto):
    """Remove acentos e coloca em maiúsculo"""
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# Pré-processa listas
WHITELIST_NORM = [normalize(x) for x in WHITELIST_OBJETO]
BLACKLIST_NORM = [normalize(x) for x in BLACKLIST_OBJETO]

print("🧹 LIMPEZA V5 - CORTE 2026 + REGRAS AVANÇADAS")

# --- DATA DE CORTE AJUSTADA ---
# Descarta tudo que encerrou ANTES de 01/01/2026
data_limite = datetime(2026, 1, 1, 0, 0, 0)

if not os.path.exists(ARQDADOS):
    print(f"❌ Arquivo {ARQDADOS} não encontrado.")
    exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    todos = json.load(f)

print(f"📦 {len(todos)} pregões carregados")

limpos = []
duplicatas = set()
excluidos_enc = 0
excluidos_blacklist = 0
excluidos_limpeza = 0

for preg in todos:
    id_preg = preg.get('id')
    
    if id_preg in duplicatas: continue
    duplicatas.add(id_preg)
    
    # Filtro Data (CORTE 01/01/2026)
    data_enc = preg.get('dataEnc', '')
    try:
        if data_enc:
            data_enc_dt = datetime.fromisoformat(data_enc.replace('Z', '+00:00'))
            # Se a data de encerramento for MENOR que 01/01/2026, tchau.
            if data_enc_dt.replace(tzinfo=None) < data_limite:
                excluidos_enc += 1
                continue
    except: pass

    # --- LÓGICA DE FILTRAGEM ---
    objeto_txt = preg.get('objeto', '')
    objeto_norm = normalize(objeto_txt)
    
    manter_pregao = True 
    motivo_exclusao = ""

    # REGRA 1: Whitelist (Fraldas/Absorventes) - Prioridade Total
    if any(t in objeto_norm for t in WHITELIST_NORM):
        manter_pregao = True
    
    # REGRA 2: Material de Limpeza (Só fica se tiver Álcool 70 nos itens)
    elif "MATERIAL DE LIMPEZA" in objeto_norm:
        tem_alcool_70 = False
        raw_itens = preg.get('itensraw', [])
        
        # Verifica nos itens se tem álcool
        if raw_itens and isinstance(raw_itens, list):
            for item in raw_itens:
                desc_item = normalize(item.get('descricao', ''))
                if "ALCOOL" in desc_item and "70" in desc_item:
                    tem_alcool_70 = True
                    break
        
        if not tem_alcool_70:
            manter_pregao = False
            excluidos_limpeza += 1
            motivo_exclusao = "Limpeza s/ Alcool 70"

    # REGRA 3: Blacklist Padrão
    elif any(t in objeto_norm for t in BLACKLIST_NORM):
        manter_pregao = False
        excluidos_blacklist += 1
        motivo_exclusao = "Blacklist Objeto"

    if not manter_pregao:
        continue

    # --- PROCESSA E SALVA ---
    lista_itens = []
    raw = preg.get('itensraw', [])
    if raw and isinstance(raw, list):
        for item in raw:
            lista_itens.append({
                'n': item.get('numeroItem'),
                'desc': item.get('descricao', ''),
                'qtd': item.get('quantidade', 0),
                'un': item.get('unidadeMedida', ''),
                'valUnit': item.get('valorUnitarioEstimado', 0)
            })

    limpos.append({
        'id': id_preg,
        'uf': preg.get('uf', ''),
        'cidade': preg.get('cidade', ''),
        'edital': preg.get('editaln', ''),
        'valor_estimado': round(preg.get('valorGlobalApi', 0), 2),
        'data_pub': preg.get('dataPub', ''),
        'data_enc': data_enc,
        'objeto': objeto_txt[:250],
        'link': preg.get('link', ''),
        'itens': lista_itens,
        'resultados_count': len(preg.get('resultadosraw', []))
    })

print(f"\n📊 RESULTADO:")
print(f"  📦 Origem: {len(todos)}")
print(f"  ❌ Antigos (< 2026): {excluidos_enc}")
print(f"  ❌ Blacklist: {excluidos_blacklist}")
print(f"  ❌ Limpeza (s/ Álcool): {excluidos_limpeza}")
print(f"  ✅ Mantidos: {len(limpos)}")

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)

print("🎉 LIMPEZA CONCLUÍDA!")
