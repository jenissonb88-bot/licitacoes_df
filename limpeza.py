import json
import gzip
import os
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'

print("🧹 LIMPEZA - ATIVOS (dataEnc ≥ 01/12/2025)")

data_limite = datetime(2025, 12, 1, 23, 59, 59)

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    todos = json.load(f)

print(f"📦 {len(todos)} pregões carregados")

limpos = []
duplicatas = set()
excluidos_enc = 0

for preg in todos:
    id_preg = preg.get('id')
    
    if id_preg in duplicatas: 
        continue
    duplicatas.add(id_preg)
    
    data_enc = preg.get('dataEnc', '')
    try:
        data_enc_dt = datetime.fromisoformat(data_enc.replace('Z', '+00:00'))
        if data_enc_dt < data_limite:
            excluidos_enc += 1
            continue
    except:
        continue
    
    limpos.append({
        'id': id_preg,
        'uf': preg.get('uf', ''),
        'cidade': preg.get('cidade', ''),
        'edital': preg.get('editaln', ''),
        'valor_estimado': round(preg.get('valorGlobalApi', 0), 2),
        'data_pub': preg.get('dataPub', ''),
        'data_enc': data_enc,
        'objeto': preg.get('objeto', '')[:150],
        'link': preg.get('link', ''),
        'itens_count': len(preg.get('itensraw', [])),
        'resultados_count': len(preg.get('resultadosraw', []))
    })

print(f"\n📊 RESULTADO:")
print(f"  📦 Carregados: {len(todos)}")
print(f"  ❌ Excluídos: {excluidos_enc}")
print(f"  ✅ Mantidos: {len(limpos)}")

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)

print("🎉 LIMPEZA OK!")
