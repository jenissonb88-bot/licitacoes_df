import json, os, gzip, unicodedata, re
from datetime import datetime

ARQ_DADOS = 'dados/oportunidades.json.gz'

# === CONFIGURAÇÃO DE CORTE CRONOLÓGICO ===
DATA_CORTE = "2026-01-01"

# === BLACKLIST REFORÇADA (ELIMINAÇÃO TOTAL DE RUÍDO) ===
BLACKLIST = [
    "LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "ODONTOLOGICO", "DENTARIO", 
    "EQUIPAMENTO", "APARELHO", "VEICULO", "OBRA", "CONSTRUCAO", "MOBILIARIO", 
    "INFORMATICA", "LIMPEZA", "MERENDA", "ALIMENTICIO", "PAPELARIA"
]

# === FOCO EM FÁRMACOS E INSUMOS ===
KEYWORDS = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "SERINGA", "AGULHA", "LUVA", "GAZE", "EQUIPO", "INSUMO", "DIETA"]

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def eh_relevante(texto):
    texto = normalize(texto)
    if any(b in texto for b in BLACKLIST): return False
    return any(k in texto for k in KEYWORDS)

def limpar():
    if not os.path.exists(ARQ_DADOS): 
        print("Arquivo de dados não encontrado.")
        return
        
    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}
    removidos_por_data = 0
    removidos_por_filtro = 0

    for lic in dados:
        # --- 1. FILTRO DE DATA (EXCLUSÃO DE 2025 E ANTERIORES) ---
        data_enc = lic.get('data_enc')
        if data_enc:
            # Comparamos apenas os primeiros 10 caracteres (YYYY-MM-DD)
            if data_enc[:10] < DATA_CORTE:
                removidos_por_data += 1
                continue

        # Se já foi processado e limpo em rodada anterior, apenas mantém
        if 'itens_raw' not in lic:
            banco_final.append(lic)
            continue

        # --- 2. FILTRO DE RELEVÂNCIA (BLACKLIST + KEYWORDS) ---
        objeto_valido = eh_relevante(lic.get('objeto', ''))
        # Pré-filtro nos itens brutos
        tem_item_saude = any(eh_relevante(it.get('descricao', '')) for it in lic.get('itens_raw', []))

        if not objeto_valido and not tem_item_saude:
            removidos_por_filtro += 1
            continue

        itens_proc = {}
        for it in lic['itens_raw']:
            # Só aceita o item se ele não for lixo (blacklist) e for de saúde
            if not eh_relevante(it.get('descricao', '')):
                continue

            num = int(it.get('numeroItem') or it.get('sequencialItem') or 0)
            if num == 0: continue
            
            itens_proc[num] = {
                "item": num,
                "desc": it.get('descricao', 'Sem descrição'),
                "qtd": it.get('quantidade', 0),
                "unitario_est": it.get('valorUnitarioEstimado', 0),
                "total_est": it.get('valorTotalEstimado', 0),
                "me_epp": "Sim" if it.get('tipoBeneficioId') in [1, 2, 3] else "Não",
                "situacao": mapa_sit.get(it.get('situacaoCompraItemId'), "EM ANDAMENTO"),
                "fornecedor": "EM ANDAMENTO"
            }

        if not itens_proc:
            removidos_por_filtro += 1
            continue

        # Processar Vencedores (Resultados)
        for res in lic.get('resultados_raw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            if num in itens_proc:
                itens_proc[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor') or res.get('nomeFornecedor') or "VENCEDOR"
                })

        # --- 3. LÓGICA DE TARJAS (CORRIGIDA) ---
        sim_me = sum(1 for i in itens_proc.values() if i['me_epp'] == 'Sim')
        if sim_me == len(itens_proc):
            lic['tarja'] = "TODO EXCLUSIVO"
        elif sim_me == 0:
            lic['tarja'] = "TODO AMPLO"
        else:
            lic['tarja'] = "PARCIAL"

        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        
        # Limpa chaves brutas para reduzir o tamanho do arquivo final
        lic.pop('itens_raw', None)
        lic.pop('resultados_raw', None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"🧹 Faxina Finalizada:")
    print(f"   - Removidos por data (pré-2026): {removidos_por_data}")
    print(f"   - Removidos pela Blacklist: {removidos_por_filtro}")
    print(f"   - Total mantido em base: {len(banco_final)}")

if __name__ == "__main__":
    limpar()
