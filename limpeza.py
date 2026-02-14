import json, os, gzip, unicodedata, re, pandas as pd

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CSV = 'Exportar Dados.csv'
DATA_CORTE = "2026-01-01"

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]
BLACKLIST = ["LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "GENERO ALIMENTICIO", "MERENDA", "ESCOLAR", "EXPEDIENTE", "EXAMES", "LABORATORIO"]

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def carregar_vendas():
    keywords = set()
    if os.path.exists(ARQ_CSV):
        try:
            df = pd.read_csv(ARQ_CSV, encoding='latin1', sep=None, engine='python')
            for val in df.iloc[:, 0].dropna().unique():
                norm = normalize(str(val))
                if len(norm) > 3: keywords.add(norm)
        except: pass
    return keywords

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    vendas = carregar_vendas()
    
    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        obj_norm = normalize(lic.get('objeto', ''))
        if any(t in obj_norm for t in BLACKLIST) or lic.get('uf') not in UFS_ALVO: continue
        if lic.get('data_enc') and lic['data_enc'][:10] < DATA_CORTE: continue

        if 'itens_raw' not in lic:
            banco_final.append(lic); continue

        itens_proc = {}
        soma_total = 0
        for it in lic['itens_raw']:
            num = int(it.get('numeroItem') or it.get('sequencialItem') or 0)
            desc_norm = normalize(it.get('descricao', ''))
            is_match = any(k in desc_norm for k in vendas)
            
            v_unit = float(it.get('valorUnitarioEstimado') or 0)
            qtd = float(it.get('quantidade') or 0)
            v_total = float(it.get('valorTotalEstimado') or (v_unit * qtd))
            soma_total += v_total
            
            itens_proc[num] = {
                "item": num, "desc": it.get('descricao', ''), "qtd": qtd,
                "total_est": v_total, "match": is_match,
                "me_epp": "Sim" if it.get('tipoBeneficioId') in [1, 2, 3] else "Não",
                "situacao": mapa_sit.get(it.get('situacaoCompraItemId'), "EM ANDAMENTO"),
                "fornecedor": "EM ANDAMENTO"
            }

        for res in lic.get('resultados_raw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            if num in itens_proc:
                itens_proc[num].update({"situacao": "HOMOLOGADO", "fornecedor": res.get('nomeRazaoSocialFornecedor') or "VENCEDOR"})

        # Tarjas ME/EPP
        sim_me = sum(1 for i in itens_proc.values() if i['me_epp'] == "Sim")
        if sim_me == len(itens_proc): lic['tarja'] = "TODO EXCLUSIVO"
        elif sim_me == 0: lic['tarja'] = "TODO AMPLO"
        else: lic['tarja'] = "PARCIAL"

        lic['valor_total_final'] = soma_total
        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        for k in ['itens_raw', 'resultados_raw', 'valor_estimado_cabecalho', 'sigiloso_original']:
            lic.pop(k, None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    limpar()
