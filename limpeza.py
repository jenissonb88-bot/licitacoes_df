import json, os, gzip, unicodedata, re, pandas as pd

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CSV = 'Exportar Dados.csv'

# BLACKLIST RIGOROSA NO OBJETO
BLACKLIST_OBJETO = ["LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "GENERO ALIMENTICIO", "MERENDA", "ESCOLAR", "EXPEDIENTE", "EXAMES", "LABORATORIO"]
UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t or '').upper()) if unicodedata.category(c) != 'Mn')

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    
    meus_produtos = set()
    if os.path.exists(ARQ_CSV):
        try:
            df = pd.read_csv(ARQ_CSV, encoding='latin1', sep=None, engine='python')
            for val in df.iloc[:, 0].dropna().unique():
                meus_produtos.add(normalize(str(val)))
        except: pass

    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        obj_norm = normalize(lic.get('objeto', ''))
        # FILTRO DE BLACKLIST NO OBJETO
        if any(t in obj_norm for t in BLACKLIST_OBJETO): continue
        # FILTRO DE ESTADO
        if lic.get('uf') not in UFS_ALVO: continue
        # FILTRO DE DATA
        if lic.get('data_enc') and lic['data_enc'][:10] < "2026-01-01": continue

        if 'itens_raw' not in lic:
            banco_final.append(lic); continue

        itens_proc = {}
        for it in lic['itens_raw']:
            num = int(it.get('numeroItem') or it.get('sequencialItem') or 0)
            desc = it.get('descricao', '')
            
            itens_proc[num] = {
                "item": num,
                "desc": desc,
                "qtd": float(it.get('quantidade') or 0),
                "unit_est": float(it.get('valorUnitarioEstimado') or 0),
                "total_est": float(it.get('valorTotalEstimado') or 0),
                "unit_hom": 0.0,
                "total_hom": 0.0,
                "me_epp": "Sim" if it.get('tipoBeneficioId') in [1, 2, 3] else "Não",
                "match": any(p in normalize(desc) for p in meus_produtos),
                "situacao": mapa_sit.get(it.get('situacaoCompraItemId'), "EM ANDAMENTO"),
                "fornecedor": "EM ANDAMENTO"
            }

        # VINCULANDO RESULTADOS EXAUSTIVOS
        for res in lic.get('resultados_raw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            if num in itens_proc:
                itens_proc[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor') or "VENCEDOR",
                    "unit_hom": float(res.get('valorUnitarioHomologado') or 0),
                    "total_hom": float(res.get('valorTotalHomologado') or 0)
                })

        # TARJAS DE EXCLUSIVIDADE
        count_me = sum(1 for i in itens_proc.values() if i['me_epp'] == "Sim")
        if count_me == len(itens_proc): lic['tarja'] = "TODO EXCLUSIVO"
        elif count_me == 0: lic['tarja'] = "TODO AMPLO"
        else: lic['tarja'] = "PARCIAL"
        
        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        lic['valor_final'] = sum(i['total_est'] for i in itens_proc.values())
        
        # PRESERVAÇÃO DOS CAMPOS MESTRE
        for k in ['itens_raw', 'resultados_raw', 'valor_global_api']: lic.pop(k, None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__": limpar()
