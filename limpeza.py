import json, os, gzip, unicodedata, re, pandas as pd

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_EXCLUIDOS = 'excluidos.txt'
ARQ_CSV = 'Exportar Dados.csv'
DATA_CORTE = "2026-01-01"

# === BLACKLIST ABSOLUTA NO OBJETO ===
BLACKLIST_OBJETO = [
    "LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", 
    "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", 
    "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "MATERIAIS PERMANENTES", 
    "MATERIAL DE PINTURA", "MATERIAIS DE CONSTRUCAO", "GENERO ALIMENTICIO", 
    "GENEROS ALIMENTICIOS", "MERENDA", "ESCOLAR", "EXPEDIENTE", 
    "MATERIAIS DE EXPEDIENTE", "EXAMES", "LABORATORIO", "LABORATORIAIS"
]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def carregar_keywords_venda():
    keywords = set()
    if os.path.exists(ARQ_CSV):
        try:
            df = pd.read_csv(ARQ_CSV, encoding='latin1', sep=None, engine='python')
            # Busca em todas as colunas do CSV por palavras relevantes
            for col in df.columns:
                for val in df[col].dropna().unique():
                    norm = normalize(str(val))
                    if len(norm) > 3: keywords.add(norm)
        except: pass
    return keywords

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    
    keywords_venda = carregar_keywords_venda()
    lista_negra_manual = []
    if os.path.exists(ARQ_EXCLUIDOS):
        with open(ARQ_EXCLUIDOS, 'r') as f:
            lista_negra_manual = [line.strip() for line in f.readlines()]

    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        # 1. Filtro de Objeto e UF
        obj_norm = normalize(lic.get('objeto', ''))
        if any(termo in obj_norm for termo in BLACKLIST_OBJETO): continue
        if lic.get('uf') not in UFS_ALVO: continue
        if lic['id'] in lista_negra_manual: continue
        if lic.get('data_enc') and lic['data_enc'][:10] < DATA_CORTE: continue

        if 'itens_raw' not in lic:
            banco_final.append(lic); continue

        itens_proc = []
        soma_total = 0
        
        # 2. Processamento de TODOS os itens (sem exclusão)
        for it in lic['itens_raw']:
            num = int(it.get('numeroItem') or it.get('sequencialItem') or 0)
            desc_norm = normalize(it.get('descricao', ''))
            
            # Checa se o item está no seu CSV de vendas
            is_match = any(k in desc_norm for k in keywords_venda)
            
            v_unit = float(it.get('valorUnitarioEstimado') or 0)
            qtd = float(it.get('quantidade') or 0)
            v_total = float(it.get('valorTotalEstimado') or (v_unit * qtd))
            soma_total += v_total
            
            itens_proc.append({
                "item": num,
                "desc": it.get('descricao', ''),
                "qtd": qtd,
                "unitario_est": v_unit,
                "total_est": v_total,
                "me_epp": "Sim" if it.get('tipoBeneficioId') in [1, 2, 3] else "Não",
                "match": is_match, # MARCAÇÃO PARA O VERDE
                "situacao": mapa_sit.get(it.get('situacaoCompraItemId'), "EM ANDAMENTO"),
                "fornecedor": "EM ANDAMENTO"
            })

        # 3. Cruzamento de Resultados (Vencedores)
        for res in lic.get('resultados_raw', []):
            num_res = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            for it_final in itens_proc:
                if it_final['item'] == num_res:
                    it_final.update({
                        "situacao": "HOMOLOGADO",
                        "fornecedor": res.get('nomeRazaoSocialFornecedor') or "VENCEDOR"
                    })

        # 4. Cálculo de Tarjas
        sim_me = sum(1 for i in itens_proc if i['me_epp'] == "Sim")
        if sim_me == len(itens_proc): lic['tarja'] = "TODO EXCLUSIVO"
        elif sim_me == 0: lic['tarja'] = "TODO AMPLO"
        else: lic['tarja'] = "PARCIAL"

        lic['valor_total_final'] = soma_total
        lic['itens'] = sorted(itens_proc, key=lambda x: x['item'])
        
        lic.pop('itens_raw', None)
        lic.pop('resultados_raw', None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    limpar()
