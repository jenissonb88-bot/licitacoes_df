import json, os, gzip, unicodedata, re, pandas as pd

ARQDADOS = 'dadosoportunidades.json.gz'
ARQCSV = 'Exportar Dados.csv'

BLACKLISTOBJETO = ['LOCACAO','ALUGUEL','GRAFICO','IMPRESSAO','EQUIPAMENTO','MOVEIS','MANUTENCAO','OBRA','INFORMATICA','VEICULO','PRESTACAO DE SERVICO','REFORMA','ESPORTIVO','MATERIAL PERMANENTE','MERENDA','ESCOLAR','EXPEDIENTE','EXAMES','LABORATORIO']

UFS_NE = ['AL','BA','CE','MA','PB','PE','PI','RN','SE']
UFS_OUTROS = ['ES','MG','RJ','SP','GO','MT','MS','DF','TO','PA','AM','RO']
UFS_EXCLUIDAS = ['PR','SC','RS','AP','AC']

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper() if unicodedata.category(c) != 'Mn')

def validar_regiao(lic):
    uf = lic.get('uf', '').upper()
    if uf in UFS_EXCLUIDAS: return False
    obj_norm = normalize(lic.get('objeto', ''))
    
    if uf in UFS_NE:
        return any(t in obj_norm for t in ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','MATERIAL MEDICO','DIETA ENTERAL','FORMULA','LU VAS','ALCOOL'])
    if uf in UFS_OUTROS:
        return any(t in obj_norm for t in ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','REMEDIO'])
    return False

def limpar():
    if not os.path.exists(ARQDADOS): return

    meusprodutos = set()
    if os.path.exists(ARQCSV):
        try:
            df = pd.read_csv(ARQCSV, encoding='latin1', sep=None, engine='python')
            for val in df.iloc[:, 0].dropna().unique():
                meusprodutos.add(normalize(str(val)))
        except: pass
    print(f"📊 Carregados {len(meusprodutos)} produtos do CSV")

    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: dados = json.load(f)
    bancofinal, mapasit = [], {1: 'EM ANDAMENTO', 2: 'HOMOLOGADO', 3: 'ANULADO', 4: 'REVOGADO', 5: 'FRACASSADO', 6: 'DESERTO'}

    for lic in dados:
        obj_norm = normalize(lic.get('objeto', ''))
        if any(t in obj_norm for t in BLACKLISTOBJETO): continue
        if not validar_regiao(lic): continue
        if lic.get('dataEnc') and lic['dataEnc'][:10] < '2026-01-01': continue

        if 'itensraw' not in lic:
            bancofinal.append(lic); continue

        itensproc, has_match = {}, False
        for it in lic['itensraw']:
            num = int(it.get('numeroItem') or it.get('sequencialItem') or 0)
            desc = it.get('descricao', ''); desc_norm = normalize(desc)
            match_prod = any(p in desc_norm for p in meusprodutos)

            itensproc[num] = {
                'item': num, 'desc': desc, 'qtd': float(it.get('quantidade') or 0),
                'unitEst': float(it.get('valorUnitarioEstimado') or 0),
                'totalEst': float(it.get('valorTotalEstimado') or 0),
                'unitHom': 0.0, 'totalHom': 0.0, 'meepp': 'Sim' if it.get('tipoBeneficioId') in [1,2,3] else 'No',
                'match': match_prod, 'situacao': mapasit.get(it.get('situacaoCompraItemId'), 'EM ANDAMENTO'),
                'fornecedor': 'EM ANDAMENTO'
            }
            if match_prod: has_match = True

        if not has_match: continue

        for res in lic.get('resultadosraw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            if num in itensproc:
                itensproc[num].update({
                    'situacao': 'HOMOLOGADO', 'fornecedor': res.get('nomeRazaoSocialFornecedor') or 'VENCEDOR',
                    'unitHom': float(res.get('valorUnitarioHomologado') or 0),
                    'totalHom': float(res.get('valorTotalHomologado') or 0)
                })

        countme = sum(1 for i in itensproc.values() if i['meepp'] == 'Sim')
        lic['tarja'] = 'TODO EXCLUSIVO' if countme == len(itensproc) else 'PARCIAL' if countme else 'TODO AMPLO'
        lic['itens'] = sorted(itensproc.values(), key=lambda x: x['item'])
        lic['valorFinal'] = sum(i['totalEst'] for i in itensproc.values())

        for k in ['itensraw', 'resultadosraw', 'valorGlobalApi']: lic.pop(k, None)
        bancofinal.append(lic)

    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(bancofinal, f, ensure_ascii=False, separators=(',', ':'))
    print(f"✅ Limpeza: {len(bancofinal)} pregões pharma finais!")

if __name__ == '__main__': limpar()
