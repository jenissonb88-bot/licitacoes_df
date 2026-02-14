import json, os, gzip, unicodedata

ARQ_DADOS = 'dados/oportunidades.json.gz'
TERMOS_LIXO = ["MERENDA", "FEIJAO", "ARROZ", "CARNE", "HORTIFRUTI", "LIMPEZA PREDIAL", "PNEU", "OBRA"]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def executar_limpeza():
    if not os.path.exists(ARQ_DADOS): return

    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_limpo = []
    for lic in dados:
        objeto = normalizar(lic['objeto'])
        if any(t in objeto for t in TERMOS_LIXO): continue

        itens_finais = []
        sim_me, total_est = 0, 0

        for it in lic.get('itens', []):
            it['me_epp'] = "Sim" if it.get('beneficio_id') in [1, 2, 3] else "Não"
            if it['me_epp'] == "Sim": sim_me += 1
            total_est += it.get('total_est', 0)
            itens_finais.append(it)

        if not itens_finais: continue

        if sim_me == len(itens_finais): lic['tarja'] = "EXCLUSIVO"
        elif sim_me > 0: lic['tarja'] = "PARCIAL"
        else: lic['tarja'] = "AMPLO"

        lic['itens'] = itens_finais
        lic['total_calculado'] = total_est
        banco_limpo.append(lic)

    banco_limpo.sort(key=lambda x: x.get('data_enc') or '', reverse=True)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_limpo, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    executar_limpeza()
