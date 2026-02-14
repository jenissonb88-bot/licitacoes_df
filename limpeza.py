import json, os, gzip, unicodedata, re

ARQ_DADOS = 'dados/oportunidades.json.gz'
BLACKLIST = ["ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", "LANCHE", "ALIMENTICIO", "PNEU", "INFORMATICA", "TI", "MOBILIARIO"]
KEYWORDS = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "SERINGA", "AGULHA", "LUVA", "GAZE", "EQUIPO", "CATETER", "DIETA", "ENTERAL"]

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def eh_relevante(texto):
    texto = normalize(texto)
    if any(re.search(r"\b" + b + r"\b", texto) for b in BLACKLIST): return False
    return any(re.search(r"\b" + k, texto) for k in KEYWORDS)

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        # Se NÃO tem 'itens_raw', significa que já foi limpo em outra rodada. Mantemos como está.
        if 'itens_raw' not in lic:
            banco_final.append(lic)
            continue

        # Só processamos o que for relevante
        if not eh_relevante(lic['objeto']) and not any(eh_relevante(it['descricao']) for it in lic['itens_raw']):
            continue

        itens_proc = {}
        for it in lic['itens_raw']:
            num = int(it.get('numeroItem') or it.get('sequencialItem'))
            itens_proc[num] = {
                "item": num, "desc": it['descricao'], "qtd": it['quantidade'],
                "unitario_est": it['valorUnitarioEstimado'], "total_est": it['valorTotalEstimado'],
                "me_epp": "Sim" if it.get('tipoBeneficioId') in [1, 2, 3] else "Não",
                "situacao": mapa_sit.get(it['situacaoCompraItemId'], "EM ANDAMENTO"),
                "fornecedor": "EM ANDAMENTO"
            }

        for res in lic.get('resultados_raw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem'))
            if num in itens_proc:
                itens_proc[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor') or "VENCEDOR"
                })

        # Define Tarja
        sim_me = sum(1 for i in itens_proc.values() if i['me_epp'] == 'Sim')
        lic['tarja'] = "EXCLUSIVO" if sim_me == len(itens_proc) else ("PARCIAL" if sim_me > 0 else "AMPLO")
        
        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        
        # Remove as chaves brutas para economizar espaço
        lic.pop('itens_raw', None)
        lic.pop('resultados_raw', None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))
    print(f"🧹 Limpeza concluída. Total em base: {len(banco_final)}")

if __name__ == "__main__":
    limpar()
