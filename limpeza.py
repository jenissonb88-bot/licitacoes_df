import json, os, gzip, unicodedata, re

ARQ_DADOS = 'dados/oportunidades.json.gz'

# === BLACKLIST REFORÇADA (ELIMINAÇÃO TOTAL DE RUÍDO) ===
BLACKLIST = [
    "LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "ODONTOLOGICO", "DENTARIO", 
    "EQUIPAMENTO", "PERMANENTE", "APARELHO", "VEICULO", "CARRO", "PNEU", 
    "OBRA", "CONSTRUCAO", "REFORMA", "MOBILIARIO", "MOVEIS", "CADEIRA", 
    "INFORMATICA", "COMPUTADOR", "SOFTWARE", "MANUTENCAO", "LIMPEZA", 
    "MERENDA", "ALIMENTICIO", "REFEICAO", "PAPELARIA", "ESCOLAR", "FARDAMENTO", 
    "UNIFORME", "ASSISTENCIA MEDICA", "PLANO DE SAUDE", "SERVICO DE", "CONSULTORIA"
]

# === FOCO EM FÁRMACOS E INSUMOS ===
KEYWORDS = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "SERINGA", "AGULHA", "LUVA", "GAZE", "EQUIPO", "CATETER", "DIETA", "ENTERAL", "INSUMO"]

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def eh_relevante(texto):
    texto = normalize(texto)
    # REGRA DE OURO: Se tiver qualquer palavra da Blacklist, descarta na hora (False)
    if any(b in texto for b in BLACKLIST): 
        return False
    # Só retorna True se tiver uma palavra-chave de fármaco/insumo
    return any(k in texto for k in KEYWORDS)

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        # Se já foi limpo, apenas mantém
        if 'itens_raw' not in lic:
            banco_final.append(lic)
            continue

        # CRITÉRIO DE FILTRO: Objeto ou Itens precisam ser relevantes E não estar na Blacklist
        objeto_valido = eh_relevante(lic.get('objeto', ''))
        itens_filtrados_brutos = [it for it in lic.get('itens_raw', []) if eh_relevante(it.get('descricao', ''))]

        if not objeto_valido and not itens_filtrados_brutos:
            continue

        itens_proc = {}
        for it in lic['itens_raw']:
            # Só processa o item se ele individualmente for relevante e não proibido
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

        # Se após filtrar os itens não sobrar nenhum item de saúde real, descarta o pregão
        if not itens_proc:
            continue

        for res in lic.get('resultados_raw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            if num in itens_proc:
                itens_proc[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor') or res.get('nomeFornecedor') or "VENCEDOR"
                })

        sim_me = sum(1 for i in itens_proc.values() if i['me_epp'] == 'Sim')
        lic['tarja'] = "EXCLUSIVO" if sim_me == len(itens_proc) else ("PARCIAL" if sim_me > 0 else "AMPLO")
        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        
        # Limpa o excesso de peso do JSON
        lic.pop('itens_raw', None)
        lic.pop('resultados_raw', None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))
    print(f"🧹 Limpeza concluída. {len(banco_final)} pregões de saúde puros salvos.")

if __name__ == "__main__":
    limpar()
