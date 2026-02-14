import json, os, gzip, unicodedata, re
from datetime import datetime

ARQ_DADOS = 'dados/oportunidades.json.gz'

# === SUA BLACKLIST BLINDADA MÁXIMA ===
BLACKLIST = ["ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", "LANCHE", "ALIMENTICIO", "MOBILIARIO", r"\bTI\b", "INFORMATICA", "PNEU", "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", "COMODATO", "EXAME", "LIMPEZA", "MANUTENCAO", "ASSISTENCIA MEDICA", "PLANO DE SAUDE", "ODONTOLOGICA", "TERCEIRIZACAO", "EQUIPAMENTO", "MERENDA", "COZINHA", "COPA", "HIGIENIZACAO", "EXPEDIENTE", "PAPELARIA", "LIXEIRA", "LIXO", "RODO", "VASSOURA", "COMPUTADOR", "IMPRESSORA", "TONER", "CARTUCHO", "ELETRODOMESTICO", "MECANICA", "PECA", "TECIDO", "FARDAMENTO", "UNIFORME", "HIDRAULIC", "ELETRIC", "AGRO", "VETERINARI", "ANIMAL", "MUDA", "SEMENTE", "BELICO", "MILITAR", "ARMAMENTO", "MUNICAO", "SOFTWARE", "SAAS", "PISCINA", "CIMENTO", "ASFALTO", "BRINQUEDO", "EVENTO", "SHOW", "FESTA", "GRAFICA", "PUBLICIDADE", "MARKETING", "PASSAGEM", "HOSPEDAGEM", "AR CONDICIONADO", "TELEFONIA", "INTERNET", "LINK DE DADOS", "SEGURO", "COPO", "MATERIAL ESPORTIVO", "ESPORTE", "MATERIAL DE CONSTRUCAO", "MATERIAL ESCOLAR", "MATERIAL DE EXPEDIENTE", "MATERIAL HIDRAULICO", "MATERIAL ELETRICO", "DIDATICO", "PEDAGOGICO", "FERRAGEM", "FERRAMENTA", "PINTURA", "TINTA", "MARCENARIA", "MADEIRA", "AGRICOLA", "JARDINAGEM", "ILUMINACAO", "DECORACAO", "AUDIOVISUAL", "FOTOGRAFICO", "MUSICAL", "INSTRUMENTO MUSICAL", "BRINDE", "TROFEU", "MEDALHA", "ELETROPORTATIL", "CAMA MESA E BANHO"]

# === SUAS PALAVRAS-CHAVE ===
KEYWORDS = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "SERINGA", "AGULHA", r"\bLUVA", r"\bGAZE", "ALGODAO", "DIPIRON", "PARACETAMOL", "INSULIN", "EQUIPO", "CATETER", "SONDA", "AVENTAL", "MASCARA", "CURATIVO", "ESPARADRAPO", "PROPE", "TOUCA", "DIETA", "ENTERAL", "SUPLEMENT", "FORMULA"]

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def eh_relevante(texto):
    texto = normalize(texto)
    for b in BLACKLIST:
        if re.search(b if r"\b" in b else r"\b" + b, texto): return False
    for k in KEYWORDS:
        if re.search(k if r"\b" in k else r"\b" + k, texto): return True
    return False

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        if not eh_relevante(lic['objeto']):
            # Se o objeto não diz muito, checa os itens
            if not any(eh_relevante(it['descricao']) for it in lic['itens_raw']): continue

        # Processamento de Itens e Resultados
        itens_proc = {}
        for it in lic['itens_raw']:
            num = int(it.get('numeroItem') or it.get('sequencialItem'))
            ben = it.get('tipoBeneficioId')
            itens_proc[num] = {
                "item": num, "desc": it['descricao'], "qtd": it['quantidade'],
                "unitario_est": it['valorUnitarioEstimado'], "total_est": it['valorTotalEstimado'],
                "me_epp": "Sim" if ben in [1, 2, 3] else "Não",
                "situacao": mapa_sit.get(it['situacaoCompraItemId'], "EM ANDAMENTO"),
                "fornecedor": "EM ANDAMENTO", "unitario_hom": 0, "total_hom": 0
            }

        for res in lic['resultados_raw']:
            num = int(res.get('numeroItem') or res.get('sequencialItem'))
            if num in itens_proc:
                itens_proc[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor') or "VENCEDOR",
                    "unitario_hom": res.get('valorUnitarioHomologado') or 0,
                    "total_hom": res.get('valorTotalHomologado') or 0
                })

        # Tarjas
        total_it = len(itens_proc)
        sim_me = sum(1 for i in itens_proc.values() if i['me_epp'] == 'Sim')
        if sim_me == total_it: lic['tarja'] = "EXCLUSIVO"
        elif sim_me > 0: lic['tarja'] = "PARCIAL"
        else: lic['tarja'] = "AMPLO"

        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        del lic['itens_raw'], lic['resultados_raw']
        final.append(lic)

    final.sort(key=lambda x: x.get('data_enc') or '', reverse=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(final, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    limpar()
