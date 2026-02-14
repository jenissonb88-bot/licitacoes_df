import json, os, gzip, unicodedata, re

ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_EXCLUIDOS = 'excluidos.txt'
DATA_CORTE = "2026-01-01"

# === SUA BLACKLIST ABSOLUTA (Veto Direto no Objeto) ===
BLACKLIST_OBJETO = [
    "LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", 
    "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", 
    "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "MATERIAIS PERMANENTES", 
    "MATERIAL DE PINTURA", "MATERIAIS DE CONSTRUCAO", "GENERO ALIMENTICIO", 
    "GENEROS ALIMENTICIOS", "MERENDA", "ESCOLAR", "EXPEDIENTE", 
    "MATERIAIS DE EXPEDIENTE", "EXAMES", "LABORATORIO", "LABORATORIAIS"
]

# === ESTADOS ALVO ===
UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

# === KEYWORDS DE SAÚDE (Para validação secundária) ===
KEYWORDS_SAUDE = ["MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "SERINGA", "AGULHA", "LUVA", "GAZE", "EQUIPO", "INSUMO", "DIETA", "ENTERAL"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper()) if unicodedata.category(c) != 'Mn')

def objeto_e_proibido(objeto_texto):
    obj_norm = normalize(objeto_texto)
    # Se encontrar qualquer termo da blacklist no objeto, retorna Verdadeiro (Proibido)
    for termo in BLACKLIST_OBJETO:
        if termo in obj_norm:
            return True
    return False

def limpar():
    if not os.path.exists(ARQ_DADOS): return
    
    lista_negra_manual = []
    if os.path.exists(ARQ_EXCLUIDOS):
        with open(ARQ_EXCLUIDOS, 'r') as f:
            lista_negra_manual = [line.strip() for line in f.readlines()]

    with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
        dados = json.load(f)

    banco_final = []
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}

    for lic in dados:
        # 1. Veto da Blacklist Direta no Objeto (Sua solicitação atual)
        if objeto_e_proibido(lic.get('objeto', '')):
            continue

        # 2. Filtro de Estados
        if lic.get('uf') not in UFS_ALVO: continue
        
        # 3. Filtro de Exclusão Manual (IDs que você clicou em excluir)
        if lic['id'] in lista_negra_manual: continue
        
        # 4. Filtro de Data (Corte 2026)
        if lic.get('data_enc') and lic['data_enc'][:10] < DATA_CORTE: continue

        # Se já foi processado e lapidado, mantém
        if 'itens_raw' not in lic:
            banco_final.append(lic)
            continue

        # 5. Processamento de Itens e Resultados
        itens_proc = {}
        soma_total = 0
        for it in lic['itens_raw']:
            desc_norm = normalize(it.get('descricao', ''))
            # Só entra se o item for de saúde (Keywords)
            if any(k in desc_norm for k in KEYWORDS_SAUDE):
                num = int(it.get('numeroItem') or it.get('sequencialItem') or 0)
                v_unit = float(it.get('valorUnitarioEstimado') or 0)
                qtd = float(it.get('quantidade') or 0)
                v_total = float(it.get('valorTotalEstimado') or (v_unit * qtd))
                soma_total += v_total
                
                itens_proc[num] = {
                    "item": num, "desc": it.get('descricao', ''), "qtd": qtd,
                    "unitario_est": v_unit, "total_est": v_total,
                    "me_epp": "Sim" if it.get('tipoBeneficioId') in [1, 2, 3] else "Não",
                    "situacao": mapa_sit.get(it.get('situacaoCompraItemId'), "EM ANDAMENTO"),
                    "fornecedor": "EM ANDAMENTO"
                }

        if not itens_proc: continue

        # Cruzamento de Resultados
        for res in lic.get('resultados_raw', []):
            num = int(res.get('numeroItem') or res.get('sequencialItem') or 0)
            if num in itens_proc:
                itens_proc[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor') or "VENCEDOR"
                })

        # Tarjas e Valores
        lic['valor_total_final'] = soma_total if soma_total > 0 else lic.get('valor_estimado_cabecalho', 0)
        lic['is_sigiloso'] = lic.get('sigiloso_original') or (lic['valor_total_final'] <= 0)
        
        sim_me = sum(1 for i in itens_proc.values() if i['me_epp'] == "Sim")
        if sim_me == len(itens_proc): lic['tarja'] = "TODO EXCLUSIVO"
        elif sim_me == 0: lic['tarja'] = "TODO AMPLO"
        else: lic['tarja'] = "PARCIAL"

        lic['itens'] = sorted(itens_proc.values(), key=lambda x: x['item'])
        for k in ['itens_raw', 'resultados_raw', 'valor_estimado_cabecalho', 'sigiloso_original']:
            lic.pop(k, None)
        banco_final.append(lic)

    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(banco_final, f, ensure_ascii=False, separators=(',', ':'))
    print(f"🧹 Limpeza concluída. {len(banco_final)} pregões de saúde puros em base.")

if __name__ == "__main__":
    limpar()
