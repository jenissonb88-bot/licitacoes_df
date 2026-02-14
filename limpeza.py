import json, os, gzip, unicodedata, re

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_EXCLUIDOS = 'excluidos.txt'

# === REGRAS DE LIMPEZA (BLACKBOARD) ===
TERMOS_PROIBIDOS = [
    "MERENDA", "FEIJAO", "ARROZ", "CARNE", "HORTIFRUTI", "PADARIA", "REFEICAO",
    "LIMPEZA PREDIAL", "SACO DE LIXO", "SABAO", "DETERGENTE", "VASSOURA",
    "PNEU", "VEICULO", "AUTO", "OBRA", "CIMENTO", "TIJOLO", "ASFALTO",
    "SOFTWARE", "COMPUTADOR", "NOTEBOOK", "INTERNET", "FARDAMENTO", "UNIFORME"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_excluidos():
    if not os.path.exists(ARQ_EXCLUIDOS): return set()
    with open(ARQ_EXCLUIDOS, 'r') as f:
        return {l.strip() for l in f if l.strip()}

def executar_limpeza():
    if not os.path.exists(ARQ_DADOS):
        print("❌ Arquivo de dados não encontrado para limpeza.")
        return

    print("🧹 Iniciando a faxina no banco de dados...")
    
    ids_banidos = carregar_excluidos()
    banco_limpo = {}
    cont_removidos = 0

    try:
        with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
            dados = json.load(f)
            
        for lic in dados:
            id_lic = lic['id']
            objeto = normalizar(lic['objeto'])
            
            # 1. Filtro de IDs Banidos Manualmente
            if id_lic in ids_banidos:
                cont_removidos += 1
                continue

            # 2. Filtro de Termos Proibidos no Objeto
            if any(termo in objeto for termo in TERMOS_PROIBIDOS):
                cont_removidos += 1
                continue

            # 3. Processamento de Itens e Classificação ME/EPP
            itens_validos = []
            sim_me = 0
            nao_me = 0
            total_est = 0

            for it in lic.get('itens', []):
                desc_it = normalizar(it['desc'])
                # Remove itens individuais de lixo dentro de edital bom
                if any(termo in desc_it for termo in TERMOS_PROIBIDOS):
                    continue
                
                itens_validos.append(it)
                total_est += it.get('total_est', 0)
                
                if it.get('me_epp') == "Sim": sim_me += 1
                else: nao_me += 1

            if not itens_validos:
                cont_removidos += 1
                continue

            # 4. Atribuição de Metadados de Qualidade
            lic['itens'] = itens_validos
            lic['valor_total_calculado'] = total_est
            lic['is_sigiloso'] = total_est <= 0
            
            # Classificação da Tarja
            if sim_me == len(itens_validos): lic['tipo_participacao'] = "EXCLUSIVO"
            elif sim_me > 0: lic['tipo_participacao'] = "PARCIAL"
            else: lic['tipo_participacao'] = "AMPLO"

            banco_limpo[id_lic] = lic

        # Salvar o banco lapidado
        lista_final = sorted(banco_limpo.values(), key=lambda x: x.get('data_encerramento') or '', reverse=True)
        
        with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
            json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

        print(f"✅ Faxina concluída! Removidos: {cont_removidos} | Mantidos: {len(lista_final)}")

    except Exception as e:
        print(f"⚠️ Erro durante a limpeza: {e}")

if __name__ == "__main__":
    executar_limpeza()
