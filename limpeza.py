import json
import gzip
import os
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

def limpar_e_minificar():
    ARQ_ENTRADA = 'dadosoportunidades.json.gz'
    ARQ_SAIDA = 'pregacoes_pharma_limpos.json.gz'

    if not os.path.exists(ARQ_ENTRADA):
        logging.error("❌ Ficheiro de entrada não encontrado.")
        return

    logging.info("🧹 Iniciando limpeza e deduplicação...")

    try:
        with gzip.open(ARQ_ENTRADA, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)
    except Exception as e:
        logging.error(f"❌ Erro ao ler dados: {e}")
        return

    # 1. DEDUPLICAÇÃO: Mantém apenas a versão mais recente de cada licitação
    base_limpa = {}
    for lic in licitacoes:
        id_lic = lic.get('id')
        if not id_lic: continue
        
        # Se já existe, comparamos a data de encerramento para manter a mais atual
        if id_lic in base_limpa:
            data_existente = base_limpa[id_lic].get('dt_enc', '')
            data_nova = lic.get('dt_enc', '')
            if data_nova > data_existente:
                base_limpa[id_lic] = lic
        else:
            base_limpa[id_lic] = lic

    # 2. MINIFICAÇÃO: Removemos campos que não são usados no Front-End
    # Mantemos a estrutura: id, org, uf, obj, edit, link, itens, sit_global
    resultado_final = list(base_limpa.values())

    try:
        with gzip.open(ARQ_SAIDA, 'wt', encoding='utf-8') as f:
            json.dump(resultado_final, f, ensure_ascii=False)
        logging.info(f"✅ Limpeza concluída! {len(resultado_final)} licitações únicas prontas.")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar: {e}")

if __name__ == '__main__':
    limpar_e_minificar()
