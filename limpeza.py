import json
import gzip
import os
import logging

# Configuração de logs para acompanhamento no GitHub Actions
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

def limpar_e_minificar():
    ARQ_ENTRADA = 'dadosoportunidades.json.gz'
    ARQ_SAIDA = 'pregacoes_pharma_limpos.json.gz'

    if not os.path.exists(ARQ_ENTRADA):
        logging.error(f"❌ Erro: Ficheiro {ARQ_ENTRADA} não encontrado.")
        return

    logging.info("🧹 Iniciando deduplicação e limpeza de banco de dados...")

    try:
        with gzip.open(ARQ_ENTRADA, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)
    except Exception as e:
        logging.error(f"❌ Erro ao ler dados: {e}")
        return

    # 1. DEDUPLICAÇÃO E ATUALIZAÇÃO
    # Usamos um dicionário para garantir que cada ID seja único
    base_limpa = {}
    for lic in licitacoes:
        id_lic = lic.get('id')
        if not id_lic: continue
        
        # Se o edital já existe na base, comparamos a data de encerramento.
        # Mantemos sempre o que tiver a data mais futura ou recente.
        if id_lic in base_limpa:
            data_existente = base_limpa[id_lic].get('dt_enc', '')
            data_nova = lic.get('dt_enc', '')
            
            # Se a nova informação for mais recente ou tiver data de encerramento maior, substitui
            if data_nova and (not data_existente or data_nova > data_existente):
                base_limpa[id_lic] = lic
        else:
            base_limpa[id_lic] = lic

    # 2. CONSOLIDAÇÃO
    # Transformamos o dicionário de volta em uma lista para o JSON
    resultado_final = list(base_limpa.values())

    try:
        # Salvamos no formato comprimido que o novo index.html (com Pako) consegue ler
        with gzip.open(ARQ_SAIDA, 'wt', encoding='utf-8') as f:
            json.dump(resultado_final, f, ensure_ascii=False)
        logging.info(f"✅ Sucesso: {len(resultado_final)} licitações únicas filtradas e salvas.")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar ficheiro final: {e}")

if __name__ == '__main__':
    limpar_e_minificar()
