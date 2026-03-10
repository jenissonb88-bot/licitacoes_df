import json
import re
import os
import sys
import csv
import gzip
import logging
import unicodedata
import shutil
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    from rapidfuzz import fuzz, process
    HAS_RAPIDFUZZ = True
except ImportError:
    print("[!] A biblioteca rapidfuzz é estritamente necessária para performance. Instale-a via pip.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class AvaliadorPortfolio:
    def __init__(self):
        self.arq_licitacoes = 'pregacoes_pharma_limpos.json.gz'
        self.arq_catalogo = 'Exportar Dados.csv'
        self.threshold_alto = 0.70
        self.threshold_medio = 0.50
        
        # Pode expandir esta lista conforme os produtos do catálogo
        self.sinonimos_farmacos = {
            "ESCOPOLAMINA": ["HIOSCINA", "BUSCOPAN", "BUTILBROMETO DE ESCOPOLAMINA"],
            "DIPIRONA": ["NOVALGINA", "METAMIZOL", "DIPIRONA SODICA"],
            "PARACETAMOL": ["TYLENOL", "ACETAMINOFENO"],
            "CEFTRIAXONA": ["ROCEFIN", "CEFTRIAXON"],
            "CLORETO DE SODIO": ["SORO FISIOLOGICO", "SF 0,9%"]
        }
        
        self.portfolio_normalizado = []

    def normalizar_texto(self, texto):
        if not texto:
            return ""
        texto = ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn')
        texto = texto.upper()
        return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', ' ', texto)).strip()

    def carregar_portfolio(self):
        if not os.path.exists(self.arq_catalogo):
            logger.error(f"Catálogo {self.arq_catalogo} não encontrado.")
            sys.exit(1)
            
        logger.info("A carregar portfólio de produtos...")
        with open(self.arq_catalogo, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                descricao = row.get('Descrição', '')
                codigo = row.get('Codigo', '')
                if descricao:
                    desc_norm = self.normalizar_texto(descricao)
                    self.portfolio_normalizado.append({
                        'id': codigo,
                        'descricao_original': descricao,
                        'descricao_norm': desc_norm
                    })
        logger.info(f"Portfólio carregado com {len(self.portfolio_normalizado)} itens.")

    def avaliar_item_licitacao(self, licitacao):
        matches_encontrados = []
        obj_norm = self.normalizar_texto(licitacao.get('obj', ''))
        
        termos_busca = [obj_norm]
        for termo_chave, sinonimos in self.sinonimos_farmacos.items():
            if termo_chave in obj_norm or any(s in obj_norm for s in sinonimos):
                termos_busca.extend(sinonimos)
                termos_busca.append(termo_chave)

        texto_consolidado = " ".join(termos_busca)

        for produto in self.portfolio_normalizado:
            # Token Set Ratio resolve variações de quantidade/texto extra
            score = fuzz.token_set_ratio(texto_consolidado, produto['descricao_norm']) / 100.0
            
            if score >= self.threshold_medio:
                tipo_match = "ALTO" if score >= self.threshold_alto else "MÉDIO"
                matches_encontrados.append({
                    'item_portfolio_id': produto['id'],
                    'descricao_portfolio': produto['descricao_original'],
                    'score': score,
                    'tipo_match': tipo_match
                })
        
        if matches_encontrados:
            return {
                'licitacao_id': f"{str(licitacao.get('id', ''))}_{str(licitacao.get('edit', ''))}",
                'matches': sorted(matches_encontrados, key=lambda x: x['score'], reverse=True)[:5]
            }
        return None

    def executar(self):
        self.carregar_portfolio()
        
        if not os.path.exists(self.arq_licitacoes):
            logger.error(f"Ficheiro {self.arq_licitacoes} não encontrado. Execute o limpeza.py primeiro.")
            sys.exit(1)

        logger.info("A iniciar avaliação de oportunidades (Fuzzy Matching)...")
        with gzip.open(self.arq_licitacoes, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)

        resultados_finais = []
        
        with ProcessPoolExecutor(max_workers=4) as executor:
            futuros = {executor.submit(self.avaliar_item_licitacao, lic): lic for lic in licitacoes}
            for future in as_completed(futuros):
                res = future.result()
                if res:
                    resultados_finais.append(res)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_saida = f"relatorio_compatibilidade_v3_{timestamp}.csv"
        
        with open(arquivo_saida, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['licitacao_id', 'item_portfolio_id', 'descricao_portfolio', 'score', 'tipo_match'])
            
            for resultado in resultados_finais:
                for match in resultado['matches']:
                    writer.writerow([
                        resultado['licitacao_id'],
                        match['item_portfolio_id'],
                        match['descricao_portfolio'],
                        f"{match['score']:.2%}",
                        match['tipo_match']
                    ])

        # Criação da cópia fixa requerida pelo Front-end (index.html)
        shutil.copyfile(arquivo_saida, "compatibilidade_latest.csv")

        logger.info(f"✅ Avaliação concluída. Encontrados matches para {len(resultados_finais)} licitações.")
        logger.info(f"📊 Relatório gerado: {arquivo_saida}")
        logger.info("📄 Cópia estática (compatibilidade_latest.csv) criada para o Painel Web.")

if __name__ == '__main__':
    avaliador = AvaliadorPortfolio()
    avaliador.executar()
