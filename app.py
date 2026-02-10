import requests
import pandas as pd
import json
import os
from datetime import datetime

# Configurações de Triagem
DATA_INICIAL = "20260101"
TERMOS_CHAVE = ["medicamento", "hospitalar", "saude", "farmacia", "insumos", "correlatos"]

def carregar_portfolio():
    """Lê o CSV de produtos para usar nomes como filtro adicional"""
    try:
        df = pd.read_csv('Exportar Dados.csv')
        # Extrai nomes únicos de produtos (primeiras palavras da descrição)
        nomes_produtos = df['Descrição'].str.split().str[0].unique().tolist()
        return [nome.lower() for nome in nomes_produtos if len(nome) > 3]
    except:
        return []

def buscar_licitacoes_pncp():
    url = "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes"
    portfolio = carregar_portfolio()
    
    oportunidades_filtradas = []
    
    # Parâmetros conforme Manual PNCP V1
    params = {
        "dataInicial": DATA_INICIAL,
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": "5", # Pregão Eletrônico
        "pagina": 1,
        "tamanhoPagina": 50
    }

    print(f"Iniciando triagem técnica no PNCP desde {DATA_INICIAL}...")
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            dados = response.json()
            items = dados.get('data', [])
            
            for item in items:
                objeto = item.get('objeto', '').lower()
                
                # Critério de Triagem: Contém termos de saúde ou itens do portfólio?
                match_termo = any(termo in objeto for termo in TERMOS_CHAVE)
                match_portfolio = any(prod in objeto for prod in portfolio)
                
                if match_termo or match_portfolio:
                    oportunidade = {
                        "numero": item.get('numeroSequencial'),
                        "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                        "uf": item.get('orgaoEntidade', {}).get('unidadeFederativaId'),
                        "objeto": item.get('objeto'),
                        "data_publicacao": item.get('dataPublicacaoPncp'),
                        "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('ano')}/{item.get('numeroSequencial')}"
                    }
                    oportunidades_filtradas.append(oportunidade)
        
        # Garante que a pasta dados existe
        os.makedirs('dados', exist_ok=True)
        
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(oportunidades_filtradas, f, ensure_ascii=False, indent=4)
            
        print(f"Triagem concluída. {len(oportunidades_filtradas)} oportunidades relevantes encontradas.")

    except Exception as e:
        print(f"Erro na conexão com API PNCP: {e}")

if __name__ == "__main__":
    buscar_licitacoes_pncp()
