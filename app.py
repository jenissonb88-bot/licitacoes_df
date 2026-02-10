import requests
import pandas as pd
import json
import os
from datetime import datetime

# CONFIGURAÇÕES ANALÍTICAS
DATA_CORTE = "20260101"
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "material medico"]

def extrair_portfolio():
    """Extrai palavras-chave do arquivo CSV enviado"""
    try:
        # Lendo o CSV ignorando a primeira coluna vazia se existir
        df = pd.read_csv('Exportar Dados.csv')
        # Pega a primeira palavra da 'Descrição' (Ex: AAS, ABIRATERONA)
        palavras = df['Descrição'].str.split().str[0].unique().tolist()
        return [str(p).lower() for p in palavras if len(str(p)) > 2]
    except Exception as e:
        print(f"Aviso: Erro ao ler portfólio CSV: {e}")
        return []

def buscar_oportunidades():
    print(f"Iniciando varredura PNCP a partir de {DATA_CORTE}...")
    url = "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes"
    portfolio = extrair_portfolio()
    
    params = {
        "dataInicial": DATA_CORTE,
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": "5", # Pregão Eletrônico
        "pagina": 1,
        "tamanhoPagina": 100
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        dados_api = response.json().get('data', [])
        
        resultados = []
        for item in dados_api:
            objeto = item.get('objeto', '').lower()
            
            # CRITÉRIO DE TRIAGEM: Termos gerais de saúde OU produtos do portfólio
            match_saude = any(t in objeto for t in TERMOS_SAUDE)
            match_prod = any(p in objeto for p in portfolio)
            
            if match_saude or match_prod:
                resultados.append({
                    "numero": item.get('numeroSequencial'),
                    "ano": item.get('ano'),
                    "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                    "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                    "uf": item.get('orgaoEntidade', {}).get('unidadeFederativaId'),
                    "objeto": item.get('objeto'),
                    "data": item.get('dataPublicacaoPncp'),
                    "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('ano')}/{item.get('numeroSequencial')}"
                })

        # Salva o resultado para o Frontend
        os.makedirs('dados', exist_ok=True)
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)
        
        print(f"Varredura finalizada: {len(resultados)} oportunidades encontradas.")

    except Exception as e:
        print(f"Erro crítico na varredura: {e}")

if __name__ == "__main__":
    buscar_oportunidades()
