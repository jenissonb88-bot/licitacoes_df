import requests
import pandas as pd
import json
import os
from datetime import datetime

# CONFIGURAÇÕES ANALÍTICAS 2026
DATA_CORTE = "20260101"
# Endpoint corrigido conforme Manual de Integração Jan/2026
URL_API = "https://pncp.gov.br/api/pncp/v1/consultas/publicacoes/licitacoes"
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene"]

def extrair_portfolio():
    try:
        # Lendo o CSV com encoding comum em arquivos exportados do Excel
        df = pd.read_csv('Exportar Dados.csv', sep=',', encoding='utf-8')
        if 'Descrição' in df.columns:
            palavras = df['Descrição'].dropna().str.split().str[0].unique().tolist()
            return [str(p).lower() for p in palavras if len(str(p)) > 2]
        return []
    except Exception as e:
        print(f"Aviso: Erro ao ler CSV (tentando latin-1): {e}")
        try:
            df = pd.read_csv('Exportar Dados.csv', sep=',', encoding='latin-1')
            palavras = df['Descrição'].dropna().str.split().str[0].unique().tolist()
            return [str(p).lower() for p in palavras if len(str(p)) > 2]
        except:
            return []

def buscar_oportunidades():
    print(f"Iniciando varredura PNCP a partir de {DATA_CORTE}...")
    portfolio = extrair_portfolio()
    
    # Parâmetros ajustados para o endpoint de publicações
    params = {
        "dataInicial": DATA_CORTE,
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": "5", # Pregão
        "pagina": 1,
        "tamanhoPagina": 100
    }

    try:
        response = requests.get(URL_API, params=params, timeout=30)
        # Se o erro 404 persistir, o PNCP pode exigir a consulta sem o código da modalidade primeiro
        if response.status_code == 404:
            print("Tentando endpoint alternativo...")
            alt_url = "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes"
            response = requests.get(alt_url, params=params, timeout=30)

        response.raise_for_status()
        dados_api = response.json().get('data', [])
        
        resultados = []
        for item in dados_api:
            objeto = item.get('objeto', '').lower()
            
            # TRIAGEM TÉCNICA
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

        os.makedirs('dados', exist_ok=True)
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)
        
        print(f"Varredura finalizada: {len(resultados)} oportunidades encontradas.")

    except Exception as e:
        print(f"Erro na varredura: {e}")

if __name__ == "__main__":
    buscar_oportunidades()
