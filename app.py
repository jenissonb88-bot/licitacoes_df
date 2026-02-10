import requests
import pandas as pd
import json
import os
from datetime import datetime

# CONFIGURAÇÕES ANALÍTICAS - LEI 14.133/2021
DATA_CORTE = "20260101"
MODALIDADE_PREGAO = "6" # Conforme lembrete: Pregão é 6
# URL base corrigida para o serviço de consulta de publicações v1
URL_API = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacoes"

TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene", "medico", "soro"]

def carregar_portfolio():
    try:
        df = pd.read_csv('Exportar Dados.csv', encoding='utf-8')
        return df['Descrição'].dropna().str.split().str[0].unique().tolist()
    except:
        return []

def buscar_licitacoes():
    print(f"Iniciando varredura técnica PNCP 2026 (Pregão 6)...")
    portfolio = carregar_portfolio()
    
    params = {
        "dataPublicacaoInicial": DATA_CORTE,
        "dataPublicacaoFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": MODALIDADE_PREGAO,
        "pagina": 1,
        "tamanhoPagina": 50
    }

    headers = {"accept": "*/*", "User-Agent": "ColetaPNCP/1.0"}

    try:
        response = requests.get(URL_API, params=params, headers=headers, timeout=30)
        
        # Fallback caso o endpoint de publicações mude
        if response.status_code == 404:
            print("Tentando endpoint alternativo de contratações...")
            alt_url = "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes"
            response = requests.get(alt_url, params=params, headers=headers, timeout=30)

        response.raise_for_status()
        dados = response.json().get('data', [])
        
        oportunidades = []
        for item in dados:
            # Triagem por texto
            txt = (item.get('objeto') or "").lower()
            match = any(t.lower() in txt for t in TERMOS_SAUDE) or any(p.lower() in txt for p in portfolio)
            
            if match:
                oportunidades.append({
                    "uf": item.get('unidadeFederativaId'),
                    "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                    "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                    "ano": item.get('ano'),
                    "sequencial": item.get('sequencial'),
                    "numero": item.get('numeroSequencial'),
                    "objeto": item.get('objeto'),
                    "data_pub": item.get('dataPublicacaoPncp'),
                    "situacao": item.get('situacaoNome'),
                    "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('ano')}/{item.get('sequencial')}"
                })

        os.makedirs('dados', exist_ok=True)
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(oportunidades, f, ensure_ascii=False, indent=4)
        
        print(f"Sucesso: {len(oportunidades)} licitações de saúde identificadas.")

    except Exception as e:
        print(f"Erro na coleta: {e}")

if __name__ == "__main__":
    buscar_licitacoes()
