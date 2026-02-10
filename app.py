import requests
import pandas as pd
import json
import os
from datetime import datetime

# CONFIGURAÇÕES TÉCNICAS (LEI 14.133/2021)
DATA_CORTE = "20260101"
MODALIDADE_PREGAO = "6"  # PREGÃO conforme Manual PNCP V1

# Lista de termos para triagem automática (Saúde/Higiene)
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene", "medico", "soro", "gaze", "luva"]

def extrair_portfolio():
    try:
        # Tenta carregar do CSV enviado pelo usuário
        df = pd.read_csv('Exportar Dados.csv', encoding='utf-8')
        palavras = df['Descrição'].dropna().str.split().str[0].unique().tolist()
        return [str(p).lower() for p in palavras if len(str(p)) > 2]
    except:
        return []

def buscar_oportunidades():
    print(f"Varredura PNCP: Modalidade {MODALIDADE_PREGAO} desde {DATA_CORTE}...")
    portfolio = extrair_portfolio()
    
    # Endpoints possíveis para contornar o erro 404
    endpoints = [
        "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes",
        "https://pncp.gov.br/api/pncp/v1/consultas/publicacoes/licitacoes"
    ]
    
    sucesso = False
    resultados = []

    for url in endpoints:
        if sucesso: break
        
        params = {
            "dataInicial": DATA_CORTE,
            "dataFinal": datetime.now().strftime("%Y%m%d"),
            "codigoModalidade": MODALIDADE_PREGAO,
            "pagina": 1,
            "tamanhoPagina": 100
        }

        try:
            print(f"Tentando endpoint: {url}")
            response = requests.get(url, params=params, timeout=20)
            
            if response.status_code == 200:
                dados_api = response.json()
                itens = dados_api.get('data', [])
                
                for item in itens:
                    objeto = item.get('objeto', '').lower()
                    
                    # TRIAGEM: Termos de Saúde OR Itens do CSV
                    match_saude = any(t in objeto for t in TERMOS_SAUDE)
                    match_prod = any(p in objeto for p in portfolio)
                    
                    if match_saude or match_prod:
                        resultados.append({
                            "numero": item.get('numeroSequencial'),
                            "ano": item.get('ano'),
                            "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                            "uf": item.get('orgaoEntidade', {}).get('unidadeFederativaId'),
                            "objeto": item.get('objeto'),
                            "data": item.get('dataPublicacaoPncp'),
                            "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('ano')}/{item.get('numeroSequencial')}"
                        })
                sucesso = True
            else:
                print(f"Status {response.status_code} para {url}")
        except Exception as e:
            print(f"Erro ao conectar em {url}: {e}")

    # Salva o arquivo JSON para o Index.html
    os.makedirs('dados', exist_ok=True)
    with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=4)
    
    print(f"Finalizado. {len(resultados)} pregões de saúde encontrados.")

if __name__ == "__main__":
    buscar_oportunidades()
