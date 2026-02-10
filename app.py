import requests
import pandas as pd
import json
import os
from datetime import datetime

# ==========================================
# CONFIGURAÇÕES DA API DE CONSULTA (Corrigido)
# ==========================================
# Endpoint Oficial para Buscas por Data (Swagger PNCP)
URL_BUSCA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
DATA_INICIO = "20260101"
MODALIDADE_PREGAO = "6"  # Pregão - Lei 14.133

# Termos para triagem
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene", "medico", "soro", "gaze", "luva"]

def carregar_portfolio():
    try:
        # Tenta ler com diferentes codificações
        try:
            df = pd.read_csv('Exportar Dados.csv', encoding='utf-8', sep=',')
        except:
            df = pd.read_csv('Exportar Dados.csv', encoding='latin-1', sep=',')
            
        if 'Descrição' in df.columns:
            return df['Descrição'].dropna().str.split().str[0].unique().tolist()
    except:
        pass
    return []

def realizar_triagem():
    print(f"--- INICIANDO COLETA (ENDPOINT PUBLICACAO) ---")
    print(f"Filtro: {DATA_INICIO} a Hoje | Modalidade: {MODALIDADE_PREGAO}")
    
    portfolio = carregar_portfolio()
    
    # Parâmetros CORRETOS conforme API de Consulta v1
    params = {
        "dataInicial": DATA_INICIO,
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidadeContratacao": MODALIDADE_PREGAO, # Nome do parâmetro corrigido
        "pagina": 1,
        "tamanhoPagina": 50
    }

    try:
        response = requests.get(URL_BUSCA, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"Erro {response.status_code}: {response.text}")
            return

        # A API de consulta/publicacao retorna lista direta em 'data'
        json_resp = response.json()
        licitacoes = json_resp.get('data', [])
        
        resultados = []
        
        for item in licitacoes:
            objeto = (item.get('objetoCompra') or "").lower()
            
            # Triagem
            match_saude = any(t in objeto for t in TERMOS_SAUDE)
            match_port = any(p.lower() in objeto for p in portfolio)
            
            if match_saude or match_port:
                resultados.append({
                    "id": item.get('id'), # Usado para buscar itens
                    "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                    "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                    "cnpj": item.get('orgaoEntidade', {}).get('cnpj'), # Importante para link
                    "uf": item.get('unidadeFederativaId'),
                    "objeto": item.get('objetoCompra'),
                    "data_pub": item.get('dataPublicacaoPncp'),
                    "valor_total": item.get('valorTotalEstimado', 0),
                    "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                })

        # Salva o arquivo
        os.makedirs('dados', exist_ok=True)
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)
            
        print(f"Sucesso! {len(resultados)} pregões encontrados.")

    except Exception as e:
        print(f"Erro na execução: {e}")

if __name__ == "__main__":
    realizar_triagem()
