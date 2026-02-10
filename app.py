import requests
import pandas as pd
import json
import os
from datetime import datetime

# CONFIGURAÇÕES SEGUNDO MANUAL 2.3.9
# O Pregão na Nova Lei (14.133) é obrigatoriamente código 6
MODALIDADE_ALVO = "6" 
DATA_INICIO = "2026-01-01" # Formato ISO costuma ser mais aceito em APIs REST
URL_BASE = "https://pncp.gov.br/api/pncp/v1/licitacoes"

# Termos para triagem automática (Saúde e Higiene)
TERMOS_SAUDE = ["medicamento", "hospitalar", "saude", "farmacia", "insumos", "higiene", "medico", "soro"]

def extrair_keywords_csv():
    try:
        # Tenta carregar o portfólio para triagem refinada
        df = pd.read_csv('Exportar Dados.csv', encoding='utf-8')
        keywords = df['Descrição'].dropna().str.split().str[0].unique().tolist()
        return [str(k).lower() for k in keywords if len(str(k)) > 2]
    except:
        return []

def realizar_coleta():
    print(f"Iniciando varredura PNCP (Modalidade: {MODALIDADE_ALVO}) desde {DATA_INICIO}...")
    keywords_portfolio = extrair_keywords_csv()
    
    # Parâmetros conforme Manual de Integração 2.3.9
    params = {
        "dataInicial": "20260101", # Formato AAAAMMDD para a consulta
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": MODALIDADE_ALVO,
        "pagina": 1,
        "tamanhoPagina": 50
    }

    headers = {
        "accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        # Tenta o endpoint principal de licitações
        response = requests.get(URL_BASE, params=params, headers=headers, timeout=30)
        
        # Se 404, tenta o endpoint de consultas (fallback)
        if response.status_code == 404:
            url_fallback = "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes"
            print(f"Endpoint principal 404. Tentando fallback: {url_fallback}")
            response = requests.get(url_fallback, params=params, headers=headers, timeout=30)

        response.raise_for_status()
        dados = response.json()
        
        # Algumas versões da API retornam os dados em 'data', outras diretamente na lista
        lista_editais = dados.get('data', dados) if isinstance(dados, dict) else dados
        
        resultados = []
        for edital in lista_editais:
            objeto = edital.get('objeto', '').lower()
            
            # Triagem Técnica: Saúde ou Itens do Portfólio
            match_saude = any(t in objeto for t in TERMOS_SAUDE)
            match_port = any(k in objeto for k in keywords_portfolio)
            
            if match_saude or match_port:
                resultados.append({
                    "uf": edital.get('orgaoEntidade', {}).get('unidadeFederativaId'),
                    "orgao": edital.get('orgaoEntidade', {}).get('razaoSocial'),
                    "numero": edital.get('numeroSequencial'),
                    "ano": edital.get('ano'),
                    "objeto": edital.get('objeto'),
                    "link": f"https://pncp.gov.br/app/editais/{edital.get('orgaoEntidade', {}).get('cnpj')}/{edital.get('ano')}/{edital.get('numeroSequencial')}",
                    "modalidade": "Pregão (6)"
                })

        # Salva o resultado para o frontend
        os.makedirs('dados', exist_ok=True)
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)
        
        print(f"Varredura concluída. Encontradas {len(resultados)} oportunidades pertinentes.")

    except Exception as e:
        print(f"Erro na execução: {e}")

if __name__ == "__main__":
    realizar_coleta()
