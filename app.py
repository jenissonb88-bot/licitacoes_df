import requests
import pandas as pd
import json
import os
from datetime import datetime

# CONFIGURAÇÕES TÉCNICAS (LEI 14.133/2021)
DATA_CORTE = "20260101"
MODALIDADE_PREGAO = "6"  # Código correto conforme Manual PNCP
URL_API = "https://pncp.gov.br/api/pncp/v1/consultas/licitacoes"

TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene", "medico"]

def extrair_portfolio():
    try:
        df = pd.read_csv('Exportar Dados.csv', encoding='utf-8')
        palavras = df['Descrição'].dropna().str.split().str[0].unique().tolist()
        return [str(p).lower() for p in palavras if len(str(p)) > 2]
    except:
        try:
            df = pd.read_csv('Exportar Dados.csv', encoding='latin-1')
            palavras = df['Descrição'].dropna().str.split().str[0].unique().tolist()
            return [str(p).lower() for p in palavras if len(str(p)) > 2]
        except:
            return []

def buscar_oportunidades():
    print(f"Varredura PNCP: Modalidade {MODALIDADE_PREGAO} (Pregão) desde {DATA_CORTE}...")
    portfolio = extrair_portfolio()
    
    # Parâmetros exatos para evitar erro 404 e filtrar por Pregão (6)
    params = {
        "dataInicial": DATA_CORTE,
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": MODALIDADE_PREGAO,
        "pagina": 1,
        "tamanhoPagina": 100
    }

    try:
        response = requests.get(URL_API, params=params, timeout=30)
        
        # Se a consulta direta por modalidade falhar, buscamos geral e filtramos no código
        if response.status_code != 200:
            print("Ajustando busca para endpoint de contratações...")
            url_alt = "https://pncp.gov.br/api/pncp/v1/consultas/contratacoes"
            response = requests.get(url_alt, params={"dataInicial": DATA_CORTE, "pagina": 1}, timeout=30)

        response.raise_for_status()
        dados_brutos = response.json()
        itens = dados_brutos.get('data', [])
        
        filtrados = []
        for item in itens:
            objeto = item.get('objeto', '').lower()
            # Validação dupla da modalidade (API ou Campo Interno)
            cod_mod = str(item.get('codigoModalidade', item.get('modalidadeId', '')))
            
            if cod_mod == MODALIDADE_PREGAO:
                is_saude = any(t in objeto for t in TERMOS_SAUDE) or any(p in objeto for p in portfolio)
                
                if is_saude:
                    filtrados.append({
                        "numero": item.get('numeroSequencial'),
                        "ano": item.get('ano'),
                        "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                        "uf": item.get('orgaoEntidade', {}).get('unidadeFederativaId'),
                        "objeto": item.get('objeto'),
                        "data": item.get('dataPublicacaoPncp'),
                        "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('ano')}/{item.get('numeroSequencial')}"
                    })

        os.makedirs('dados', exist_ok=True)
        with open('dados/oportunidades.json', 'w', encoding='utf-8') as f:
            json.dump(filtrados, f, ensure_ascii=False, indent=4)
        
        print(f"Sucesso: {len(filtrados)} pregões de saúde encontrados.")

    except Exception as e:
        print(f"Erro na varredura: {e}")

if __name__ == "__main__":
    buscar_oportunidades()
