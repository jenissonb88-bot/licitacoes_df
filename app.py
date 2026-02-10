import requests
import pandas as pd
import json
import os
from datetime import datetime

# ==========================================
# CONFIGURAÇÕES DA API DE CONSULTA (LEI 14.133)
# ==========================================
# Endpoint correto para BUSCA (Não usar /api/pncp/, usar /api/consulta/)
URL_BUSCA = "https://pncp.gov.br/api/consulta/v1/contratacoes"
DATA_INICIO = "20260101"
MODALIDADE_PREGAO = "6"

# Termos de triagem
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene", "medico", "soro", "gaze", "luva"]

def carregar_portfolio():
    """Lê o CSV local para enriquecer a triagem"""
    try:
        # Tenta ler com diferentes codificações para evitar erro
        try:
            df = pd.read_csv('Exportar Dados.csv', encoding='utf-8', sep=',')
        except:
            df = pd.read_csv('Exportar Dados.csv', encoding='latin-1', sep=',')
            
        if 'Descrição' in df.columns:
            # Pega a primeira palavra de cada produto (Ex: AAS, DIPIRONA)
            return df['Descrição'].dropna().str.split().str[0].unique().tolist()
    except Exception as e:
        print(f"Aviso: Não foi possível ler o CSV de portfólio ({e}). Usando apenas termos genéricos.")
    return []

def realizar_triagem():
    print(f"--- INICIANDO COLETA PNCP (CONSULTA PÚBLICA) ---")
    print(f"Filtros: A partir de {DATA_INICIO} | Modalidade: {MODALIDADE_PREGAO} (Pregão)")
    
    portfolio = carregar_portfolio()
    
    # Parâmetros oficiais da API de Consulta
    params = {
        "dataInicial": DATA_INICIO,
        "dataFinal": datetime.now().strftime("%Y%m%d"),
        "codigoModalidade": MODALIDADE_PREGAO,
        "pagina": 1,
        "tamanhoPagina": 50,
        "ordenacao": "-dataPublicacaoPncp" # Mais recentes primeiro
    }

    try:
        response = requests.get(URL_BUSCA, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"Erro na API: {response.status_code} - {response.text}")
            return

        dados_json = response.json()
        # A API de consulta retorna os itens dentro de 'data'
        licitacoes = dados_json.get('data', [])
        
        resultados = []
        
        for item in licitacoes:
            objeto = (item.get('objetoCompra') or "").lower()
            
            # 1. Triagem: Verifica se é Saúde (via termos ou portfolio)
            match_saude = any(t in objeto for t in TERMOS_SAUDE)
            match_port = any(p.lower() in objeto for p in portfolio)
            
            if match_saude or match_port:
                # Estrutura unificada para o Frontend
                resultados.append({
                    "id": item.get('id'), # ID global, essencial para buscar itens depois
                    "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                    "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                    "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                    "uf": item.get('unidadeFederativaId'),
                    "objeto": item.get('objetoCompra'),
                    "modalidade": item.get('modalidadeId'), # Deve ser 6
                    "data_pub": item.get('dataPublicacaoPncp'),
                    "valor_total": item.get('valorTotalEstimado', 0),
                    "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                })

        # Salva o JSON
        os.makedirs('dados', exist_ok=True)
        caminho_arquivo = 'dados/oportunidades.json'
        
        with open(caminho_arquivo, 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)
            
        print(f"Sucesso! {len(resultados)} pregões de saúde encontrados e salvos em {caminho_arquivo}.")

    except Exception as e:
        print(f"Erro crítico na execução: {e}")

if __name__ == "__main__":
    realizar_triagem()
