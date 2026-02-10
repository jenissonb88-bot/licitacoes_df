import requests
import json
from datetime import datetime, timedelta
import os
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import re

# --- CONFIGURAÇÕES ANALISTA DE LICITAÇÕES ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) # Início do ano fiscal
ARQ_DADOS = 'dados/oportunidades.json' # Caminho para o frontend ler
ARQ_CHECKPOINT = 'checkpoint.txt'
TEMPO_LIMITE_SEGURO = 19800  # 5.5 horas (segurança para GitHub Actions)
MODALIDADE_PREGAO = "6"

# Termos de triagem (Regra de Negócio)
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "insumos", "saude", "higiene", "medico", "soro", "gaze", "luva", "enfermagem"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'MonitorLicita/2.0 (Analista Saude)'
}

INICIO_EXECUCAO = time.time()

# -------------------------------------------------
# 1. FUNÇÕES AUXILIARES E CARGA DE DADOS
# -------------------------------------------------

def carregar_portfolio():
    """Lê o CSV para enriquecer a busca com nomes comerciais"""
    try:
        # Tenta utf-8 e depois latin-1
        try:
            df = pd.read_csv('Exportar Dados.csv', encoding='utf-8', sep=',')
        except:
            df = pd.read_csv('Exportar Dados.csv', encoding='latin-1', sep=',')
        
        if 'Descrição' in df.columns:
            return df['Descrição'].dropna().str.split().str[0].unique().tolist()
    except:
        pass
    return []

def carregar_banco():
    """Carrega dados existentes para evitar duplicidade"""
    os.makedirs('dados', exist_ok=True)
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Erro ao carregar banco: {e}")
    return []

def salvar_estado(lista_dados, proximo_dia):
    """Persiste os dados e o checkpoint"""
    # Ordena por data de publicação (mais recente primeiro)
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_dados, f, indent=4, ensure_ascii=False)
    
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))
        
    print(f" 💾 [Salvo! Checkpoint: {proximo_dia.strftime('%d/%m')}]", end="", flush=True)

# -------------------------------------------------
# 2. CORE DA CONEXÃO
# -------------------------------------------------

def criar_sessao():
    """Cria uma sessão HTTP resiliente a falhas"""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False # Ignora erro de certificado do governo
    # Configura retentativas automáticas para erros 500, 502, 503, 504
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def processar_dia(session, lista_atual, data_analise, portfolio):
    DATA_STR = data_analise.strftime('%Y%m%d')
    print(f"\n📅 Varrendo {data_analise.strftime('%d/%m/%Y')}...", end=" ", flush=True)
    
    pagina = 1
    novos_itens = 0
    
    # URL CORRETA DA API DE CONSULTA
    url_base = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

    while True:
        params = {
            "dataInicial": DATA_STR, 
            "dataFinal": DATA_STR, 
            "codigoModalidadeContratacao": MODALIDADE_PREGAO, 
            "pagina": pagina, 
            "tamanhoPagina": 50
        }

        try:
            resp = session.get(url_base, params=params, timeout=30)
            if resp.status_code != 200: 
                print(f"[HTTP {resp.status_code}]", end="")
                break
                
            dados_json = resp.json()
            licitacoes = dados_json.get('data', [])
            
            if not licitacoes: break # Fim das páginas

            for item in licitacoes:
                # LÓGICA DE TRIAGEM (FILTRO)
                objeto = (item.get('objetoCompra') or "").lower()
                
                match_saude = any(t in objeto for t in TERMOS_SAUDE)
                match_port = any(p.lower() in objeto for p in portfolio)

                if match_saude or match_port:
                    # Cria ID único para não duplicar no JSON
                    id_unico = str(item.get('id')) 
                    
                    # Verifica se já existe na lista
                    if not any(x['id'] == id_unico for x in lista_atual):
                        nova_oportunidade = {
                            "id": id_unico,
                            "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                            "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                            "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                            "uf": item.get('unidadeFederativaId'),
                            "objeto": item.get('objetoCompra'),
                            "modalidade": "Pregão (6)",
                            "data_pub": item.get('dataPublicacaoPncp'),
                            "valor_total": item.get('valorTotalEstimado', 0),
                            "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                        }
                        lista_atual.append(nova_oportunidade)
                        novos_itens += 1
                        print(".", end="", flush=True)

            # Controle de paginação
            total_paginas = dados_json.get('totalPaginas', 1)
            if pagina >= total_paginas: break
            pagina += 1
            
        except Exception as e:
            print(f"[Erro: {e}]", end="")
            break
            
    if novos_itens > 0:
        print(f" (+{novos_itens} opps)", end="")
    else:
        print("(0)", end="")

# -------------------------------------------------
# 3. LOOP PRINCIPAL
# -------------------------------------------------

def ler_checkpoint():
    """Recupera a última data processada ou começa de 2026"""
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                dt_str = f.read().strip()
                return datetime.strptime(dt_str, '%Y%m%d')
        except: pass
    return DATA_INICIO_VARREDURA

def main():
    print(f"--- 🚀 MONITOR DE LICITAÇÕES SAÚDE (PNCP) ---")
    
    session = criar_sessao()
    banco_dados = carregar_banco()
    portfolio = carregar_portfolio()
    
    data_atual = ler_checkpoint()
    hoje = datetime.now()
    
    # Garante que não ultrapasse o dia de hoje
    if data_atual > hoje: data_atual = hoje

    while data_atual.date() <= hoje.date():
        processar_dia(session, banco_dados, data_atual, portfolio)
        
        # Avança para o próximo dia
        data_proxima = data_atual + timedelta(days=1)
        salvar_estado(banco_dados, data_proxima)
        data_atual = data_proxima
        
        # Proteção para o GitHub Actions não matar o processo abruptamente
        if (time.time() - INICIO_EXECUCAO) > TEMPO_LIMITE_SEGURO:
            print(f"\n⚠️ TEMPO LIMITE DE EXECUÇÃO. Pausando...")
            break

    print("\n🏁 Varredura finalizada.")

if __name__ == "__main__":
    main()
