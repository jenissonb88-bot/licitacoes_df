import requests
import json
from datetime import datetime, timedelta
import os
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# --- CONFIGURAÇÕES ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1)
ARQ_DADOS = 'dados/oportunidades.js' # Mudança para .js
ARQ_CHECKPOINT = 'checkpoint.txt'
MODALIDADE_PREGAO = "6"

ESTADOS_ALVO = [
    "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", # Nordeste
    "ES", "MG", "RJ", "SP",                               # Sudeste
    "AM", "PA", "TO", "DF", "GO", "MT", "MS"              # Norte/Centro-Oeste selecionados
]

TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "material medico", "enfermagem", "soro", "gaze", "higiene pessoal", "seringa", "reagente", "odontologic"]

BLACKLIST = ["computador", "notebook", "software", "predial", "pintura", "engenharia", "obra", "alimento", "ovo", "carne", "veiculo", "pneu"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def criar_sessao():
    session = requests.Session()
    session.headers.update({'Accept': 'application/json', 'User-Agent': 'MonitorHealth/5.0'})
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session

def salvar_dados_js(lista_dados):
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    conteudo_js = f"const dadosLicitacoes = {json_str};"
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(conteudo_js)
    print(f"💾 Dados exportados para {ARQ_DADOS}")

def processar_um_dia(session, lista_atual, data_analise):
    ds = data_analise.strftime('%Y%m%d')
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": ds, "dataFinal": ds, "codigoModalidadeContratacao": MODALIDADE_PREGAO, "pagina": 1, "tamanhoPagina": 50}

    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200: return lista_atual
        
        licitacoes = resp.json().get('data', [])
        for item in licitacoes:
            uf = item.get('unidadeFederativaId')
            obj = (item.get('objetoCompra') or "").lower()

            if uf in ESTADOS_ALVO and any(t in obj for t in TERMOS_SAUDE) and not any(b in obj for b in BLACKLIST):
                id_u = str(item.get('id'))
                if not any(x['id'] == id_u for x in lista_atual):
                    unidade = item.get('unidadeOrgao', {})
                    lista_atual.append({
                        "id": id_u,
                        "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                        "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                        "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                        "unidade_compradora": unidade.get('nomeUnidade'),
                        "uasg": unidade.get('codigoUnidade'),
                        "uf": uf,
                        "cidade": unidade.get('municipioNome'),
                        "objeto": item.get('objetoCompra'),
                        "data_pub": item.get('dataPublicacaoPncp'),
                        "data_abertura": item.get('dataAberturaProposta'),
                        "data_atualizacao": item.get('dataAtualizacao'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
    except Exception as e: print(f"Erro: {e}")
    return lista_atual

def main():
    session = criar_sessao()
    if os.path.exists('dados/oportunidades.json'): # Migração se existir antigo
         with open('dados/oportunidades.json', 'r', encoding='utf-8') as f: banco = json.load(f)
    else: banco = []
    
    # Simulação de processamento (ajuste para ler checkpoint se desejar)
    data_alvo = DATA_INICIO_VARREDURA 
    banco = processar_um_dia(session, banco, data_alvo)
    salvar_dados_js(banco)

if __name__ == "__main__":
    main()
