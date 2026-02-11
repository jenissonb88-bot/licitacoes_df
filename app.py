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
ARQ_DADOS = 'dados/oportunidades.js' # Gerando .js para evitar erro de CORS
MODALIDADE_PREGAO = "6"

# 1. FILTRO GEOGRÁFICO (WhiteList conforme solicitado)
ESTADOS_ALVO = [
    "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", # Nordeste
    "ES", "MG", "RJ", "SP",                               # Sudeste
    "AM", "PA", "TO", "DF", "GO", "MT", "MS"              # Norte/Centro-Oeste selecionados
]

# 2. TERMOS DE INTERESSE (Saúde)
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "material medico", "enfermagem", "soro", "gaze", "seringa", "agulha", "reagente"]

# 3. BLACKLIST (Filtro de Lixo)
BLACKLIST = ["computador", "notebook", "software", "predial", "pintura", "engenharia", "obra", "alimento", "ovo", "carne", "veiculo"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def criar_sessao():
    session = requests.Session()
    session.headers.update({'Accept': 'application/json', 'User-Agent': 'MonitorPNCP/5.0'})
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session

def salvar_dados_js(lista_dados):
    """ Salva os dados como uma variável JavaScript """
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    
    # Limpeza opcional: manter apenas últimos 180 dias
    data_limite = datetime.now() - timedelta(days=180)
    lista_dados = [i for i in lista_dados if datetime.fromisoformat(i['data_pub'].split('T')[0]) > data_limite]

    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    conteudo_js = f"const dadosLicitacoes = {json_str};"
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(conteudo_js)
    print(f"💾 Sucesso! {len(lista_dados)} licitações salvas em {ARQ_DADOS}")

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
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}",
                        "link_pncp": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
    except Exception as e: print(f"Erro: {e}")
    return lista_atual

def main():
    session = criar_sessao()
    # Tenta carregar dados existentes para não perder histórico
    banco = []
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                content = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                banco = json.loads(content)
        except: pass

    # Inicia coleta do dia atual (ou checkpoint)
    print("🔎 Iniciando varredura...")
    banco = processar_um_dia(session, banco, datetime.now()) # Pode usar data fixa para testes
    salvar_dados_js(banco)

if __name__ == "__main__":
    main()
