import requests
import json
from datetime import datetime, timedelta
import os
import urllib3
import pandas as pd

# --- CONFIGURAÇÕES ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]

# Termos de Saúde Ampliados
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "medico", "insumo", "soro", "gaze", "seringa", "luva", "reagente", "saude", "higiene"]

# Blacklist Rígida
BLACKLIST = ["computador", "notebook", "pneu", "veiculo", "obra", "engenharia", "pavimentacao", "mobiliario"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def carregar_portfolio():
    """Extrai nomes de remédios do seu arquivo CSV 'Exportar Dados'"""
    try:
        df = pd.read_csv('Exportar Dados.xls - Exportar Dados.csv')
        # Pega a primeira palavra da descrição (ex: 'AAS', 'ABIRATERONA')
        return df['Descrição'].str.split().str[0].str.lower().unique().tolist()
    except: return []

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                content = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                return json.loads(content)
        except: pass
    return []

def salvar_dados_js(lista_dados):
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json_str};")

def main():
    session = requests.Session()
    session.verify = False
    
    portfolio = carregar_portfolio()
    termos_busca = list(set(TERMOS_SAUDE + portfolio))
    
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')
    else:
        data_atual = DATA_INICIO_VARREDURA

    hoje = datetime.now()
    if data_atual.date() > hoje.date():
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🔎 ANALISANDO: {data_atual.strftime('%d/%m/%Y')}...")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": ds, "dataFinal": ds, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50}

    banco = carregar_banco()
    novos = 0
    
    try:
        resp = session.get(url, params=params, timeout=60)
        licitacoes = resp.json().get('data', [])
        
        for item in licitacoes:
            # CORREÇÃO DA UF: Tenta pegar de dois lugares na API
            uf = item.get('unidadeFederativaId') or item.get('unidadeOrgao', {}).get('ufSigla')
            obj = (item.get('objetoCompra') or "").lower()
            
            if uf in ESTADOS_ALVO and any(t in obj for t in termos_busca) and not any(b in obj for b in BLACKLIST):
                id_u = str(item.get('id'))
                if not any(x['id'] == id_u for x in banco):
                    unidade = item.get('unidadeOrgao', {})
                    banco.append({
                        "id": id_u,
                        "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                        "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                        "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                        "unidade_compradora": unidade.get('nomeUnidade'),
                        "uasg": unidade.get('codigoUnidade'),
                        "uf": uf,
                        "cidade": unidade.get('municipioNome'),
                        "objeto": item.get('objetoCompra'),
                        "quantidade_itens": item.get('quantidadeItens', 0),
                        "data_pub": item.get('dataPublicacaoPncp'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1
                    print(f"   ✅ CAPTURADO: {obj[:50]}...")

        salvar_dados_js(banco)
        proximo = data_atual + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        with open('env.txt', 'w') as f: f.write(f"CONTINUAR_EXECUCAO={'true' if proximo.date() <= hoje.date() else 'false'}")
        print(f"📊 Fim do dia. Novos: {novos}. Próximo: {proximo.strftime('%d/%m/%Y')}")

    except Exception as e:
        print(f"💥 Erro: {e}")

if __name__ == "__main__":
    main()
