import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

# Filtro Geográfico: Nordeste, Sudeste, Centro-Oeste + selecionados do Norte
ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]

# Termos de Saúde (Ampliados para maior captura)
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "medico", "insumo", "soro", "gaze", "seringa", "luva", "reagente", "odontolog", "laborator", "higiene", "cirurgico"]

# Blacklist (O que descartar)
BLACKLIST = ["computador", "notebook", "pneu", "veiculo", "obra", "engenharia", "pavimentacao", "ar condicionado", "mobiliario"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def criar_sessao():
    session = requests.Session()
    session.verify = False
    # Estratégia de re-tentativa para evitar quedas de conexão
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update({'User-Agent': 'MonitorSaude/5.5', 'Accept': 'application/json'})
    return session

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                content = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                return json.loads(content)
        except: pass
    return []

def salvar_dados_js(lista_dados):
    # Limpeza: mantém apenas os últimos 180 dias
    data_limite = datetime.now() - timedelta(days=180)
    lista_dados = [i for i in lista_dados if datetime.fromisoformat(i['data_pub'].split('T')[0]) > data_limite]
    
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json_str};")

def main():
    session = criar_sessao()
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')
    else:
        data_atual = DATA_INICIO_VARREDURA

    hoje = datetime.now()
    if data_atual.date() > hoje.date():
        print("✅ Sistema 100% atualizado.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🔎 ANALISANDO: {data_atual.strftime('%d/%m/%Y')}...")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": ds, "dataFinal": ds, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 100}

    banco = carregar_banco()
    novos = 0
    
    try:
        # Timeout estendido para 60 segundos
        resp = session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        
        licitacoes = resp.json().get('data', [])
        for item in licitacoes:
            uf = item.get('unidadeFederativaId') or item.get('unidadeOrgao', {}).get('ufSigla')
            obj = (item.get('objetoCompra') or "").lower()
            
            if uf in ESTADOS_ALVO and any(t in obj for t in TERMOS_SAUDE) and not any(b in obj for b in BLACKLIST):
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
                        "data_abertura": item.get('dataAberturaProposta'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}",
                        "link_pncp": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1

        salvar_dados_js(banco)
        proximo = data_atual + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        with open('env.txt', 'w') as f: f.write(f"CONTINUAR_EXECUCAO={'true' if proximo.date() <= hoje.date() else 'false'}")
        print(f"✅ Sucesso. Itens novos: {novos}. Próximo: {proximo.strftime('%d/%m/%Y')}")

    except Exception as e:
        print(f"💥 Erro na conexão: {e}. O checkpoint não foi avançado para tentar novamente.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")

if __name__ == "__main__":
    main()
