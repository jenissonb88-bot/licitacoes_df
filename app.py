import requests
import json
from datetime import datetime, timedelta
import os
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# --- CONFIGURAÇÕES ANALISTA ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1)
ARQ_DADOS = 'dados/oportunidades.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
TEMPO_LIMITE_SEGURO = 19800 
MODALIDADE_PREGAO = "6"

# 1. TERMOS DE SAÚDE (O que queremos)
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "farmacia", "farmaceutic", 
    "material medico", "enfermagem", "soro", "gaze", "luva cirurgica", 
    "higiene pessoal", "fralda", "cateter", "seringa", "agulha",
    "fios de sutura", "atadura", "algodao", "esparadrapo"
]

# 2. BLACKLIST (O que NÃO queremos - Baseado nos seus exemplos)
BLACKLIST = [
    # TI e Eletrônicos
    "computador", "desktop", "notebook", "tablet", "monitor", "impressora",
    "toner", "cartucho", "software", "saas", "inteligencia artificial",
    "identificador facial", "automatizado", "informatica", "teclado", "mouse",
    "nobreak", "estabilizador", "servidor", "rede", "cabo de rede",
    
    # Manutenção Predial e Obras
    "predial", "manutencao preventiva", "manutencao corretiva", "ar condicionado",
    "eletrica", "hidraulica", "pintura", "alvenaria", "engenharia", "obra",
    "ferramenta", "extintor", "elevador", "jardinagem", "poda", "roçada",
    "mobiliario", "moveis", "cadeira", "mesa", "armario", "divisoria",
    
    # Alimentação
    "genero alimenticio", "alimentacao", "hortifrutigranjeiro", "ovo", "carne",
    "frango", "peixe", "leite", "cafe", "acucar", "lanche", "refeicao",
    "coffee break", "buffet", "agua mineral", "cantina", "cozinha",
    
    # Educação e Outros
    "aula pratica", "curso tecnico", "quimica industrial", "didatico",
    "pedagogico", "brinquedo", "esportiv", "musical", "automotiv",
    "veiculo", "pneu", "combustivel", "lubrificante", "transporte",
    "grafica", "banner", "panfleto", "publicidade", "evento"
]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'MonitorLicita/2.2 (HealthFilter)'
}

INICIO_EXECUCAO = time.time()

# -------------------------------------------------
# FUNÇÕES
# -------------------------------------------------

def carregar_portfolio():
    try:
        try:
            df = pd.read_csv('Exportar Dados.csv', encoding='utf-8', sep=',')
        except:
            df = pd.read_csv('Exportar Dados.csv', encoding='latin-1', sep=',')
        
        if 'Descrição' in df.columns:
            # Filtra palavras muito curtas para evitar falso positivo (ex: "DE", "EM")
            raw_list = df['Descrição'].dropna().str.split().str[0].unique().tolist()
            return [str(x).lower() for x in raw_list if len(str(x)) > 3]
    except:
        pass
    return []

def carregar_banco():
    os.makedirs('dados', exist_ok=True)
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return []

def salvar_estado(lista_dados, proximo_dia):
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_dados, f, indent=4, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))
    print(f" 💾 [Salvo! Checkpoint: {proximo_dia.strftime('%d/%m')}]", end="", flush=True)

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def processar_dia(session, lista_atual, data_analise, portfolio):
    DATA_STR = data_analise.strftime('%Y%m%d')
    print(f"\n📅 Varrendo {data_analise.strftime('%d/%m/%Y')}...", end=" ", flush=True)
    
    pagina = 1
    novos_itens = 0
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
            if resp.status_code != 200: break
                
            dados_json = resp.json()
            licitacoes = dados_json.get('data', [])
            
            if not licitacoes: break

            for item in licitacoes:
                objeto = (item.get('objetoCompra') or "").lower()
                
                # --- FILTRO 1: BLACKLIST (BLOQUEIO IMEDIATO) ---
                # Se tiver qualquer termo da blacklist, descarta.
                if any(bad in objeto for bad in BLACKLIST):
                    continue 

                # --- FILTRO 2: WHITELIST (TERMOS DESEJADOS) ---
                match_saude = any(t in objeto for t in TERMOS_SAUDE)
                match_port = any(p.lower() in objeto for p in portfolio)

                if match_saude or match_port:
                    id_unico = str(item.get('id')) 
                    
                    if not any(x['id'] == id_unico for x in lista_atual):
                        nova_oportunidade = {
                            "id": id_unico,
                            "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                            "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                            "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                            "uf": item.get('unidadeFederativaId'),
                            "objeto": item.get('objetoCompra'), # Texto original
                            "modalidade": "Pregão (6)",
                            "data_pub": item.get('dataPublicacaoPncp'),
                            "valor_total": item.get('valorTotalEstimado', 0),
                            "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                        }
                        lista_atual.append(nova_oportunidade)
                        novos_itens += 1
                        print(".", end="", flush=True)

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

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return DATA_INICIO_VARREDURA

def main():
    print(f"--- 🚀 MONITOR DE LICITAÇÕES SAÚDE (FILTRO REFORÇADO) ---")
    print(f"🚫 Blacklist Ativa: {len(BLACKLIST)} termos bloqueados (TI, Obras, Alimentos, etc).")
    
    session = criar_sessao()
    banco_dados = carregar_banco()
    portfolio = carregar_portfolio()
    
    data_atual = ler_checkpoint()
    hoje = datetime.now()
    if data_atual > hoje: data_atual = hoje

    while data_atual.date() <= hoje.date():
        processar_dia(session, banco_dados, data_atual, portfolio)
        data_proxima = data_atual + timedelta(days=1)
        salvar_estado(banco_dados, data_proxima)
        data_atual = data_proxima
        
        if (time.time() - INICIO_EXECUCAO) > TEMPO_LIMITE_SEGURO:
            print(f"\n⚠️ TEMPO LIMITE. Pausando...")
            break

    print("\n🏁 Varredura finalizada.")

if __name__ == "__main__":
    main()
