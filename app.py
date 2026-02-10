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
ARQ_DADOS = 'dados/oportunidades.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
MODALIDADE_PREGAO = "6"

# Termos de Saúde
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "farmacia", "farmaceutic", 
    "material medico", "enfermagem", "soro", "gaze", "luva cirurgica", 
    "higiene pessoal", "fralda", "cateter", "seringa", "agulha",
    "fios de sutura", "atadura", "algodao", "esparadrapo"
]

# Blacklist
BLACKLIST = [
    "computador", "desktop", "notebook", "tablet", "monitor", "impressora",
    "toner", "cartucho", "software", "saas", "inteligencia artificial",
    "identificador facial", "automatizado", "informatica", "teclado", "mouse",
    "nobreak", "estabilizador", "servidor", "rede", "cabo de rede",
    "predial", "manutencao preventiva", "manutencao corretiva", "ar condicionado",
    "eletrica", "hidraulica", "pintura", "alvenaria", "engenharia", "obra",
    "ferramenta", "extintor", "elevador", "jardinagem", "poda", "roçada",
    "mobiliario", "moveis", "cadeira", "mesa", "armario", "divisoria",
    "genero alimenticio", "alimentacao", "hortifrutigranjeiro", "ovo", "carne",
    "frango", "peixe", "leite", "cafe", "acucar", "lanche", "refeicao",
    "coffee break", "buffet", "agua mineral", "cantina", "cozinha",
    "aula pratica", "curso tecnico", "quimica industrial", "didatico",
    "pedagogico", "brinquedo", "esportiv", "musical", "automotiv",
    "veiculo", "pneu", "combustivel", "lubrificante", "transporte",
    "grafica", "banner", "panfleto", "publicidade", "evento"
]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'MonitorLicita/3.1 (DebugMode)'
}

def carregar_portfolio():
    try:
        try:
            df = pd.read_csv('Exportar Dados.csv', encoding='utf-8', sep=',')
        except:
            df = pd.read_csv('Exportar Dados.csv', encoding='latin-1', sep=',')
        if 'Descrição' in df.columns:
            return df['Descrição'].dropna().str.split().str[0].unique().tolist()
    except: pass
    return []

def carregar_banco():
    os.makedirs('dados', exist_ok=True)
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return []

def salvar_dados(lista_dados):
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_dados, f, indent=4, ensure_ascii=False)

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                dt_str = f.read().strip()
                return datetime.strptime(dt_str, '%Y%m%d')
        except: pass
    return DATA_INICIO_VARREDURA

def atualizar_checkpoint(proximo_dia):
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def processar_um_dia(session, lista_atual, data_analise, portfolio):
    DATA_STR = data_analise.strftime('%Y%m%d')
    print(f"\n🕵️  RAIO-X DO DIA: {data_analise.strftime('%d/%m/%Y')}")
    
    pagina = 1
    novos_itens = 0
    total_api = 0
    descartes_blacklist = 0
    descartes_tema = 0
    
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
                print(f"   [Erro API: {resp.status_code}]")
                break
                
            dados_json = resp.json()
            licitacoes = dados_json.get('data', [])
            
            if not licitacoes: break

            total_api += len(licitacoes)

            for item in licitacoes:
                objeto = (item.get('objetoCompra') or "").lower()
                
                # --- DEBUG ---
                # Se for blacklist, conta +1
                if any(bad in objeto for bad in BLACKLIST): 
                    descartes_blacklist += 1
                    continue 

                # Se não for saúde/portfólio, conta +1
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
                            "objeto": item.get('objetoCompra'),
                            "modalidade": "Pregão (6)",
                            "data_pub": item.get('dataPublicacaoPncp'),
                            "valor_total": item.get('valorTotalEstimado', 0),
                            "link": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                        }
                        lista_atual.append(nova_oportunidade)
                        novos_itens += 1
                        print(f"   ✅ CAPTURADO: {item.get('objetoCompra')[:50]}...")
                else:
                    descartes_tema += 1

            total_paginas = dados_json.get('totalPaginas', 1)
            print(f"   -> Pág {pagina}/{total_paginas} processada...")
            
            if pagina >= total_paginas: break
            pagina += 1
            
        except Exception as e:
            print(f"   [Erro Crítico: {e}]")
            break
    
    print("-" * 40)
    print(f"📊 RESUMO DO DIA {data_analise.strftime('%d/%m')}:")
    print(f"   🔹 Total recebido da API: {total_api}")
    print(f"   🚫 Bloqueados pela Blacklist: {descartes_blacklist}")
    print(f"   ⚠️ Ignorados (Fora do Tema): {descartes_tema}")
    print(f"   ✅ Oportunidades Salvas: {novos_itens}")
    print("-" * 40)
    
    return lista_atual

def main():
    print(f"--- 🚀 MONITOR DE LICITAÇÕES (MODO DETETIVE) ---")
    
    session = criar_sessao()
    banco_dados = carregar_banco()
    portfolio = carregar_portfolio()
    
    data_atual = ler_checkpoint()
    hoje = datetime.now()
    
    if data_atual.date() > hoje.date():
        print("✅ Sistema atualizado.")
        with open(os.environ.get('GITHUB_ENV', 'env.txt'), 'a') as f:
            f.write("CONTINUAR_EXECUCAO=false\n")
        return

    processar_um_dia(session, banco_dados, data_atual, portfolio)
    salvar_dados(banco_dados)
    
    proximo_dia = data_atual + timedelta(days=1)
    atualizar_checkpoint(proximo_dia)
    print(f"💾 Checkpoint avançado para: {proximo_dia.strftime('%d/%m/%Y')}")

    precisa_continuar = proximo_dia.date() <= hoje.date()
    with open(os.environ.get('GITHUB_ENV', 'env.txt'), 'a') as f:
        f.write(f"CONTINUAR_EXECUCAO={str(precisa_continuar).lower()}\n")
        
    if precisa_continuar:
        print("🔄 Solicitando próxima execução...")

if __name__ == "__main__":
    main()
