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
# Defina a data de início (pode ser ajustada conforme necessidade)
DATA_INICIO_VARREDURA = datetime(2026, 1, 1)
ARQ_DADOS = 'dados/oportunidades.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
MODALIDADE_PREGAO = "6"

# 1. FILTRO GEOGRÁFICO (WhiteList)
ESTADOS_ALVO = [
    # Nordeste
    "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE",
    # Sudeste
    "ES", "MG", "RJ", "SP",
    # Norte (Selecionados)
    "AM", "PA", "TO",
    # Centro-Oeste
    "DF", "GO", "MT", "MS"
]

# 2. TERMOS DE INTERESSE (Saúde)
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "farmacia", "farmaceutic", 
    "material medico", "enfermagem", "soro", "gaze", "luva cirurgica", 
    "higiene pessoal", "fralda", "cateter", "seringa", "agulha",
    "fios de sutura", "atadura", "algodao", "esparadrapo", "reagente",
    "analise clinica", "laboratorial", "odontologic", "ortese", "protese"
]

# 3. BLACKLIST (Filtro de Lixo)
BLACKLIST = [
    # TI e Eletrônicos
    "computador", "desktop", "notebook", "tablet", "monitor", "impressora",
    "toner", "cartucho", "software", "saas", "inteligencia artificial",
    "identificador facial", "automatizado", "informatica", "teclado", "mouse",
    "nobreak", "estabilizador", "servidor", "rede", "cabo de rede", "licenca",
    "sistema de informacao", "videomonitoramento", "camera", "webcam", "drone",
    
    # Manutenção Predial e Obras
    "predial", "manutencao preventiva e corretiva predial", "ar condicionado",
    "eletrica", "hidraulica", "pintura", "alvenaria", "engenharia", "obra",
    "reforma", "cimento", "tijolo", "argamassa", "ferramenta", "extintor", 
    "elevador", "jardinagem", "poda", "roçada", "paisagismo", "climatizacao",
    
    # Mobiliário e Escritório
    "mobiliario", "moveis", "cadeira", "mesa", "armario", "divisoria",
    "poltrona", "estante", "persiana", "arquivo de aco", "papel a4",
    
    # Alimentação
    "genero alimenticio", "alimentacao", "hortifrutigranjeiro", "ovo", "carne",
    "frango", "peixe", "leite", "cafe", "acucar", "lanche", "refeicao",
    "coffee break", "buffet", "agua mineral", "cantina", "cozinha", 
    "pereciveis", "estocaveis", "bebida", "cesta basica",
    
    # Outros
    "aula pratica", "curso tecnico", "quimica industrial", "didatico",
    "pedagogico", "brinquedo", "esportiv", "musical", "automotiv",
    "veiculo", "pneu", "combustivel", "lubrificante", "transporte",
    "grafica", "banner", "panfleto", "publicidade", "evento", "locacao de palco"
]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'MonitorLicita/5.0 (HealthFocus)'
}

def carregar_portfolio():
    """ Carrega palavras-chave extras de um CSV se existir """
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
    # Ordena por data de publicação (mais recente primeiro)
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
    print(f"   📍 Região: NE, SE, CO + AM/PA/TO")
    
    pagina = 1
    novos_itens = 0
    total_api = 0
    descartes_uf = 0
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
                # 1. FILTRO DE ESTADO (UF)
                uf_item = item.get('unidadeFederativaId')
                if uf_item not in ESTADOS_ALVO:
                    descartes_uf += 1
                    continue

                objeto = (item.get('objetoCompra') or "").lower()
                
                # 2. FILTRO BLACKLIST (Anti-Lixo)
                if any(bad in objeto for bad in BLACKLIST): 
                    descartes_blacklist += 1
                    continue 

                # 3. FILTRO TEMÁTICO (Saúde / Portfolio)
                match_saude = any(t in objeto for t in TERMOS_SAUDE)
                match_port = False
                
                if not match_saude and portfolio:
                    match_port = any(p.lower() in objeto for p in portfolio)

                if match_saude or match_port:
                    id_unico = str(item.get('id')) 
                    
                    if not any(x['id'] == id_unico for x in lista_atual):
                        
                        # --- CAPTURA DE DADOS DETALHADA ---
                        
                        # Estrutura da Unidade e UASG
                        dados_unidade = item.get('unidadeOrgao', {})
                        uasg_codigo = dados_unidade.get('codigoUnidade', 'N/A')
                        nome_unidade = dados_unidade.get('nomeUnidade', 'Não Informado')
                        
                        # Datas Importantes
                        dt_abertura = item.get('dataAberturaProposta') 
                        dt_atualizacao = item.get('dataAtualizacao')
                        
                        nova_oportunidade = {
                            "id": id_unico,
                            "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                            
                            # Órgão Superior (Quem paga)
                            "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                            "cnpj": item.get('orgaoEntidade', {}).get('cnpj'),
                            
                            # Unidade Executora (Quem compra)
                            "unidade_compradora": nome_unidade,
                            "uasg": uasg_codigo,
                            
                            "uf": uf_item,
                            "cidade": dados_unidade.get('municipioNome', 'Não Informado'),
                            "objeto": item.get('objetoCompra'),
                            "modalidade": "Pregão (6)",
                            "data_pub": item.get('dataPublicacaoPncp'),
                            "data_abertura_proposta": dt_abertura,
                            "data_atualizacao": dt_atualizacao,
                            "valor_total": item.get('valorTotalEstimado', 0),
                            
                            # Links
                            "link_pncp": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}",
                            "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                        }
                        lista_atual.append(nova_oportunidade)
                        novos_itens += 1
                        print(f"   ✅ CAPTURADO [{uf_item}]: {item.get('objetoCompra')[:50]}...")
                else:
                    descartes_tema += 1

            total_paginas = dados_json.get('totalPaginas', 1)
            print(f"   -> Pág {pagina}/{total_paginas} processada...")
            
            if pagina >= total_paginas: break
            pagina += 1
            
        except Exception as e:
            print(f"   [Erro Crítico: {e}]")
            time.sleep(5) 
            break
    
    print("-" * 40)
    print(f"📊 RESUMO DO DIA {data_analise.strftime('%d/%m')}:")
    print(f"   🔹 Total API: {total_api}")
    print(f"   🗺️ Ignorados UF: {descartes_uf}")
    print(f"   🚫 Blacklist: {descartes_blacklist}")
    print(f"   ⚠️ Fora Tema: {descartes_tema}")
    print(f"   ✅ Salvos: {novos_itens}")
    print("-" * 40)
    
    return lista_atual

def main():
    print(f"--- 🚀 MONITOR DE LICITAÇÕES (CONFIGURAÇÃO FINAL) ---")
    
    session = criar_sessao()
    banco_dados = carregar_banco()
    portfolio = carregar_portfolio()
    
    data_atual = ler_checkpoint()
    hoje = datetime.now()
    
    if data_atual.date() > hoje.date():
        print("✅ Sistema atualizado.")
        # Salva variável para o GitHub Actions parar
        with open(os.environ.get('GITHUB_ENV', 'env.txt'), 'a') as f:
            f.write("CONTINUAR_EXECUCAO=false\n")
        return

    processar_um_dia(session, banco_dados, data_atual, portfolio)
    salvar_dados(banco_dados)
    
    proximo_dia = data_atual + timedelta(days=1)
    atualizar_checkpoint(proximo_dia)
    print(f"💾 Checkpoint avançado para: {proximo_dia.strftime('%d/%m/%Y')}")

    # Lógica Recursiva
    precisa_continuar = proximo_dia.date() <= hoje.date()
    with open(os.environ.get('GITHUB_ENV', 'env.txt'), 'a') as f:
        f.write(f"CONTINUAR_EXECUCAO={str(precisa_continuar).lower()}\n")
        
    if precisa_continuar:
        print("🔄 Solicitando próxima execução...")

if __name__ == "__main__":
    main()
