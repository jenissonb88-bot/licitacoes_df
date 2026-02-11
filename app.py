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
ARQ_DADOS = 'dados/oportunidades.js' # Formato JS para leitura direta no navegador
ARQ_CHECKPOINT = 'checkpoint.txt'
MODALIDADE_PREGAO = "6"

# 1. FILTRO GEOGRÁFICO (WhiteList: NE, SE, CO + AM, PA, TO)
ESTADOS_ALVO = [
    "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", # Nordeste
    "ES", "MG", "RJ", "SP",                               # Sudeste
    "AM", "PA", "TO", "DF", "GO", "MT", "MS"              # Norte/Centro-Oeste selecionados
]

# 2. TERMOS DE INTERESSE (Saúde)
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "farmacia", "farmaceutic", 
    "material medico", "enfermagem", "soro", "gaze", "luva cirurgica", 
    "higiene pessoal", "fralda", "cateter", "seringa", "agulha",
    "fios de sutura", "atadura", "algodao", "esparadrapo", "reagente",
    "analise clinica", "laboratorial", "odontologic", "ortese", "protese"
]

# 3. BLACKLIST (Filtro de Lixo - Refinado)
BLACKLIST = [
    "computador", "desktop", "notebook", "tablet", "monitor", "software", "saas",
    "identificador facial", "predial", "manutencao predial", "ar condicionado",
    "pintura", "alvenaria", "engenharia", "obra", "reforma", "cimento",
    "mobiliario", "moveis", "cadeira", "mesa", "genero alimenticio", "ovo", "carne",
    "hortifrutigranjeiro", "veiculo", "pneu", "combustivel", "grafica"
]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'MonitorHealth/5.0 (Analista de Licitações)'
}

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session

def carregar_banco():
    """ Carrega dados do arquivo JS existente para manter histórico """
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                content = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                return json.loads(content)
        except: pass
    return []

def salvar_dados_js(lista_dados):
    """ Salva os dados como variável JS e aplica limpeza de 180 dias """
    # Ordenar por data (mais recente primeiro)
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    
    # Limpeza: Mantém apenas licitações dos últimos 180 dias
    data_limite = datetime.now() - timedelta(days=180)
    lista_dados = [
        item for item in lista_dados 
        if datetime.fromisoformat(item['data_pub'].split('T')[0]) > data_limite
    ]

    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    conteudo_js = f"const dadosLicitacoes = {json_str};"
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(conteudo_js)
    print(f"💾 {len(lista_dados)} licitações guardadas em {ARQ_DADOS}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return DATA_INICIO_VARREDURA

def atualizar_checkpoint(data):
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data.strftime('%Y%m%d'))

def processar_um_dia(session, lista_atual, data_analise):
    ds = data_analise.strftime('%Y%m%d')
    print(f"\n🔎 Varredura: {data_analise.strftime('%d/%m/%Y')}")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pagina = 1
    novos = 0

    while True:
        params = {
            "dataInicial": ds, 
            "dataFinal": ds, 
            "codigoModalidadeContratacao": MODALIDADE_PREGAO, 
            "pagina": pagina, 
            "tamanhoPagina": 50
        }

        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code != 200: break
            
            dados = resp.json()
            licitacoes = dados.get('data', [])
            if not licitacoes: break

            for item in licitacoes:
                uf = item.get('unidadeFederativaId')
                objeto = (item.get('objetoCompra') or "").lower()

                # Filtros: UF + Saúde + Blacklist
                if uf in ESTADOS_ALVO and any(t in objeto for t in TERMOS_SAUDE) and not any(b in objeto for b in BLACKLIST):
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
                            "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}",
                            "link_pncp": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                        })
                        novos += 1

            if pagina >= dados.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e:
            print(f"Erro na página {pagina}: {e}")
            break

    print(f"✅ Itens novos: {novos}")
    return lista_atual

def main():
    session = criar_sessao()
    banco = carregar_banco()
    data_atual = ler_checkpoint()
    hoje = datetime.now()

    # Se a data do checkpoint já passou de hoje, encerra
    if data_atual.date() > hoje.date():
        print("✨ Tudo atualizado.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    # Processa apenas UM dia por execução (para não estourar o tempo do GitHub)
    banco = processar_um_dia(session, banco, data_atual)
    salvar_dados_js(banco)
    
    # Avança o checkpoint
    proximo_dia = data_atual + timedelta(days=1)
    atualizar_checkpoint(proximo_dia)

    # Sinaliza se precisa continuar a recursão
    precisa_continuar = proximo_dia.date() <= hoje.date()
    with open('env.txt', 'w') as f:
        val = "true" if precisa_continuar else "false"
        f.write(f"CONTINUAR_EXECUCAO={val}")
    
    print(f"📡 Próxima execução necessária? {val}")

if __name__ == "__main__":
    main()
