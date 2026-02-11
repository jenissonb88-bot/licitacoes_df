import requests
import json
from datetime import datetime, timedelta
import os
import urllib3

# --- CONFIGURAÇÕES DE ALVO ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js' # Formato para evitar erro de CORS
ARQ_CHECKPOINT = 'checkpoint.txt'

# Filtro de Estados (Nordeste, Sudeste, Centro-Oeste + selecionados do Norte)
ESTADOS_ALVO = [
    "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", # Nordeste
    "ES", "MG", "RJ", "SP",                               # Sudeste
    "AM", "PA", "TO", "DF", "GO", "MT", "MS"              # Norte/Centro-Oeste selecionados
]

# Termos Positivos (Saúde)
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "farmacia", "medico", "insumo", "soro", 
    "gaze", "seringa", "luva", "reagente", "odontolog", "laborator", 
    "higiene pessoal", "enfermagem", "material cirurgico"
]

# Blacklist (Lixo - Itens que você quer descartar)
BLACKLIST = [
    "computador", "notebook", "tablet", "software", "pneu", "veiculo", 
    "obra", "engenharia", "pavimentacao", "ar condicionado", "mobiliario",
    "pintura", "alvenaria", "reforma", "ferramenta"
]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def carregar_banco():
    """Carrega dados existentes do arquivo JS para não perder o histórico."""
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                content = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                return json.loads(content)
        except: pass
    return []

def salvar_dados_js(lista_dados):
    """Salva os dados como variável JS e remove itens com mais de 180 dias."""
    # Limpeza para manter o index.html rápido
    data_limite = datetime.now() - timedelta(days=180)
    lista_dados = [
        item for item in lista_dados 
        if datetime.fromisoformat(item['data_pub'].split('T')[0]) > data_limite
    ]
    
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json_str};")
    print(f"💾 Banco de dados atualizado: {len(lista_dados)} licitações.")

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

def main():
    session = requests.Session()
    session.verify = False
    session.headers.update({'Accept': 'application/json', 'User-Agent': 'AnalistaBot/5.1'})
    
    banco = carregar_banco()
    data_atual = ler_checkpoint()
    hoje = datetime.now()

    if data_atual.date() > hoje.date():
        print("✅ Sistema 100% atualizado.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🔎 Analisando data: {data_atual.strftime('%d/%m/%Y')}...")
    
    # URL da API do PNCP para contratações públicas
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "dataInicial": ds, 
        "dataFinal": ds, 
        "codigoModalidadeContratacao": "6", # Pregão
        "pagina": 1, 
        "tamanhoPagina": 100
    }

    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"⚠️ Erro na API: {resp.status_code}")
            return

        licitacoes = resp.json().get('data', [])
        novos = 0
        
        for item in licitacoes:
            uf = item.get('unidadeFederativaId')
            obj = (item.get('objetoCompra') or "").lower()
            
            # Aplicação dos Filtros Rígidos
            if uf in ESTADOS_ALVO and any(t in obj for t in TERMOS_SAUDE) and not any(b in obj for b in BLACKLIST):
                id_u = str(item.get('id'))
                
                # Evitar duplicados no banco
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
                        "quantidade_itens": item.get('quantidadeItens', 0), # NOVO: Quantidade total
                        "data_pub": item.get('dataPublicacaoPncp'),
                        "data_abertura": item.get('dataAberturaProposta'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1

        salvar_dados_js(banco)
        
        # Avança para o dia seguinte
        proximo_dia = data_atual + timedelta(days=1)
        atualizar_checkpoint(proximo_dia)
        
        # Sinaliza para o GitHub Actions continuar se ainda não chegou em "hoje"
        precisa_continuar = proximo_dia.date() <= hoje.date()
        with open('env.txt', 'w') as f:
            val = "true" if precisa_continuar else "false"
            f.write(f"CONTINUAR_EXECUCAO={val}")
            
        print(f"✅ Dia processado. Itens novos: {novos}. Próximo: {proximo_dia.strftime('%d/%m/%Y')}")

    except Exception as e:
        print(f"💥 Erro crítico: {e}")

if __name__ == "__main__":
    main()
