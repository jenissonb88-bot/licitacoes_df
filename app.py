import requests
import json
from datetime import datetime, timedelta
import os
import urllib3

# --- CONFIGURAÇÃO DE ALVO ---
# Se 01/01/2026 está vazio, o robô vai tentar 02/01, 03/01... até achar.
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]

# Expandimos os termos para capturar variações comuns em editais de saúde
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "saude", "farmacia", "medico", "penso", 
    "luva", "soro", "hospital", "insumos", "odontologico", "fisioterap",
    "laboratorio", "reagente", "quimico", "limpeza", "higiene"
]

BLACKLIST = ["computador", "notebook", "pneu", "veiculo", "obra", "engenharia", "pavimentação"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    conteudo_js = f"const dadosLicitacoes = {json_str};"
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(conteudo_js)

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
    
    banco = carregar_banco()
    data_atual = ler_checkpoint()
    hoje = datetime.now()

    # Se já atualizou tudo até hoje, para.
    if data_atual.date() > hoje.date():
        print("✅ Tudo atualizado até hoje.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🔎 Analisando: {data_atual.strftime('%d/%m/%Y')}...")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "dataInicial": ds, "dataFinal": ds, 
        "codigoModalidadeContratacao": "6", 
        "pagina": 1, "tamanhoPagina": 50
    }

    try:
        resp = session.get(url, params=params, timeout=30)
        licitacoes = resp.json().get('data', [])
        
        novos = 0
        for item in licitacoes:
            uf = item.get('unidadeFederativaId')
            obj = (item.get('objetoCompra') or "").lower()
            
            # Filtro Lógico
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
                        "data_pub": item.get('dataPublicacaoPncp'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1

        salvar_dados_js(banco)
        
        # Avança o dia para a próxima execução
        proximo_dia = data_atual + timedelta(days=1)
        atualizar_checkpoint(proximo_dia)
        
        # O segredo da recursividade: Mesmo que ache 0, ele avança e pede pra rodar de novo
        precisa_continuar = proximo_dia.date() <= hoje.date()
        with open('env.txt', 'w') as f:
            val = "true" if precisa_continuar else "false"
            f.write(f"CONTINUAR_EXECUCAO={val}")
            
        print(f"✅ Dia processado. Itens novos: {novos}. Próximo: {proximo_dia.strftime('%d/%m/%Y')}")

    except Exception as e:
        print(f"💥 Erro: {e}")

if __name__ == "__main__":
    main()
