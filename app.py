import requests
import json
from datetime import datetime, timedelta
import os
import urllib3

# --- CONFIGURAÇÕES ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "medico", "insumo", "soro", "gaze", "seringa", "luva", "reagente"]
BLACKLIST = ["computador", "notebook", "pneu", "veiculo", "obra", "engenharia"]

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
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json_str};")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return DATA_INICIO_VARREDURA

def main():
    session = requests.Session()
    session.verify = False
    
    data_atual = ler_checkpoint()
    hoje = datetime.now()

    if data_atual.date() > hoje.date():
        print("✅ Sistema atualizado.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    # CORREÇÃO DO ERRO 400: O PNCP exige formato YYYYMMDD para a URL de consulta de publicações
    ds = data_atual.strftime('%Y%m%d')
    print(f"🔎 Analisando data: {data_atual.strftime('%d/%m/%Y')}...")
    
    # Endpoint correto conforme Manual PNCP V1
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    
    # Parâmetros ajustados para evitar Bad Request
    params = {
        "dataInicial": ds,
        "dataFinal": ds,
        "codigoModalidadeContratacao": "6",
        "pagina": "1",
        "tamanhoPagina": "50"
    }

    try:
        resp = session.get(url, params=params, timeout=30)
        
        # Se der erro 400, vamos tentar o formato com hífen (YYYY-MM-DD) como fallback
        if resp.status_code == 400:
            print("⚠️ Erro 400 detectado. Tentando formato de data alternativo...")
            params["dataInicial"] = data_atual.strftime('%Y-%m-%d')
            params["dataFinal"] = data_atual.strftime('%Y-%m-%d')
            resp = session.get(url, params=params, timeout=30)

        if resp.status_code != 200:
            print(f"❌ Erro persistente na API: {resp.status_code}")
            # Avançamos o dia mesmo com erro para não travar o robô em um dia problemático
            proximo = data_atual + timedelta(days=1)
            with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
            with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=true")
            return

        licitacoes = resp.json().get('data', [])
        banco = carregar_banco()
        novos = 0
        
        for item in licitacoes:
            uf = item.get('unidadeFederativaId')
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
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1

        salvar_dados_js(banco)
        proximo = data_atual + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=true")
        print(f"✅ Sucesso! Itens novos: {novos}. Próximo dia: {proximo.strftime('%d/%m/%Y')}")

    except Exception as e:
        print(f"💥 Erro crítico: {e}")

if __name__ == "__main__":
    main()
