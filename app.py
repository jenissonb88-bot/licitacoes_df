import requests
import json
from datetime import datetime, timedelta
import os
import urllib3

# --- CONFIGURAÇÕES ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

# Estados Alvo
ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]

# Termos de Saúde (Ampliados para capturar editais genéricos)
TERMOS_SAUDE = [
    "medicamento", "hospitalar", "saude", "farmacia", "medico", "insumo", 
    "soro", "gaze", "seringa", "luva", "reagente", "odontolog", "cirurgico",
    "material de consumo", "quimico", "laboratorio", "fisioterapia"
]

# Blacklist (Removendo o que atrapalha)
BLACKLIST = ["computador", "notebook", "pneu", "veiculo", "obra", "engenharia", "pavimentacao", "locacao de imovel"]

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
    # Mantém histórico de 180 dias
    limite = datetime.now() - timedelta(days=180)
    lista_dados = [i for i in lista_dados if datetime.fromisoformat(i['data_pub'].split('T')[0]) > limite]
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista_dados, indent=4, ensure_ascii=False)};")

def main():
    session = requests.Session()
    session.verify = False
    
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')
    else:
        data_atual = DATA_INICIO_VARREDURA

    hoje = datetime.now()
    if data_atual.date() > hoje.date():
        print("✅ Sistema atualizado.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"\n🔎 VARREDURA: {data_atual.strftime('%d/%m/%Y')}")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": ds, "dataFinal": ds, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 100}

    banco = carregar_banco()
    novos = 0
    
    try:
        resp = session.get(url, params=params, timeout=60)
        licitacoes = resp.json().get('data', [])
        print(f"📦 Editais encontrados no PNCP: {len(licitacoes)}")
        
        for item in licitacoes:
            # Captura UF de qualquer lugar disponível
            uf = item.get('unidadeFederativaId') or item.get('unidadeOrgao', {}).get('ufSigla')
            obj = (item.get('objetoCompra') or "").lower()
            
            # Validação Logística
            match_uf = uf in ESTADOS_ALVO or uf is None # Se UF for nula, deixamos passar para conferência manual
            match_saude = any(t in obj for t in TERMOS_SAUDE)
            match_black = any(b in obj for b in BLACKLIST)
            
            if match_uf and match_saude and not match_black:
                id_u = str(item.get('id'))
                if not any(x['id'] == id_u for x in banco):
                    unid = item.get('unidadeOrgao', {})
                    banco.append({
                        "id": id_u,
                        "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                        "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                        "unidade_compradora": unid.get('nomeUnidade'),
                        "uasg": unid.get('codigoUnidade'),
                        "uf": uf or "N/A",
                        "cidade": unid.get('municipioNome'),
                        "objeto": item.get('objetoCompra'),
                        "quantidade_itens": item.get('quantidadeItens', 0),
                        "data_pub": item.get('dataPublicacaoPncp'),
                        "data_abertura": item.get('dataAberturaProposta'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}",
                        "link_pncp": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1
                    print(f"   💊 CAPTURADO: {obj[:60]}...")

        salvar_dados_js(banco)
        proximo = data_atual + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=true")
        print(f"📊 Novos itens hoje: {novos}")

    except Exception as e:
        print(f"💥 Erro: {e}")

if __name__ == "__main__":
    main()
