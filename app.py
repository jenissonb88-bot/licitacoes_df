import requests
import json
from datetime import datetime, timedelta
import os
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES DE ALVO ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

# Filtro Geográfico
ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]

# Termos de Saúde (Lógica original que capturou dados)
TERMOS_SAUDE = ["medicamento", "hospitalar", "farmacia", "medico", "insumo", "soro", "gaze", "seringa", "luva", "reagente", "saude"]

# Blacklist
BLACKLIST = ["computador", "notebook", "pneu", "veiculo", "obra", "engenharia"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def carregar_banco():
    """ Carrega o JS existente e extrai o JSON dele """
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                content = f.read()
                # Remove a variável JS para ler como JSON
                json_part = content.replace('const dadosLicitacoes = ', '').rstrip(';')
                return json.loads(json_part)
        except: pass
    return []

def salvar_dados_js(lista_dados):
    """ Salva no formato JS para o index.html ler sem erro de CORS """
    # Ordenar: Mais recentes primeiro
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista_dados, indent=4, ensure_ascii=False)};")

def main():
    session = requests.Session()
    session.verify = False
    
    # Carrega dados e checkpoint
    banco = carregar_banco()
    
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
    print(f"🔎 Analisando: {data_atual.strftime('%d/%m/%Y')}...")
    
    # URL e Parâmetros (Voltando ao básico que funcionou)
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "dataInicial": ds,
        "dataFinal": ds,
        "codigoModalidadeContratacao": "6",
        "pagina": 1,
        "tamanhoPagina": 50
    }

    try:
        resp = session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        
        licitacoes = resp.json().get('data', [])
        novos = 0
        
        for item in licitacoes:
            # Chaves originais da API PNCP (Sem erros de nomenclatura)
            uf = item.get('unidadeFederativaId')
            obj = (item.get('objetoCompra') or "").lower()
            
            # Filtro Lógico
            if uf in ESTADOS_ALVO and any(t in obj for t in TERMOS_SAUDE) and not any(b in obj for b in BLACKLIST):
                # ID único para evitar duplicados
                id_pncp = str(item.get('id'))
                
                if not any(x['id'] == id_pncp for x in banco):
                    unidade = item.get('unidadeOrgao', {})
                    banco.append({
                        "id": id_pncp,
                        "numero": f"{item.get('numeroCompra')}/{item.get('anoCompra')}",
                        "orgao": item.get('orgaoEntidade', {}).get('razaoSocial'),
                        "unidade_compradora": unidade.get('nomeUnidade'),
                        "uasg": unidade.get('codigoUnidade'),
                        "uf": uf,
                        "cidade": unidade.get('municipioNome'),
                        "objeto": item.get('objetoCompra'),
                        "quantidade_itens": item.get('quantidadeItens', 0),
                        "data_pub": item.get('dataPublicacaoPncp'), # Chave CORRETA
                        "data_abertura": item.get('dataAberturaProposta'),
                        "valor_total": item.get('valorTotalEstimado', 0),
                        "link_api": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}",
                        "link_pncp": f"https://pncp.gov.br/app/editais/{item.get('orgaoEntidade', {}).get('cnpj')}/{item.get('anoCompra')}/{item.get('numeroCompra')}"
                    })
                    novos += 1
                    print(f"   💊 Capturado: {item.get('numeroCompra')}")

        salvar_dados_js(banco)
        
        # Avança o dia
        proximo = data_atual + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        
        # GitHub Actions Recursividade
        with open('env.txt', 'w') as f:
            f.write("CONTINUAR_EXECUCAO=true")
            
        print(f"✅ Itens novos: {novos}. Próximo: {proximo.strftime('%d/%m/%Y')}")

    except Exception as e:
        print(f"💥 Erro: {e}")
        # Mesmo com erro, permite que o Actions tente novamente ou avance
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=true")

if __name__ == "__main__":
    main()
