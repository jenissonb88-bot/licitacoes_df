import requests
import json
from datetime import datetime, timedelta
import os
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# --- CONFIGURAÇÕES DE TESTE ---
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) # Tente voltar ao dia 01/01/2026
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
MODALIDADE_PREGAO = "6"

ESTADOS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "DF", "GO", "MT", "MS"]

# Expandimos a lista para garantir captura no teste
TERMOS_SAUDE = ["medicamento", "hospitalar", "saude", "farmacia", "medico", "penso", "luva", "soro", "hospital"]

BLACKLIST = ["computador", "software", "obra", "pneu", "veiculo"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def salvar_dados_js(lista_dados):
    lista_dados.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    json_str = json.dumps(lista_dados, indent=4, ensure_ascii=False)
    conteudo_js = f"const dadosLicitacoes = {json_str};"
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(conteudo_js)

def main():
    session = requests.Session()
    session.verify = False
    
    # FORÇAR DATA PARA TESTE (01 de Janeiro de 2026)
    data_teste = datetime(2026, 1, 1)
    ds = data_teste.strftime('%Y%m%d')
    
    print(f"🔎 INICIANDO TESTE DE DIAGNÓSTICO PARA O DIA: {data_teste.strftime('%d/%m/%Y')}")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "dataInicial": ds, 
        "dataFinal": ds, 
        "codigoModalidadeContratacao": MODALIDADE_PREGAO, 
        "pagina": 1, 
        "tamanhoPagina": 50
    }

    try:
        resp = session.get(url, params=params, timeout=30)
        print(f"📡 Status da API: {resp.status_code}")
        
        licitacoes = resp.json().get('data', [])
        print(f"📦 Total de licitações encontradas no PNCP neste dia: {len(licitacoes)}")
        
        banco = []
        for item in licitacoes:
            uf = item.get('unidadeFederativaId')
            obj = (item.get('objetoCompra') or "").lower()
            
            # LOG DE ANÁLISE
            if uf not in ESTADOS_ALVO:
                # print(f"❌ Descartado por UF ({uf}): {obj[:50]}")
                continue
            
            match_saude = any(t in obj for t in TERMOS_SAUDE)
            if not match_saude:
                # print(f"❌ Descartado por não ser Saúde: {obj[:50]}")
                continue
                
            match_black = any(b in obj for b in BLACKLIST)
            if match_black:
                print(f"🚫 Bloqueado pela Blacklist: {obj[:50]}")
                continue

            print(f"✅ LICITAÇÃO CAPTURADA: {obj[:70]}")
            unidade = item.get('unidadeOrgao', {})
            banco.append({
                "id": str(item.get('id')),
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

        salvar_dados_js(banco)
        print(f"\n🚀 FIM DO TESTE. Total salvo: {len(banco)}")
        
        # Cria env.txt para o GitHub não dar erro
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=true")

    except Exception as e:
        print(f"💥 ERRO CRÍTICO: {e}")

if __name__ == "__main__":
    main()
