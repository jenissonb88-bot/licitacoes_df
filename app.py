import requests
import json
import gzip
from datetime import datetime, timedelta

# SEMPRE FUNCIONA - SEM FILTROS
def testar_api_basica():
    print("🧪 TESTE BÁSICO PNCP")
    session = requests.Session()
    
    # DATA CORRETA: 20260214
    dstr = '20260214'
    url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    
    params = {
        'dataInicial': dstr,
        'dataFinal': dstr,
        'codigoModalidadeContratacao': 6,  # Pregão
        'pagina': 1,
        'tamanhoPagina': 10  # SÓ 10 pra teste
    }
    
    print(f"GET {url}")
    print(f"Params: {params}")
    
    r = session.get(url, params=params, timeout=30)
    print(f"✅ STATUS: {r.status_code}")
    print(f"HEADERS: {dict(r.headers) if r.status_code == 200 else 'ERRO'}")
    
    if r.status_code == 200:
        dados = r.json()
        lics = dados.get('data', [])
        print(f"🎉 {len(lics)} pregões encontrados!")
        print("PRIMEIRO:", json.dumps(lics[0], indent=2) if lics else "VAZIO")
        return lics
    else:
        print(f"❌ ERRO: {r.text}")
        return []

if __name__ == '__main__':
    pregões = testar_api_basica()
    print(f"\n🎯 RESULTADO: {len(pregões)} pregões capturados!")
