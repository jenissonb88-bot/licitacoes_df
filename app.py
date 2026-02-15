# TOPO do app.py - ADICIONE LOGS:
print("🔍 TESTANDO API PNCP AGORA...")
url_teste = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
params_teste = {
    'dataInicial': '2026-02-14', 'dataFinal': '2026-02-14',
    'codigoModalidadeContratacao': 6,
    'pagina': 1, 'tamanhoPagina': 5  # SÓ 5 pra teste
}
r = requests.get(url_teste, params=params_teste, timeout=30)
print(f"STATUS: {r.status_code}")
print(f"HEADERS: {dict(r.headers)}")
print(f"RESPONSE: {r.text[:1000]}")  # Primeiros 1000 chars
