import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 10 

# Blacklist preventiva (não gasta tempo com o que você já proibiu no objeto)
BLACKLIST_OBJETO = ["LOCACAO", "ALUGUEL", "GRAFICO", "IMPRESSAO", "EQUIPAMENTO", "MOVEIS", "MANUTENCAO", "OBRA", "INFORMATICA", "VEICULO", "PRESTACAO DE SERVICO", "REFORMA", "ESPORTIVO", "MATERIAL PERMANENTE", "GENERO ALIMENTICIO", "MERENDA", "ESCOLAR", "EXPEDIENTE", "EXAMES", "LABORATORIO"]

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    """Busca exaustiva de todos os itens (até 5000+)"""
    itens = []
    pag = 1
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 100}, timeout=20, verify=False)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 100: break
            pag += 1
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    """Busca os vencedores oficiais"""
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    try:
        r = session.get(url, timeout=20, verify=False)
        if r.status_code == 200:
            dados = r.json()
            return dados.get('data', []) if isinstance(dados, dict) else dados
    except: pass
    return []

def processar_licitacao(lic, session):
    """Mapeia os dados brutos conforme manuais do PNCP"""
    try:
        obj_bruto = lic.get('objetoCompra') or lic.get('objeto', '')
        obj_norm = ''.join(c for c in unicodedata.normalize('NFD', obj_bruto.upper()) if unicodedata.category(c) != 'Mn')
        
        # Filtro preventivo de Blacklist
        if any(t in obj_norm for t in BLACKLIST_OBJETO): return None

        cnpj = lic['orgaoEntidade']['cnpj'] if 'orgaoEntidade' in lic else lic.get('cnpj')
        ano = lic['anoCompra'] if 'anoCompra' in lic else lic.get('ano_compra')
        seq = lic['sequencialCompra'] if 'sequencialCompra' in lic else lic.get('sequencial_compra')
        unid = lic.get('unidadeOrgao', {})
        
        return {
            "id": f"{cnpj}{ano}{seq}",
            "data_pub": lic.get('dataPublicacaoPncp') or lic.get('data_pub'),
            "data_enc": lic.get('dataEncerramentoProposta') or lic.get('data_enc'),
            "uf": unid.get('ufSigla'),
            "cidade": unid.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'] if 'orgaoEntidade' in lic else lic.get('orgao'),
            "unidade_compradora": unid.get('nomeUnidade', 'Não Informada'),
            "objeto": obj_bruto,
            "edital_n": f"{str(lic.get('numeroCompra')).zfill(5)}/{ano}",
            "uasg": unid.get('codigoUnidade', '---'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "valor_estimado_cabecalho": float(lic.get('valorTotalEstimado') or 0),
            "sigiloso_original": lic.get('niValorTotalEstimado', False),
            "itens_raw": buscar_todos_itens(session, cnpj, ano, seq),
            "resultados_raw": buscar_todos_resultados(session, cnpj, ano, seq)
        }
    except: return None

if __name__ == "__main__":
    hoje = datetime.now()
    session = criar_sessao()
    banco = {}

    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                dados_existentes = json.load(f)
                banco = {i['id']: i for i in dados_existentes}
        except: pass

    # --- 1. REVISOR (Atualiza quem já encerrou) ---
    print("🔄 Revisor: Atualizando resultados de pregões passados...")
    pendentes = [id_l for id_l, l in banco.items() if l.get('data_enc', '')[:10] <= hoje.strftime('%Y-%m-%d')]
    for id_l in pendentes[-100:]: # Revisa os últimos 100 para manter o fluxo rápido
        cnpj, ano, seq = id_l[:14], id_l[14:18], id_l[18:]
        banco[id_l]['resultados_raw'] = buscar_todos_resultados(session, cnpj, ano, seq)

    # --- 2. SNIPER (Captura novos pregões divulgados) ---
    data_alvo = hoje - timedelta(days=1)
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try:
                cp = datetime.strptime(f.read().strip(), '%Y%m%d')
                data_alvo = cp if cp <= hoje else data_alvo
            except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper: Capturando novos pregões do dia {data_alvo.strftime('%d/%m/%Y')}")
    
    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pag_pub = 1
    novos_do_dia = 0

    while True:
        params = {
            "dataInicial": d_str, 
            "dataFinal": d_str, 
            "codigoModalidadeContratacao": "6", 
            "pagina": pag_pub, 
            "tamanhoPagina": 50
        }
        
        r = session.get(url_pub, params=params, timeout=30, verify=False)
        if r.status_code != 200: break
        
        res_json = r.json()
        lics = res_json.get('data', [])
        total_paginas = res_json.get('totalPaginas', 1)
        
        if not lics: break

        print(f"   Página {pag_pub} de {total_paginas} do dia {d_str}...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
            for f in concurrent.futures.as_completed(futuros):
                res = f.result()
                if res: 
                    banco[res['id']] = res
                    novos_do_dia += 1

        if pag_pub >= total_paginas: break
        pag_pub += 1

    # SALVAR
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    # Checkpoint e Efeito Dominó
    proximo_dia = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))
    
    if "GITHUB_OUTPUT" in os.environ:
        trigger = "true" if proximo_dia.date() <= hoje.date() else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            print(f"trigger_next={trigger}", file=f)
    
    print(f"✅ Finalizado {data_alvo.strftime('%d/%m/%Y')}. Novos pregões: {novos_do_dia}")
