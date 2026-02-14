import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# Desativa avisos de SSL (comum em APIs governamentais)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
MAX_WORKERS = 8 # Mantido moderado para estabilidade da API

def criar_sessao():
    s = requests.Session()
    # Retry para evitar quedas em conexões instáveis do governo
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    """Busca exaustiva de todos os itens da licitação (suporta 5000+)"""
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
    """Busca os vencedores oficiais da licitação"""
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    try:
        r = session.get(url, timeout=20, verify=False)
        if r.status_code == 200:
            dados = r.json()
            return dados.get('data', []) if isinstance(dados, dict) else dados
    except: pass
    return []

def processar_licitacao(lic, session):
    """Extrai os dados brutos conforme manuais do PNCP 2026"""
    try:
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        id_lic = f"{cnpj}{ano}{seq}"
        
        # Unidade Compradora e UASG (codigoUnidade)
        unid_obj = lic.get('unidadeOrgao', {})
        nome_unidade = unid_obj.get('nomeUnidade', 'Unidade não informada')
        codigo_unidade = unid_obj.get('codigoUnidade', '---')
        
        return {
            "id": id_lic,
            "data_pub": lic.get('dataPublicacaoPncp'),
            "data_enc": lic.get('dataEncerramentoProposta'),
            "uf": unid_obj.get('ufSigla'),
            "cidade": unid_obj.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'],
            "unidade_compradora": nome_unidade, # CORRIGIDO
            "objeto": lic.get('objetoCompra'),
            "edital_n": f"{str(lic.get('numeroCompra')).zfill(5)}/{ano}", # Formato 00001/2026
            "uasg": codigo_unidade,
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "valor_estimado_cabecalho": float(lic.get('valorTotalEstimado') or 0),
            "sigiloso_original": lic.get('niValorTotalEstimado', False), # Identificador de sigilo
            "itens_raw": buscar_todos_itens(session, cnpj, ano, seq),
            "resultados_raw": buscar_todos_resultados(session, cnpj, ano, seq)
        }
    except Exception as e:
        return None

if __name__ == "__main__":
    hoje = datetime.now()
    # Default: busca dados de ontem se não houver checkpoint
    data_alvo = hoje - timedelta(days=1)
    
    # Validação do Checkpoint
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            try:
                cp = datetime.strptime(f.read().strip(), '%Y%m%d')
                # Se o checkpoint for futuro (erro de 2025), reseta para ontem
                if cp > hoje:
                    data_alvo = hoje - timedelta(days=1)
                else:
                    data_alvo = cp
            except: pass

    session = criar_sessao()
    banco = {}
    
    # Carrega base existente (comprimida)
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                dados_existentes = json.load(f)
                banco = {i['id']: i for i in dados_existentes}
        except: pass

    d_str = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP Iniciado - Varrendo Dia: {data_alvo.strftime('%d/%m/%Y')}")

    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pag_pub = 1
    novos_no_dia = 0

    # Loop de páginas do dia (Efeito Dominó Exaustivo)
    while True:
        params = {
            "dataInicial": d_str, 
            "dataFinal": d_str, 
            "codigoModalidadeContratacao": "6", 
            "pagina": pag_pub, 
            "tamanhoPagina": 50
        }
        
        try:
            r = session.get(url_pub, params=params, timeout=30, verify=False)
            if r.status_code != 200: break
            
            res_json = r.json()
            lics = res_json.get('data', [])
            total_paginas = res_json.get('totalPaginas', 1)
            
            if not lics: break

            print(f"   Página {pag_pub} de {total_paginas}...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session): l for l in lics}
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: 
                        banco[res['id']] = res
                        novos_no_dia += 1

            if pag_pub >= total_paginas: break
            pag_pub += 1
        except: break

    # Salva os dados brutos para o Faxineiro processar
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False)

    # Prepara o próximo dia para o Checkpoint
    proximo_dia = data_alvo + timedelta(days=1)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))
    
    # RESOLUÇÃO DO EFEITO DOMINÓ (Sinaliza se deve rodar de novo)
    # Se o dia processado ainda for anterior a hoje, trigger_next = true
    if "GITHUB_OUTPUT" in os.environ:
        # Se proximo_dia <= hoje, ainda há o que buscar
        trigger = "true" if proximo_dia.date() <= hoje.date() else "false"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            print(f"trigger_next={trigger}", file=f)
    
    print(f"✅ Dia {data_alvo.strftime('%d/%m/%Y')} finalizado. Novos capturados: {novos_no_dia}")
