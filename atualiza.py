import requests
import json
import gzip
import os
import concurrent.futures
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ORIGINAIS ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz'
ARQ_LOG = 'log_atualizacao.txt'
MAXWORKERS = 10

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Auditor/24.1'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def log_mensagem(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def buscar_resultado_no_pncp(lic_id, item_num, session):
    """lic_id formato: CNPJ+ANO+SEQ"""
    try:
        cnpj = lic_id[:14]
        ano = lic_id[14:18]
        seq = lic_id[18:]
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{item_num}/resultados"
        
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            res = r.json()
            if isinstance(res, list) and len(res) > 0:
                return res[0]
    except: pass
    return None

def auditoria_licitacao(lic, session):
    teve_mudanca = False
    lic_id = lic.get('id')
    
    # Percorre os itens que ainda não foram homologados
    for it in lic.get('itens', []):
        # ✅ CORREÇÃO 1: Se estiver vazio, assume "EM ANDAMENTO"
        if it.get('sit', 'EM ANDAMENTO') == "EM ANDAMENTO":
            resultado = buscar_resultado_no_pncp(lic_id, it.get('n'), session)
            
            if resultado:
                fornecedor = resultado.get('nomeRazaoSocialFornecedor') or resultado.get('razaoSocial')
                ni = resultado.get('niFornecedor', '')
                valor = float(resultado.get('valorUnitarioHomologado') or 0)
                
                if fornecedor:
                    it['res_forn'] = f"{fornecedor} ({ni})" if ni else fornecedor
                    it['res_val'] = valor
                    it['sit'] = "HOMOLOGADO"
                    teve_mudanca = True
                    
    return teve_mudanca

if __name__ == '__main__':
    if not os.path.exists(ARQDADOS):
        log_mensagem("❌ Banco de dados não encontrado.")
        exit(0)

    log_mensagem("🚀 Iniciando Auditoria de Vencedores...")
    
    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
        banco = json.load(f)

    session = criar_sessao()
    mudancas_totais = 0
    
    # ✅ CORREÇÃO 2: Filtra considerando o padrão "EM ANDAMENTO" para campos vazios
    pendentes = [l for l in banco if any(it.get('sit', 'EM ANDAMENTO') == "EM ANDAMENTO" for it in l.get('itens', []))]

    if not pendentes:
        log_mensagem("✅ Todos os itens já estão homologados.")
        exit(0)

    log_mensagem(f"⏳ Verificando {len(pendentes)} licitações pendentes...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
        futuros = {exe.submit(auditoria_licitacao, l, session): l for l in pendentes}
        for f in concurrent.futures.as_completed(futuros):
            if f.result():
                mudancas_totais += 1

    if mudancas_totais > 0:
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(banco, f, ensure_ascii=False)
        log_mensagem(f"💾 Auditoria finalizada: {mudancas_totais} licitações atualizadas.")
    else:
        log_mensagem("ℹ️ Nenhuma nova homologação encontrada nesta rodada.")
