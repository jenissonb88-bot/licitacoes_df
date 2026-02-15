import requests
import json
import os
import unicodedata
import gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import time

ARQDADOS = 'dadosoportunidades.json.gz'
ARQCHECKPOINT = 'checkpoint.txt'
MAXWORKERS = 3

def normalize(t):
    """Remove acentos e uppercase"""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data):
    """PNCP exige YYYYMMDD"""
    return data.strftime('%Y%m%d')

def criar_sessao():
    """Sessão com retry automático"""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    """Busca TODOS itens paginados"""
    itens = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 100}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 100: break
            pag += 1
            time.sleep(0.5)  # Rate limit
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    """Busca TODOS resultados paginados - ATUALIZAÇÃO AUTOMÁTICA"""
    resultados = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 100}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            resultados.extend(lista)
            if len(lista) < 100: break
            pag += 1
            time.sleep(0.5)
        except: break
    return resultados

def e_pharma(lic):
    """Filtro inteligente PHARMA por região"""
    obj = lic.get('objetoCompra') or lic.get('objeto', '')
    obj_norm = normalize(obj)
    uf = lic.get('unidadeOrgao', {}).get('ufSigla', '').upper()
    
    if uf in ['PR','SC','RS','AP','AC']: return False
    
    if uf in ['AL','BA','CE','MA','PB','PE','PI','RN','SE']:
        termos = ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','MATERIAL MEDICO','DIETA','LU VAS','ALCOOL','SERINGA','VACINA']
        return any(t in obj_norm for t in termos)
    
    termos_puros = ['MEDICAMENTO','FARMACIA','INSUMO FARMACEUTICO','REMEDIO','FARMACEUTICO','MANIPULACAO']
    return any(t in obj_norm for t in termos_puros)

def precisa_atualizar(lic_atual, lic_nova):
    """Verifica se precisa atualizar (novos resultados/homologações)"""
    if not lic_atual: return True
    
    # Atualiza se tem novos resultados
    if len(lic_nova.get('resultadosraw', [])) > len(lic_atual.get('resultadosraw', [])):
        return True
    
    # Atualiza se mudou valor homologado
    valor_antigo = sum(float(r.get('valorTotalHomologado', 0) or 0) 
                      for r in lic_atual.get('resultadosraw', []))
    valor_novo = sum(float(r.get('valorTotalHomologado', 0) or 0) 
                    for r in lic_nova.get('resultadosraw', []))
    
    return valor_novo != valor_antigo

def processar_licitacao(lic, session):
    """Extrai detalhes + verifica atualização"""
    try:
        if not e_pharma(lic): return None
        
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
        print(f"💊 [{unid.get('ufSigla','??')}] {lic.get('objetoCompra','')[:60]}")
        
        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itensraw: return None
        
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)
        
        resultado = {
            'id': f"{cnpj}{ano}{seq}",
            'dataPub': lic.get('dataPublicacaoPncp'),
            'dataEnc': lic.get('dataEncerramentoProposta'),
            'uf': unid.get('ufSigla'),
            'cidade': unid.get('municipioNome'),
            'orgao': lic['orgaoEntidade']['razaoSocial'],
            'unidadeCompradora': unid.get('nomeUnidade', 'Não Informada'),
            'objeto': lic.get('objetoCompra') or lic.get('objeto', ''),
            'editaln': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic.get('valorTotalEstimado') or 0),
            'itensraw': itensraw,
            'resultadosraw': resultadosraw,
            'ultimaAtualizacao': datetime.now().isoformat()
        }
        
        return resultado
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None

def buscar_dia_completo(session, data_alvo, banco):
    """Busca TODAS páginas de um dia específico"""
    dstr = formatar_data_pncp(data_alvo)
    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    total_capturados = 0
    
    pag = 1
    while True:
        params = {
            'dataInicial': dstr,
            'dataFinal': dstr,
            'codigoModalidadeContratacao': 6,
            'pagina': pag,
            'tamanhoPagina': 50
        }
        
        print(f"📄 Dia {data_alvo.strftime('%Y-%m-%d')} - Pg {pag}...")
        r = session.get(url_pub, params=params, timeout=30)
        
        if r.status_code not in [200, 400]:
            print(f"⚠️  Status {r.status_code} - Continuando...")
            time.sleep(2)
            pag += 1
            continue
            
        if r.status_code == 400:
            print("✅ Dia completo!")
            break
            
        dados = r.json()
        lics = dados.get('data', [])
        total_paginas = dados.get('totalPaginas', pag) or 999
        
        print(f"📊 Pg {pag}/{total_paginas}: {len(lics)} pregões")
        
        if not lics: break
        
        pharma_lics = [lic for lic in lics if e_pharma(lic)]
        print(f"💊 {len(pharma_lics)} pharma encontrados")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
            for futuro in concurrent.futures.as_completed(futuros):
                res = futuro.result()
                if res:
                    if precisa_atualizar(banco.get(res['id']), res):
                        banco[res['id']] = res
                        total_capturados += 1
                        print(f"✅ {res['uf']}-{res['editaln']}: {'ATUALIZADO' if res['id'] in banco else 'NOVO'}")

        if len(lics) < 50 or pag >= total_paginas: break
        pag += 1
        time.sleep(1)  # Rate limit
        
    return total_capturados

if __name__ == '__main__':
    print("🚀 SNIPER PHARMA v3.0 - BUSCA CONTÍNUA + ATUALIZAÇÃO")
    
    hoje = datetime.now().date()
    session = criar_sessao()
    banco = {}
    
    # Carrega banco existente
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.load(f)}
            print(f"📦 {len(banco)} pregões carregados")
        except: pass

    # Lê checkpoint (último dia processado)
    data_proxima = hoje - timedelta(days=1)
    if os.path.exists(ARQCHECKPOINT):
        try:
            with open(ARQCHECKPOINT, 'r') as f:
                ultima_data = datetime.strptime(f.read().strip(), '%Y-%m-%d').date()
                data_proxima = (ultima_data - timedelta(days=1)).date()
        except:
            data_proxima = hoje - timedelta(days=7)  # Começa 1 semana atrás

    print(f"📅 Iniciando em: {data_proxima}")
    
    total_processados = 0
    dias_processados = 0
    
    # 🔄 LOOP CONTÍNUO ATÉ HOJE
    while data_proxima <= hoje:
        print(f"\n{'='*60}")
        print(f"🔄 PROCESSANDO DIA: {data_proxima.strftime('%Y-%m-%d')}")
        print(f"{'='*60}")
        
        novos_hoje = buscar_dia_completo(session, datetime.combine(data_proxima, datetime.min.time()), banco)
        total_processados += novos_hoje
        dias_processados += 1
        
        if novos_hoje > 0:
            print(f"✅ DIA {data_proxima.strftime('%Y-%m-%d')}: {novos_hoje} pharma {'atualizados/novos'}")
        else:
            print(f"⚪ DIA {data_proxima.strftime('%Y-%m-%d')}: sem pharma novos")
        
        # SALVA PROGRESSO
        os.makedirs('dados', exist_ok=True)
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False, separators=(',', ':'))
        
        # ATUALIZA CHECKPOINT
        with open(ARQCHECKPOINT, 'w') as f:
            f.write(data_proxima.strftime('%Y-%m-%d'))
        
        print(f"💾 Salvo: {len(banco)} total | {total_processados} processados")
        
        # PRÓXIMO DIA
        data_proxima = (data_proxima + timedelta(days=1)).date()
        time.sleep(3)  # Pausa entre dias
    
    print(f"\n🎉 COMPLETO!")
    print(f"📊 {dias_processados} dias processados")
    print(f"💾 {len(banco)} pregões pharma TOTAL")
    print(f"🆕 {total_processados} atualizados/novos encontrados")
