import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import time

# Configurações
ARQDADOS = 'dadosoportunidades.json.gz'
MAXWORKERS = 5  # Aumentei levemente para agilizar a busca extra

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data_obj):
    if isinstance(data_obj, date):
        return data_obj.strftime('%Y%m%d')
    return data_obj.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    # Retry mais agressivo para garantir que pegue o resultado
    retries = Retry(total=8, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 50}, timeout=15)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if len(lista) < 50: break
            pag += 1
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    resultados = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados'
        try:
            # Timeout maior para resultados, pois é onde costuma falhar
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 50}, timeout=20)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            resultados.extend(lista)
            if len(lista) < 50: break
            pag += 1
        except: break
    return resultados

def e_pharma(lic):
    obj = lic.get('objetoCompra') or lic.get('objeto', '')
    obj_norm = normalize(obj)
    
    termos_gatilho = [
        'MEDICAMENT', 'FARMAC', 'HOSPITAL', 'SAUDE', 'ODONTO', 'ENFERMAGEM',
        'MATERIAL MEDICO', 'INSUMO', 'LUVA', 'SERINGA', 'AGULHA', 'LABORATORI',
        'FRALDA', 'ABSORVENTE', 'REMEDIO', 'SORO', 
        'HIPERTENSIV', 'INJETAV', 'ONCOLOGIC', 'ANALGESIC', 
        'ANTI-INFLAMAT', 'ANTIBIOTIC', 'ANTIDEPRESSIV', 
        'ANSIOLITIC', 'DIABETIC', 'GLICEMIC', 'CONTROLAD',
        'MATERIAL PENSO', 'DIETA', 'FORMULA', 'PROTEIC', 
        'CALORIC', 'GAZE', 'ATADURA'
    ]
    
    return any(t in obj_norm for t in termos_gatilho)

def precisa_processar_profundo(lic_nova, lic_banco):
    """
    Decide se devemos gastar tempo baixando itens e resultados.
    """
    # 1. Se não temos no banco, PRECISA baixar.
    if not lic_banco: 
        return True
    
    # 2. Se a data de atualização do PNCP mudou, PRECISA baixar.
    dt_atual = lic_banco.get('dataAtualizacaoPncp')
    dt_nova = lic_nova.get('dataAtualizacaoPncp') or lic_nova.get('dataAtualizacao')
    if dt_nova and dt_atual and dt_nova != dt_atual:
        return True

    # 3. REGRA DE OURO (CORREÇÃO DE RESULTADOS):
    # Se já temos no banco, mas NÃO TEMOS RESULTADOS salvos,
    # FORÇAMOS uma nova busca para ver se saiu algo novo.
    resultados_salvos = lic_banco.get('resultadosraw', [])
    if not resultados_salvos:
        # Só força se não for muito antigo (ex: dataEnc existe)
        return True
        
    return False

def processar_licitacao(lic_resumo, session, banco):
    try:
        id_lic = f"{lic_resumo['orgaoEntidade']['cnpj']}{lic_resumo['anoCompra']}{lic_resumo['sequencialCompra']}"
        lic_banco = banco.get(id_lic)

        # Filtro rápido de objeto
        if not e_pharma(lic_resumo): return None

        # Verifica se precisamos gastar API call
        if not precisa_processar_profundo(lic_resumo, lic_banco):
            return None # Pula, já temos atualizado

        # Se chegou aqui, vamos baixar tudo
        cnpj = lic_resumo['orgaoEntidade']['cnpj']
        ano = lic_resumo['anoCompra']
        seq = lic_resumo['sequencialCompra']
        unid = lic_resumo.get('unidadeOrgao', {})
        
        # Estrutura
        dados_tratados = {
            'id': id_lic,
            'dataPub': lic_resumo.get('dataPublicacaoPncp'),
            'dataEnc': lic_resumo.get('dataEncerramentoProposta'),
            'dataAtualizacaoPncp': lic_resumo.get('dataAtualizacao'),
            'uf': unid.get('ufSigla'),
            'cidade': unid.get('municipioNome'),
            'orgao': lic_resumo['orgaoEntidade']['razaoSocial'],
            'unidadeCompradora': unid.get('nomeUnidade', 'Não Informada'),
            'objeto': lic_resumo.get('objetoCompra') or lic_resumo.get('objeto', ''),
            'editaln': f"{str(lic_resumo.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'valorGlobalApi': float(lic_resumo.get('valorTotalEstimado') or 0),
            'itensraw': [],
            'resultadosraw': [],
            'ultimaAtualizacao': datetime.now().isoformat()
        }
        
        # Busca Itens
        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itensraw: return None
        dados_tratados['itensraw'] = itensraw
        
        # Busca Resultados (AGORA OBRIGATÓRIO TENTAR SEMPRE QUE ENTRAR AQUI)
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)
        dados_tratados['resultadosraw'] = resultadosraw
        
        return dados_tratados
    except Exception as e:
        return None

def buscar_dia_completo(session, data_obj, banco):
    if isinstance(data_obj, date):
        data_obj = datetime.combine(data_obj, datetime.min.time())
    
    dstr = formatar_data_pncp(data_obj)
    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    total_capturados = 0
    
    pag = 1
    while True:
        params = {
            'dataInicial': dstr, 'dataFinal': dstr,
            'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50
        }
        
        print(f"📄 {data_obj.strftime('%Y-%m-%d')} - Pg {pag}...")
        try:
            r = session.get(url_pub, params=params, timeout=30)
            if r.status_code != 200: break
            dados = r.json()
            lics = dados.get('data', [])
            total_paginas = dados.get('totalPaginas', pag) or 999
            
            if not lics: break
            
            # Filtra só pharma antes de processar threads
            pharma_lics = [l for l in lics if e_pharma(l)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                # Passamos o banco para dentro da função para decidir lá se baixa ou não
                futuros = [exe.submit(processar_licitacao, l, session, banco) for l in pharma_lics]
                
                for futuro in concurrent.futures.as_completed(futuros):
                    try:
                        res = futuro.result()
                        if res:
                            # Se retornou algo, é porque baixou dados novos/atualizados
                            banco[res['id']] = res
                            total_capturados += 1
                            
                            n_res = len(res['resultadosraw'])
                            status = "ATUALIZADO"
                            if n_res > 0: status += " COM RESULTADOS"
                            
                            print(f"✅ {status}: {res['uf']} - {res['editaln']} (Itens: {len(res['itensraw'])} | Res: {n_res})")
                    except: pass

            if len(lics) < 50 or pag >= total_paginas: break
            pag += 1
            # Pausa suave
            time.sleep(0.5)
        except Exception as e:
            print(f"Erro paginação: {e}")
            break
    
    return total_capturados

if __name__ == '__main__':
    print(f"🚀 SNIPER PHARMA V-APP 2.3 (Resultados Agressivos)")
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
    args = parser.parse_args()

    session = criar_sessao()
    banco = {}
    
    # Carrega banco existente (Tenta recuperar dados anteriores)
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f)
                banco = {i['id']: i for i in (d if isinstance(d, list) else [])}
            print(f"📦 Carregados do Cache: {len(banco)}")
        except: 
            print("⚠️ Cache ilegível ou vazio, iniciando do zero.")

    datas = []
    if args.start and args.end:
        dt_ini = datetime.strptime(args.start, '%Y-%m-%d').date()
        dt_fim = datetime.strptime(args.end, '%Y-%m-%d').date()
        delta = dt_fim - dt_ini
        for i in range(delta.days + 1): datas.append(dt_ini + timedelta(days=i))
    else:
        datas.append(date.today() - timedelta(days=1))

    total = 0
    for dia in datas:
        print(f"\n🔄 Processando {dia}...")
        total += buscar_dia_completo(session, dia, banco)

    if total > 0 or datas:
        print("\n💾 Salvando...")
        if "/" in ARQDADOS: os.makedirs(os.path.dirname(ARQDADOS), exist_ok=True)
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
            
    print("🏁 Fim.")
