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
ARQCHECKPOINT = 'checkpoint.txt'
MAXWORKERS = 3

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data_obj):
    if isinstance(data_obj, date):
        return data_obj.strftime('%Y%m%d')
    return data_obj.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
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
            time.sleep(0.5)
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
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
    if not lic_atual: return True
    return len(lic_nova.get('resultadosraw', [])) > len(lic_atual.get('resultadosraw', []))

def processar_licitacao(lic, session):
    try:
        if not e_pharma(lic): return None
        
        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        unid = lic.get('unidadeOrgao', {})
        
        itensraw = buscar_todos_itens(session, cnpj, ano, seq)
        # Se falhar itens, tenta salvar mesmo assim se tiver objeto claro, 
        # mas aqui mantemos logica original de retornar None
        if not itensraw: return None 
        
        resultadosraw = buscar_todos_resultados(session, cnpj, ano, seq)
        
        return {
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
    except: return None

def buscar_dia_completo(session, data_obj, banco):
    if isinstance(data_obj, date):
        data_obj = datetime.combine(data_obj, datetime.min.time())
    
    dstr = formatar_data_pncp(data_obj)
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
        
        print(f"📄 {data_obj.strftime('%Y-%m-%d')} - Pg {pag}...")
        try:
            r = session.get(url_pub, params=params, timeout=30)
            
            if r.status_code == 400: # Pagina invalida geralmente é fim
                break
                
            if r.status_code != 200:
                print(f"⚠️ Status {r.status_code}")
                # Se erro for temporario, tenta proxima pag ou encerra
                break
                
            dados = r.json()
            lics = dados.get('data', [])
            total_paginas = dados.get('totalPaginas', pag) or 999
            
            print(f"📊 Pg {pag}/{total_paginas}: {len(lics)} pregões encontrados")
            
            if not lics: break
            
            pharma_lics = [lic for lic in lics if e_pharma(lic)]
            if pharma_lics:
                print(f"💊 Encontrados {len(pharma_lics)} potenciais Pharma...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
                for futuro in concurrent.futures.as_completed(futuros):
                    try:
                        res = futuro.result()
                        if res and precisa_atualizar(banco.get(res['id']), res):
                            banco[res['id']] = res
                            total_capturados += 1
                            print(f"✅ SALVO: {res['uf']} - {res['editaln']}")
                    except Exception as e:
                        pass # Ignora erros individuais de processamento

            if len(lics) < 50 or pag >= total_paginas: break
            pag += 1
            time.sleep(1)
        except Exception as e:
            print(f"Erro na paginação: {e}")
            break
    
    return total_capturados

if __name__ == '__main__':
    print(f"🚀 SNIPER PHARMA v4.0 (Github Actions Ready)")
    
    # --- Configuração de Argumentos (NOVO) ---
    parser = argparse.ArgumentParser(description='Sniper Pharma Crawler')
    parser.add_argument('--start', type=str, help='Data inicial YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='Data final YYYY-MM-DD')
    args = parser.parse_args()

    hoje = date.today()
    session = criar_sessao()
    banco = {}
    
    # 1. Carrega Banco Existente
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                dados_carregados = json.load(f)
                # Converte lista para dict se necessario
                if isinstance(dados_carregados, list):
                    banco = {i['id']: i for i in dados_carregados}
                else:
                    banco = dados_carregados
            print(f"📦 {len(banco)} pregões carregados do disco.")
        except Exception as e: 
            print(f"⚠️ Erro ao abrir banco: {e}")
            banco = {}

    # 2. Define Período de Busca
    datas_para_buscar = []

    if args.start and args.end:
        # Modo Argumentos (Github Actions)
        try:
            dt_ini = datetime.strptime(args.start, '%Y-%m-%d').date()
            dt_fim = datetime.strptime(args.end, '%Y-%m-%d').date()
            print(f"🤖 MODO ARGUMENTOS: De {dt_ini} até {dt_fim}")
            
            delta = dt_fim - dt_ini
            for i in range(delta.days + 1):
                datas_para_buscar.append(dt_ini + timedelta(days=i))
                
        except ValueError as e:
            print(f"❌ Erro formato data: {e}")
            sys.exit(1)
    else:
        # Modo Checkpoint (Legado / Local)
        print("🏠 MODO CHECKPOINT LOCAL")
        data_busca = None
        if os.path.exists(ARQCHECKPOINT):
            try:
                with open(ARQCHECKPOINT, 'r') as f:
                    checkpoint_str = f.read().strip()
                    data_busca = datetime.strptime(checkpoint_str, '%Y-%m-%d').date()
            except: pass
        
        if data_busca is None:
            data_busca = hoje - timedelta(days=1)
        
        if data_busca > hoje:
            print(f"⏭️ Checkpoint {data_busca} é futuro. Nada a fazer.")
            sys.exit(0)
            
        datas_para_buscar.append(data_busca)

    # 3. Executa a Busca para cada dia na lista
    total_novos_geral = 0
    ultima_data_processada = None

    for dia_atual in datas_para_buscar:
        print(f"\n{'='*60}")
        print(f"🔄 PROCESSANDO: {dia_atual}")
        print(f"{'='*60}")
        
        qtd = buscar_dia_completo(session, dia_atual, banco)
        total_novos_geral += qtd
        ultima_data_processada = dia_atual
        
        print(f"📈 Dia {dia_atual} finalizado. Novos itens: {qtd}")

    # 4. Salva os Dados (Apenas uma vez no final para economizar I/O)
    if ultima_data_processada:
        print(f"\n💾 Salvando banco de dados...")
        try:
            # Garante que a pasta existe (caso mude o path no futuro)
            if "/" in ARQDADOS:
                os.makedirs(os.path.dirname(ARQDADOS), exist_ok=True)
                
            with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
                json.dump(list(banco.values()), f, ensure_ascii=False)
            print(f"✅ Banco salvo com sucesso! Total registros: {len(banco)}")
        except Exception as e:
            print(f"❌ Erro fatal ao salvar: {e}")

        # 5. Atualiza Checkpoint
        # Define o checkpoint como o dia SEGUINTE ao último processado
        try:
            proximo_dia_checkpoint = ultima_data_processada + timedelta(days=1)
            with open(ARQCHECKPOINT, 'w') as f:
                f.write(proximo_dia_checkpoint.strftime('%Y-%m-%d'))
            print(f"🏁 Checkpoint atualizado para: {proximo_dia_checkpoint}")
        except:
            print("⚠️ Falha ao atualizar arquivo checkpoint")
            
    print(f"\n🎉 EXECUÇÃO CONCLUÍDA. Total capturado nesta sessão: {total_novos_geral}")
