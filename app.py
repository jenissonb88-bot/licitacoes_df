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
MAXWORKERS = 5

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data_obj):
    if isinstance(data_obj, date):
        return data_obj.strftime('%Y%m%d')
    return data_obj.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
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

def processar_licitacao(lic_resumo, session, banco):
    try:
        id_lic = f"{lic_resumo['orgaoEntidade']['cnpj']}{lic_resumo['anoCompra']}{lic_resumo['sequencialCompra']}"
        lic_banco = banco.get(id_lic)

        # Filtro de interesse
        if not e_pharma(lic_resumo): return None

        # Verificação de Atualização (Data ou Falta de Itens)
        precisa_baixar = False
        if not lic_banco: 
            precisa_baixar = True
        else:
            dt_atual = lic_banco.get('dt_upd_pncp')
            dt_nova = lic_resumo.get('dataAtualizacaoPncp') or lic_resumo.get('dataAtualizacao')
            if dt_nova and dt_atual and dt_nova != dt_atual:
                precisa_baixar = True
            # Se já existe mas não tem itens (erro anterior), baixa de novo
            elif not lic_banco.get('itens'):
                precisa_baixar = True

        if not precisa_baixar: return None

        # --- BAIXANDO DADOS ---
        cnpj = lic_resumo['orgaoEntidade']['cnpj']
        ano = lic_resumo['anoCompra']
        seq = lic_resumo['sequencialCompra']
        
        # 1. Busca Itens Brutos
        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itens_raw: return None

        # 2. Busca Resultados Brutos
        resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq)
        
        # 3. Mapeia Resultados para acesso rápido (Dict por numeroItem)
        mapa_res = {}
        for r in resultados_raw:
            try: mapa_res[int(r['numeroItem'])] = r
            except: pass

        # 4. CONSTRUÇÃO DA LISTA OTIMIZADA (AQUI ESTÁ A MÁGICA DA REDUÇÃO)
        itens_limpos = []
        for item in itens_raw:
            try:
                num = int(item.get('numeroItem'))
                
                # Tratamento ME/EPP na fonte
                bid = 4
                if item.get('tipoBeneficioId') is not None: bid = item.get('tipoBeneficioId')
                elif isinstance(item.get('tipoBeneficio'), dict): bid = item['tipoBeneficio'].get('value')
                elif isinstance(item.get('tipoBeneficio'), int): bid = item.get('tipoBeneficio')
                
                # Dados do Resultado (se houver)
                res_match = mapa_res.get(num)
                sit_txt = str(item.get('situacaoCompraItemName', '')).upper()
                
                status_final = "ABERTO" # Default
                if res_match: status_final = "HOMOLOGADO"
                elif "CANCELADO" in sit_txt: status_final = "CANCELADO"
                elif "FRACASSADO" in sit_txt: status_final = "FRACASSADO"
                elif "DESERTO" in sit_txt: status_final = "DESERTO"

                # Objeto Item Magro
                item_obj = {
                    'n': num,
                    'd': item.get('descricao', ''),
                    'q': float(item.get('quantidade', 0)),
                    'u': item.get('unidadeMedida', ''),
                    'v_est': float(item.get('valorUnitarioEstimado', 0)),
                    'benef': bid, # Salva o código cru (1, 2, 3, 4)
                    'sit': status_final
                }

                # Se tem resultado, adiciona dados do vencedor no próprio item
                if res_match:
                    item_obj['res_forn'] = res_match.get('razaoSocial')
                    item_obj['res_val'] = float(res_match.get('valorUnitarioHomologado', 0))

                itens_limpos.append(item_obj)
            except: continue

        # 5. Monta o Objeto Final do Pregão (Sem RAW data)
        unid = lic_resumo.get('unidadeOrgao', {})
        
        dados_tratados = {
            'id': id_lic,
            'dt_pub': lic_resumo.get('dataPublicacaoPncp'),
            'dt_enc': lic_resumo.get('dataEncerramentoProposta'),
            'dt_upd_pncp': lic_resumo.get('dataAtualizacao'),
            'uf': unid.get('ufSigla'),
            'cid': unid.get('municipioNome'),
            'org': lic_resumo['orgaoEntidade']['razaoSocial'],
            'unid_nome': unid.get('nomeUnidade', 'Não Informada'),
            'obj': lic_resumo.get('objetoCompra') or lic_resumo.get('objeto', ''),
            'edit': f"{str(lic_resumo.get('numeroCompra', '')).zfill(5)}/{ano}",
            'uasg': unid.get('codigoUnidade', '---'),
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'val_tot': float(lic_resumo.get('valorTotalEstimado') or 0),
            'itens': itens_limpos, # Lista otimizada
            'timestamp': datetime.now().isoformat()
        }
        
        return dados_tratados
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
            pharma_lics = [l for l in lics if e_pharma(l)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session, banco) for l in pharma_lics]
                for futuro in concurrent.futures.as_completed(futuros):
                    try:
                        res = futuro.result()
                        if res:
                            banco[res['id']] = res
                            total_capturados += 1
                            # Conta quantos tem resultado
                            n_com_res = sum(1 for i in res['itens'] if i.get('sit') == 'HOMOLOGADO')
                            print(f"✅ SALVO: {res['uf']} - {res['edit']} (Itens: {len(res['itens'])} | Homol: {n_com_res})")
                    except: pass

            if len(lics) < 50 or pag >= total_paginas: break
            pag += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"Erro paginação: {e}")
            break
    
    return total_capturados

if __name__ == '__main__':
    print(f"🚀 SNIPER PHARMA V3.0 (Slim & Fast)")
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
    args = parser.parse_args()

    session = criar_sessao()
    banco = {}
    
    if os.path.exists(ARQDADOS):
        try:
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f)
                banco = {i['id']: i for i in (d if isinstance(d, list) else [])}
            print(f"📦 Carregados do Cache: {len(banco)}")
        except: pass

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
        print("\n💾 Salvando (Comprimido)...")
        if "/" in ARQDADOS: os.makedirs(os.path.dirname(ARQDADOS), exist_ok=True)
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
            
    print("🏁 Fim.")
