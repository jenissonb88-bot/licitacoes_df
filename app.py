import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import csv
import re
import concurrent.futures
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
ARQ_CATALOGO = 'Exportar Dados.csv'
ARQ_MANUAL = 'links_manuais.txt'
MAXWORKERS = 15
DATA_CORTE_FIXA = datetime(2026, 1, 1)

# --- GEOGRAFIA E MAPA ---
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
MAPA_SITUACAO = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- DICIONÁRIOS DE FILTRAGEM ---
VETOS_IMEDIATOS = [normalize(x) for x in ["PRESTACAO DE SERVICO", "SERVICO DE ENGENHARIA", "LOCACAO", "INSTALACAO", "MANUTENCAO", "UNIFORME", "TEXTIL", "REFORMA", "LIMPEZA PREDIAL", "LAVANDERIA", "IMPRESSAO", "CONSULTORIA", "TREINAMENTO", "VIGILANCIA", "PORTARIA", "RECEPCAO", "EVENTOS", "BUFFET", "SONDAGEM", "GEOLOGIA", "OBRAS", "PAVIMENTACAO", "RECAPEAMENTO"]]
WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO"]]
WL_MATERIAIS_NUTRI = [normalize(x) for x in ["MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS", "MASCARA", "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO", "DIETA", "NUTRICAO CLINICA"]]

# --- CARREGAMENTO DO CATÁLOGO ---
CATALOGO = set()
if os.path.exists(ARQ_CATALOGO):
    try:
        with open(ARQ_CATALOGO, 'r', encoding='latin-1') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader, None)
            for row in reader:
                if len(row) > 2:
                    for termo in [row[0], row[2]]:
                        n = normalize(termo)
                        if len(n) > 3: CATALOGO.add(n)
        print(f"📚 Catálogo carregado: {len(CATALOGO)} termos válidos.")
    except Exception as e:
        print(f"⚠️ Erro ao ler catálogo: {e}")

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/14.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def processar_licitacao(lic, session, forcado=False):
    try:
        if not isinstance(lic, dict): return ('ERRO', None, 0, 0)
        
        uo = lic.get('unidadeOrgao', {})
        uf = uo.get('ufSigla', '').upper()
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        obj_norm = normalize(obj_raw)
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()
        
        if not forcado:
            # 1. BARREIRA GEOGRÁFICA ABSOLUTA
            if uf in ESTADOS_BLOQUEADOS: return ('VETADO', None, 0, 0)
            
            # 2. FILTRO DE DATA
            if not dt_enc_str: return ('ERRO', None, 0, 0)
            dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)
            
            # 3. VETOS DE RUÍDO
            if any(v in obj_norm for v in VETOS_IMEDIATOS): return ('VETADO', None, 0, 0)
            
            # 4. PERTINÊNCIA BÁSICA
            tem_interesse = False
            if any(t in obj_norm for t in WL_MEDICAMENTOS): tem_interesse = True
            elif uf in NE_ESTADOS and any(t in obj_norm for t in WL_MATERIAIS_NUTRI): tem_interesse = True
            elif "SAUDE" in obj_norm or "HOSPITAL" in obj_norm: tem_interesse = True

            if not tem_interesse: return ('IGNORADO', None, 0, 0)

        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        if r.status_code != 200: return ('ERRO', None, 0, 0)
        
        itens_raw = r.json().get('data', []) if isinstance(r.json(), dict) else r.json()
        if not itens_raw: return ('IGNORADO', None, 0, 0)

        itens_brutos = []
        tem_catalogo = forcado
        
        for it in itens_raw:
            if not isinstance(it, dict): continue
            desc = it.get('descricao', '')
            desc_norm = normalize(desc)
            
            # Limpeza de lixo interno no edital
            if any(v in desc_norm for v in ["ARROZ", "FEIJAO", "PNEU", "GASOLINA", "RODA", "LIVRO", "ACUCAR"]): continue
            
            # Identificação NCM ou Catálogo
            if str(it.get('ncmNbsCodigo','')).startswith('30') or any(c in desc_norm for c in CATALOGO):
                tem_catalogo = True
                
            sit_id = int(it.get('situacaoCompraItem') or 1)
            
            itens_brutos.append({
                'n': it.get('numeroItem'), 
                'd': desc, 
                'q': float(it.get('quantidade') or 0),
                'u': it.get('unidadeMedida', 'UN'), 
                'v_est': float(it.get('valorUnitarioEstimado') or 0),
                'benef': int(it.get('tipoBeneficioId') or 4), # Valor Original
                'sit': MAPA_SITUACAO.get(sit_id, "EM ANDAMENTO"), 
                'res_forn': None, 
                'res_val': 0.0
            })

        if not itens_brutos: return ('IGNORADO', None, 0, 0)
        
        # A Regra de Ouro Geográfica (aplicada se não for manual)
        if not forcado and uf not in NE_ESTADOS and not tem_catalogo and not any(m in obj_norm for m in WL_MEDICAMENTOS):
            return ('IGNORADO', None, 0, 0)

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': uo.get('codigoUnidade', '---'), 'org': lic['orgaoEntidade'].get('razaoSocial', '---'), 
            'unid_nome': uo.get('nomeUnidade', '---'), 'cid': uo.get('municipioNome', '---'), 'obj': obj_raw, 
            'edit': f"{lic.get('numeroCompra', '')}/{ano}", 'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': float(lic.get('valorTotalEstimado') or 0), 'itens': itens_brutos
        }
        
        return ('CAPTURADO', dados_finais, len(itens_brutos), 0)
    except Exception as e: return ('ERRO', None, 0, 0)

def processar_inclusoes_manuais(session, banco):
    if not os.path.exists(ARQ_MANUAL): return
    print("\n⚙️ Processando Inclusões Manuais...")
    try:
        with open(ARQ_MANUAL, 'r', encoding='utf-8') as f: links = f.read().splitlines()
        padrao = re.compile(r'/editais/(\d+)/(\d+)/(\d+)')
        for link in links:
            match = padrao.search(link)
            if match:
                cnpj, ano, seq = match.groups()
                r = session.get(f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}')
                if r.status_code == 200:
                    st, d, i, h = processar_licitacao(r.json(), session, forcado=True)
                    if st == 'CAPTURADO' and d:
                        banco[d['id']] = d
                        print(f"   ✅ Captura Manual Sucesso: {cnpj}/{ano}/{seq}")
        open(ARQ_MANUAL, 'w').close() 
    except Exception as e: print(f"Erro Inclusão Manual: {e}")

def buscar_periodo(session, banco, d_ini, d_fim):
    stats = {'vetados': 0, 'capturados': 0, 'itens': 0, 'homologados': 0, 'ignorados': 0, 'erros': 0}
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        print(f"\n📅 --- DATA: {dia} ---")
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
        pag = 1
        while True:
            try:
                r = session.get(url, params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
                if r.status_code != 200: break
                dados = r.json(); lics = dados.get('data', [])
                if not lics: break
            except: break
            
            tot_pag = dados.get('totalPaginas', 1)
            s_pag = {'vetados': 0, 'capturados': 0, 'itens': 0, 'homologados': 0, 'ignorados': 0, 'erros': 0}

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    st, d, i, h = f.result()
                    if st == 'CAPTURADO':
                        s_pag['capturados'] += 1; s_pag['itens'] += i; s_pag['homologados'] += h
                        if d: banco[d['id']] = d
                    elif st == 'VETADO': s_pag['vetados'] += 1
                    elif st == 'IGNORADO': s_pag['ignorados'] += 1
                    else: s_pag['erros'] += 1
            
            for k in stats: stats[k] += s_pag[k]
            print(f"   📄 Pág {pag}/{tot_pag}: 🎯 {s_pag['capturados']} Caps | 🚫 {s_pag['vetados']} Vetos | 👁️ {s_pag['ignorados']} Ign | 🔥 {s_pag['erros']} Err")
            if pag >= tot_pag: break
            pag += 1

    print("\n" + "="*40 + "\n📊 RESUMO GERAL\n" + "="*40)
    print(f"✅ CAPTURADOS: {stats['capturados']}\n🚫 VETADOS:    {stats['vetados']}")
    print(f"👁️ IGNORADOS:  {stats['ignorados']}\n📦 ITENS:      {stats['itens']}\n🔥 ERROS:      {stats['erros']}")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=2)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        
        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    banco = {x['id']: x for x in json.load(f)}
            except: pass
            
        processar_inclusoes_manuais(session, banco)
        buscar_periodo(session, banco, dt_start, dt_end)
        
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
