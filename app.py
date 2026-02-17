import requests
import json
import os
import unicodedata
import gzip
import argparse
import sys
import csv
import traceback
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
ARQ_CATALOGO = 'Exportar Dados.csv'
MAXWORKERS = 10 
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- GEOGRAFIA ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

# BLOQUEIO TOTAL (Nem perde tempo baixando)
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- CARREGAMENTO DO CATÁLOGO ---
CATALOGO_TERMOS = set()
if os.path.exists(ARQ_CATALOGO):
    try:
        for enc in ['utf-8', 'latin-1', 'cp1252']:
            try:
                with open(ARQ_CATALOGO, 'r', encoding=enc) as f:
                    leitor = csv.reader(f, delimiter=';') 
                    cabecalho = next(leitor, None)
                    if cabecalho and len(cabecalho) < 2: 
                        f.seek(0)
                        leitor = csv.reader(f, delimiter=',')
                        next(leitor, None)
                    
                    for row in leitor:
                        if len(row) > 1:
                            termos = [row[0], row[1]] if len(row) > 1 else [row[0]]
                            for t in termos:
                                norm = normalize(t)
                                if len(norm) > 4: CATALOGO_TERMOS.add(norm)
                print(f"📚 Catálogo carregado no App: {len(CATALOGO_TERMOS)} termos.")
                break
            except UnicodeDecodeError: continue
            except Exception: break
    except: print("⚠️ Aviso: Não foi possível ler o catálogo CSV.")

# --- LISTAS DE PALAVRAS-CHAVE ---

# 1. ITENS UNIVERSAIS NE (Passam por cima de tudo no Nordeste)
TERMOS_UNIVERSAIS_NE = [
    "FRALDA", "ABSORVENTE", "ALCOOL 70", "ALCOOL ETILICO", "ALCOOL GEL", "ALCOOL EM GEL"
]

# 2. VETOS (Muros de Contenção)
VETOS_ALIMENTACAO = [normalize(x) for x in [
    "ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE", "PERECIVEIS", 
    "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS", "LANCHE", "REFEICOES", 
    "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE", "POLPA DE FRUTA", "ESTIAGEM"
]]

VETOS_EDUCACAO = [normalize(x) for x in [
    "MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS", 
    "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO"
]]

VETOS_OPERACIONAL = [normalize(x) for x in [
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", 
    "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", 
    "COMBUSTIVEL", "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA",
    "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", 
    "FARDAMENTO", "VESTUARIO", "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS"
]]

VETOS_ADM = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "CREDENCIAMENTO", "LEILAO", "ALIENACAO"]]

TODOS_VETOS = VETOS_ALIMENTACAO + VETOS_EDUCACAO + VETOS_OPERACIONAL + VETOS_ADM

# 3. ALVOS PADRÃO
WL_MEDICAMENTOS = [normalize(x) for x in [
    "MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", 
    "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO"
]]

WL_NUTRI_CLINICA = [normalize(x) for x in [
    "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", 
    "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA"
]]

WL_MATERIAIS_NE = [normalize(x) for x in [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", 
    "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA CIRURGICA"
]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    
    # SALVA-VIDAS UNIVERSAL NE:
    # Se for do NE e tiver Fralda, Absorvente ou Álcool 70, IGNORA OS VETOS.
    if uf in NE_ESTADOS:
        if any(univ in obj for univ in TERMOS_UNIVERSAIS_NE):
            return False # Não veta!

    # Valida Vetos Absolutos
    for v in TODOS_VETOS:
        if v in obj:
            # Proteção para Dietas no NE
            if "NUTRICAO" in v or "ALIMENT" in v:
                if any(bom in obj for bom in WL_NUTRI_CLINICA) and "ESCOLAR" not in obj:
                    return False
            return True
            
    # Veto Contextual de Limpeza/Higiene
    # Se tiver Álcool 70 (Universal NE), já passou pelo Salva-Vidas acima.
    if "LIMPEZA" in obj or "HIGIENE" in obj:
        if not any(x in obj for x in ["HOSPITALAR", "UBS", "SAUDE", "CLINICA"]):
            return True
            
    return False

def safe_float(val):
    try:
        if val is None: return 0.0
        return float(val)
    except: return 0.0

def processar_licitacao(lic, session):
    try:
        # 1. Validação Estrutural e Data
        if not isinstance(lic, dict): return ('ERRO', None, 0, 0)
        
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        uo = lic.get('unidadeOrgao')
        if not isinstance(uo, dict): uo = {}
        uf = uo.get('ufSigla', '').upper()
        
        # 1.1 VETO GEOGRÁFICO TOTAL (Economia de recurso)
        if uf in ESTADOS_BLOQUEADOS: return ('VETADO', None, 0, 0)

        dt_enc_str = lic.get('dataEncerramentoProposta')
        if not dt_enc_str: return ('ERRO', None, 0, 0)
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)

        # 2. Aplicação dos Vetos
        if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 0)

        # 3. Análise de Interesse
        obj_norm = normalize(obj_raw)
        tem_interesse = False
        
        # A. Universais NE (Fralda, Absorvente, Alcool 70) - Prioridade Máxima no NE
        if uf in NE_ESTADOS and any(t in obj_norm for t in TERMOS_UNIVERSAIS_NE):
            tem_interesse = True

        # B. Medicamentos: Passa em TODO BRASIL (exceto bloqueados)
        elif any(t in obj_norm for t in WL_MEDICAMENTOS):
            tem_interesse = True
            
        # C. Nutrição e Materiais: Passa APENAS NO NORDESTE
        elif uf in NE_ESTADOS and any(t in obj_norm for t in WL_MATERIAIS_NE + WL_NUTRI_CLINICA):
            tem_interesse = True
            
        # D. Termos Genéricos de Saúde (Para verificar itens)
        elif "SAUDE" in obj_norm or "HOSPITAL" in obj_norm:
            tem_interesse = True

        if not tem_interesse: return ('IGNORADO', None, 0, 0)

        # 4. Captura e Análise de Itens
        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        
        r_itens = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        if r_itens.status_code != 200: return ('ERRO', None, 0, 0)
        
        resp_json = r_itens.json()
        if isinstance(resp_json, dict): itens_raw = resp_json.get('data', [])
        elif isinstance(resp_json, list): itens_raw = resp_json
        else: return ('IGNORADO', None, 0, 0)

        if not itens_raw: return ('IGNORADO', None, 0, 0)

        itens_limpos = []
        homologados = 0
        tem_item_catalogo = False
        
        for it in itens_raw:
            if not isinstance(it, dict): continue
            
            desc = it.get('descricao', '')
            desc_norm = normalize(desc)
            
            # Filtro Individual de Item 
            # (Remove lixo óbvio, mas deixa Fralda/Alcool se for o caso)
            if any(v in desc_norm for v in ["ARROZ", "FEIJAO", "CARNE", "PNEU", "GASOLINA", "RODA", "LIVRO", "COPO", "CAFE", "ACUCAR"]):
                continue

            if any(term in desc_norm for term in CATALOGO_TERMOS):
                tem_item_catalogo = True
            
            num = it.get('numeroItem')
            res = None
            if it.get('temResultado'):
                try:
                    r_res = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados", timeout=15)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        if isinstance(rl, list): res = rl[0] if len(rl) > 0 else None
                        elif isinstance(rl, dict): res = rl
                        if not isinstance(res, dict): res = None
                        if res: homologados += 1
                except: pass

            itens_limpos.append({
                'n': num, 'd': desc, 'q': safe_float(it.get('quantidade')),
                'u': it.get('unidadeMedida', ''), 'v_est': safe_float(it.get('valorUnitarioEstimado')),
                'benef': it.get('tipoBeneficioId') or 4,
                'sit': "HOMOLOGADO" if res else str(it.get('situacaoCompraItemName', 'ABERTO')).upper(),
                'res_forn': (res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')) if res else None,
                'res_val': safe_float(res.get('valorUnitarioHomologado')) if res else 0.0
            })

        if not itens_limpos: return ('IGNORADO', None, 0, 0)
        
        # 5. Validação Final (A Regra de Ouro Geográfica)
        if uf not in NE_ESTADOS:
            # Fora do NE: Só Medicamento explícito ou Catálogo
            if not tem_item_catalogo and not any(m in obj_norm for m in WL_MEDICAMENTOS):
                 return ('IGNORADO', None, 0, 0)

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': lic['unidadeOrgao'].get('codigoUnidade', '---'),
            'org': lic['orgaoEntidade']['razaoSocial'], 'unid_nome': lic['unidadeOrgao'].get('nomeUnidade', '---'),
            'cid': lic['unidadeOrgao'].get('municipioNome'), 'obj': obj_raw, 
            'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': safe_float(lic.get('valorTotalEstimado')), 
            'itens': itens_limpos
        }
        
        return ('CAPTURADO', dados_finais, len(itens_limpos), homologados)

    except: return ('ERRO', None, 0, 0)

def buscar_periodo(session, banco, d_ini, d_fim):
    stats = {'vetados': 0, 'capturados': 0, 'itens': 0, 'homologados': 0, 'ignorados': 0, 'erros': 0}
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        print(f"\n📅 --- DATA: {dia} ---")
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
        pag = 1
        while True:
            r = session.get(url, params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
            if r.status_code != 200: break
            dados = r.json(); lics = dados.get('data', [])
            if not lics: break
            
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
    print(f"✅ CAPTURADOS: {stats['capturados']}")
    print(f"🚫 VETADOS:    {stats['vetados']}")
    print(f"👁️ IGNORADOS:  {stats['ignorados']}")
    print(f"📦 ITENS:      {stats['itens']}")
    print("="*40 + "\n")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        dt_start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=2)
        dt_end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        session = criar_sessao(); banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                d = json.load(f); banco = {x['id']: x for x in d}
        buscar_periodo(session, banco, dt_start, dt_end)
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
