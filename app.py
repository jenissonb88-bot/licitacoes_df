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
ARQDADOS = 'pregacoes_pharma_limpos.json.gz' # Mantido para o index.html
ARQ_LOCK = 'execucao.lock'
ARQ_CATALOGO = 'Exportar Dados.csv'
ARQ_MANUAL = 'links_manuais.txt' # Sistema de Inclusão Manual mantido
MAXWORKERS = 15 
DATA_CORTE_FIXA = datetime(2025, 12, 1)

# --- GEOGRAFIA (Sua Lógica) ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
EXT_ESTADOS = ['ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

MAPA_SITUACAO = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}

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
                print(f"📚 Catálogo carregado: {len(CATALOGO_TERMOS)} termos.")
                break
            except: continue
    except: pass

# --- VETOS (Sua Lógica de Muros de Contenção) ---
VETOS_ALIMENTACAO = [normalize(x) for x in [
    "ALIMENTACAO ESCOLAR", "GENEROS ALIMENTICIOS", "MERENDA", "PNAE", "PERECIVEIS", 
    "HORTIFRUTI", "CARNES", "PANIFICACAO", "CESTAS BASICAS", "LANCHE", "REFEICOES", 
    "COFFEE BREAK", "BUFFET", "COZINHA", "AÇOUGUE", "POLPA DE FRUTA", "ESTIAGEM"
]]

VETOS_EDUCACAO = [normalize(x) for x in [
    "MATERIAL ESCOLAR", "PEDAGOGICO", "DIDATICO", "BRINQUEDOS", "LIVROS", 
    "TRANSPORTE ESCOLAR", "KIT ALUNO", "REDE MUNICIPAL DE ENSINO", "SECRETARIA DE EDUCACAO"
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

# --- ALVOS (Seus Ímãs) ---
WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO"]]
WL_NUTRI_CLINICA = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA"]]
WL_MATERIAIS_NE = [normalize(x) for x in ["MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA CIRURGICA"]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/12.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital(obj_raw, uf):
    obj = normalize(obj_raw)
    for v in TODOS_VETOS:
        if v in obj:
            if "NUTRICAO" in v or "ALIMENT" in v:
                if any(bom in obj for bom in WL_NUTRI_CLINICA) and "ESCOLAR" not in obj: return False
            return True
    if "LIMPEZA" in obj or "HIGIENE" in obj:
        if not any(x in obj for x in ["HOSPITALAR", "UBS", "SAUDE", "CLINICA"]): return True
    return False

def safe_float(val):
    try: return float(val) if val is not None else 0.0
    except: return 0.0

def safe_int(val, default=4):
    try: return int(val) if val is not None else default
    except: return default

def processar_licitacao(lic, session, forcado=False):
    id_ref = "DESC"
    try:
        if not isinstance(lic, dict): return ('ERRO', {'msg': 'Formato inválido'}, 0, 0)
        
        cnpj = lic.get('orgaoEntidade', {}).get('cnpj', '0000')
        ano = lic.get('anoCompra', '0000')
        seq = lic.get('sequencialCompra', '0000')
        id_ref = f"{cnpj}/{ano}/{seq}"

        uo = lic.get('unidadeOrgao', {})
        uf = uo.get('ufSigla', '').upper()
        obj_raw = lic.get('objetoCompra') or "Sem Objeto"
        obj_norm = normalize(obj_raw)
        
        dt_enc_str = lic.get('dataEncerramentoProposta') or datetime.now().isoformat()
        
        # Lógica de Triagem apenas se não for Inclusão Manual
        if not forcado:
            dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt_enc < DATA_CORTE_FIXA: return ('IGNORADO', None, 0, 0)
            if veta_edital(obj_raw, uf): return ('VETADO', None, 0, 0)

            tem_interesse = False
            if any(t in obj_norm for t in WL_MEDICAMENTOS): tem_interesse = True
            elif uf in NE_ESTADOS and any(t in obj_norm for t in WL_MATERIAIS_NE + WL_NUTRI_CLINICA): tem_interesse = True
            elif "SAUDE" in obj_norm or "HOSPITAL" in obj_norm: tem_interesse = True

            if not tem_interesse: return ('IGNORADO', None, 0, 0)

        # Captura de Itens
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r_itens = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        if r_itens.status_code != 200: return ('ERRO', {'msg': f'HTTP {r_itens.status_code}'}, 0, 0)
        
        resp_json = r_itens.json()
        if isinstance(resp_json, dict): itens_raw = resp_json.get('data', [])
        elif isinstance(resp_json, list): itens_raw = resp_json
        else: return ('IGNORADO', None, 0, 0)

        if not itens_raw: return ('IGNORADO', None, 0, 0)

        itens_limpos = []
        homologados = 0
        tem_item_catalogo = forcado # Se forçado, assume que tem
        
        for it in itens_raw:
            if not isinstance(it, dict): continue
            
            desc = it.get('descricao', '')
            desc_norm = normalize(desc)
            ncm = str(it.get('ncmNbsCodigo', ''))
            
            # Filtro de Lixo Interno
            if any(v in desc_norm for v in ["ARROZ", "FEIJAO", "CARNE", "PNEU", "GASOLINA", "RODA", "LIVRO", "COPO", "CAFE", "ACUCAR"]):
                continue

            # Mantive a adição do NCM aqui como uma camada extra de segurança sem quebrar sua lógica
            if ncm.startswith('30') or any(term in desc_norm for term in CATALOGO_TERMOS):
                tem_item_catalogo = True
            
            sit_id = safe_int(it.get('situacaoCompraItem'), 1)
            status_final = MAPA_SITUACAO.get(sit_id, "EM ANDAMENTO")
            num = it.get('numeroItem')
            res_forn, res_val = None, 0.0
            
            if it.get('temResultado') or sit_id == 2:
                try:
                    r_res = session.get(f"{url_itens}/{num}/resultados", timeout=15)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        res = rl[0] if isinstance(rl, list) and len(rl) > 0 else (rl if isinstance(rl, dict) else None)
                        if res:
                            nf = res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')
                            ni = res.get('niFornecedor')
                            if nf: res_forn = f"{nf} (CNPJ: {ni})" if ni else nf
                            res_val = safe_float(res.get('valorUnitarioHomologado'))
                            if sit_id == 1 and res_forn: status_final = "HOMOLOGADO"
                except: pass

            if status_final == "HOMOLOGADO": homologados += 1

            # CHAVES ADAPTADAS PARA O SEU FRONTEND HTML (index.html)
            itens_limpos.append({
                'n': num, 
                'desc': desc, 
                'qtd': safe_float(it.get('quantidade')),
                'un': it.get('unidadeMedida', 'UN'), 
                'valUnit': safe_float(it.get('valorUnitarioEstimado')),
                'valHomologado': res_val,
                'benef': safe_int(it.get('tipoBeneficioId'), 4),
                'situacao': status_final, 
                'fornecedor': res_forn
            })

        if not itens_limpos: return ('IGNORADO', None, 0, 0)
        
        # A Regra de Ouro Geográfica (aplicada se não for manual)
        if not forcado and uf not in NE_ESTADOS:
            if not tem_item_catalogo and not any(m in obj_norm for m in WL_MEDICAMENTOS):
                 return ('IGNORADO', None, 0, 0)

        # Cálculo do Tipo de Licitação para o HTML
        todos_exclusivos = all(i['benef'] in [1, 2, 3] for i in itens_limpos)
        algum_exclusivo = any(i['benef'] in [1, 2, 3] for i in itens_limpos)
        tipo_lic = "EXCLUSIVO" if todos_exclusivos else ("PARCIAL" if algum_exclusivo else "AMPLO")

        # ESTRUTURA FINAL ADAPTADA PARA O INDEX.HTML
        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 
            'data_enc': dt_enc_str, 
            'uf': uf, 
            'uasg': lic.get('unidadeOrgao', {}).get('codigoUnidade', '---'),
            'orgao': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'), 
            'unidade': lic.get('unidadeOrgao', {}).get('nomeUnidade', '---'),
            'cidade': lic.get('unidadeOrgao', {}).get('municipioNome', '---'), 
            'objeto': obj_raw, 
            'edital': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'valor_estimado': safe_float(lic.get('valorTotalEstimado')), 
            'tipo_licitacao': tipo_lic,
            'itens': itens_limpos
        }
        
        return ('CAPTURADO', dados_finais, len(itens_limpos), homologados)

    except Exception as e: return ('ERRO', {'msg': str(e)}, 0, 0)

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
        open(ARQ_MANUAL, 'w').close() # Limpa após capturar
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
