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

# --- CONFIGURAÇÕES DE DIAGNÓSTICO ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 3  # Reduzido para facilitar a leitura do log
DATA_CORTE_FIXA = datetime(2025, 12, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
EXT_ESTADOS = ['ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- REGRAS (As mesmas da versão anterior) ---
VETOS_ABSOLUTOS = [normalize(x) for x in ["ADESAO", "INTENCAO", "IRP", "BUFFET", "EVENTOS", "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "EXAME LABORATORI", "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "VIGILANCIA", "LOCACAO", "PNEU", "VEICULO", "INFORMATICA"]]
RESGATES_MEDICOS = [normalize(x) for x in ["PROCEDIMENTO", "CIRURGICO", "ESTERIL", "EXAME", "70%", "GEL", "HOSPITALAR", "AMBIDESTRA", "LATEX", "NITRILICA"]]
VETOS_CONTEXTUAIS = [normalize(x) for x in ["JARDINAGEM", "COZINHA", "DOMESTICA", "LIMPEZA", "PVC", "RASPA", "VAQUETA"]]

WL_GLOBAL_MEDS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOAL", "COMPRIMIDO", "INJETAVEL"]]
WL_NE_MATS_DIETAS = [normalize(x) for x in ["MMH", "INSUMO", "MATERIAL MEDIC", "DIETA", "NUTRI", "FORMULA", "EQUIPO", "SERINGA", "SONDA"]]
WL_GENERICO_SAUDE = [normalize(x) for x in ["SAUDE", "HOSPITAL", "FMS", "FARMACIA", "UNIDADE BASICA"]]

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def veta_edital_diagnostico(obj_raw, uf):
    obj = normalize(obj_raw)
    
    # Check Veto Absoluto
    for v in VETOS_ABSOLUTOS:
        if v in obj:
            # Exceção de Dietas no NE
            if uf in NE_ESTADOS and any(x in obj for x in ["DIETA", "FORMULA", "NUTRICIONAL"]):
                return False, None
            return True, v # Retorna qual palavra vetou

    # Check Veto Contextual
    if any(x in obj for x in ["LUVA", "ALCOOL", "LIMPEZA"]):
        for v in VETOS_CONTEXTUAIS:
            if v in obj:
                if not any(r in obj for r in RESGATES_MEDICOS):
                    return True, f"{v} (Sem Resgate)"
    return False, None

def processar_licitacao(lic, session):
    id_compra = f"{lic['orgaoEntidade']['cnpj']}{lic['anoCompra']}{lic['sequencialCompra']}"
    obj_raw = lic.get('objetoCompra') or "SEM OBJETO"
    
    try:
        # 1. Diagnóstico de Data
        dt_enc_str = lic.get('dataEncerramentoProposta')
        if not dt_enc_str:
            print(f" ⚠️ DATA NULA: {obj_raw[:30]}...")
            return None
            
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA: 
            return None # Data antiga silenciosa (normal)

        # 2. Diagnóstico de UF
        uf = (lic.get('unidadeOrgao', {}).get('ufSigla') or '').upper()
        if uf not in (NE_ESTADOS + EXT_ESTADOS): 
            return None # UF fora do escopo (normal)

        # 3. Diagnóstico de Veto
        vetado, motivo = veta_edital_diagnostico(obj_raw, uf)
        if vetado:
            print(f" 🚫 VETADO [{motivo}]: {obj_raw[:50]}...")
            return None

        # 4. Diagnóstico de Whitelist (Interesse)
        obj_norm = normalize(obj_raw)
        tem_interesse = False
        motivo_interesse = ""

        # Teste Global
        if any(t in obj_norm for t in WL_GLOBAL_MEDS + WL_GENERICO_SAUDE):
            tem_interesse = True
            motivo_interesse = "GLOBAL/SAUDE"
        # Teste NE
        elif uf in NE_ESTADOS and any(t in obj_norm for t in WL_NE_MATS_DIETAS):
            tem_interesse = True
            motivo_interesse = "MAT/DIETA NE"
        
        if not tem_interesse:
            # AQUI ESTÁ O PULO DO GATO: Vamos ver o que estamos perdendo
            print(f" ⚠️ IGNORADO (Sem termo chave): {obj_raw[:60]}...")
            return None

        # 5. Captura de Itens
        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        url_itens = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        r_itens = session.get(url_itens, params={'pagina': 1, 'tamanhoPagina': 100}, timeout=20)
        
        if r_itens.status_code != 200:
            print(f" 🔥 ERRO API ITENS ({r_itens.status_code}): {obj_raw[:30]}...")
            return None
            
        itens_raw = r_itens.json().get('data', [])
        
        if not itens_raw:
            print(f" ⚠️ ZERO ITENS NA API: {obj_raw[:40]}...")
            return None

        itens_limpos = []
        for it in itens_raw:
            num = it.get('numeroItem')
            res = None
            if it.get('temResultado'):
                try:
                    r_res = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados", timeout=15)
                    if r_res.status_code == 200:
                        rl = r_res.json()
                        res = rl[0] if isinstance(rl, list) and rl else rl
                except: pass

            itens_limpos.append({
                'n': num, 'd': it.get('descricao', ''), 'q': float(it.get('quantidade', 0)),
                'u': it.get('unidadeMedida', ''), 'v_est': float(it.get('valorUnitarioEstimado', 0)),
                'benef': it.get('tipoBeneficioId') or 4,
                'sit': "HOMOLOGADO" if res else str(it.get('situacaoCompraItemName', 'ABERTO')).upper(),
                'res_forn': (res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')) if res else None,
                'res_val': float(res.get('valorUnitarioHomologado') or 0) if res else 0
            })

        print(f" ✅ CAPTURADO [{motivo_interesse}]: {obj_raw[:50]}... | {len(itens_limpos)} itens")

        return {
            'id': id_compra, 'dt_enc': dt_enc_str, 'uf': uf, 
            'uasg': lic['unidadeOrgao'].get('codigoUnidade', '---'),
            'org': lic['orgaoEntidade']['razaoSocial'], 'unid_nome': lic['unidadeOrgao'].get('nomeUnidade', '---'),
            'cid': lic['unidadeOrgao'].get('municipioNome'), 'obj': obj_raw, 
            'edit': f"{str(lic.get('numeroCompra', '')).zfill(5)}/{ano}",
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}", 
            'val_tot': float(lic.get('valorTotalEstimado') or 0), 'itens': itens_limpos
        }

    except Exception as e:
        print(f" 🔥 ERRO CRÍTICO NO EDITAL {obj_raw[:20]}...: {str(e)}")
        return None

def buscar_periodo(session, banco, d_ini, d_fim):
    delta = d_fim - d_ini
    for i in range(delta.days + 1):
        dia = (d_ini + timedelta(days=i)).strftime('%Y%m%d')
        print(f"\n📅 --- DIAGNÓSTICO: {dia} ---")
        url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
        pag = 1
        while True:
            r = session.get(url, params={'dataInicial': dia, 'dataFinal': dia, 'codigoModalidadeContratacao': 6, 'pagina': pag, 'tamanhoPagina': 50}, timeout=30)
            if r.status_code != 200: 
                print(f" 🔥 ERRO API BUSCA: {r.status_code}")
                break
            dados = r.json(); lics = dados.get('data', [])
            if not lics: 
                print(" ⚠️ Sem editais nesta página.")
                break
            
            print(f" 📄 Pág {pag}: Analisando {len(lics)} editais...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = [exe.submit(processar_licitacao, l, session) for l in lics]
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res: banco[res['id']] = res
            
            if pag >= dados.get('totalPaginas', 1): break
            pag += 1

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
