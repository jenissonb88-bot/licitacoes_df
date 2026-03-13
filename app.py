import requests
import json
import os
import unicodedata
import gzip
import concurrent.futures
import sys
import time
import random
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ORIGINAIS E DE SEGURANÇA ---
ARQDADOS = 'dadosoportunidades.json.gz' 
ARQ_DICIONARIO = 'dicionario_ouro.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_LOCK = 'execucao.lock'
ARQ_LOG = 'log_captura.txt'

MAXWORKERS = 4 

# --- GEOGRAFIA DE PRECISÃO ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
UFS_PERMITIDAS_MED = NE_ESTADOS + ['DF', 'ES', 'MG', 'RJ', 'SP', 'GO', 'MT', 'MS', 'AM', 'PA', 'TO', 'BR', '']
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', 'BR', '']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- VETOS ABSOLUTOS ---
VETOS_ABSOLUTOS = [normalize(x) for x in [
    # Serviços e TI
    "SOFTWARE", "IMPLANTACAO", "LICENCA", "COMPUTADOR", "INFORMATICA", "IMPRESSAO",
    "PRESTACAO DE SERVICO", "TERCEIRIZACAO", "LOCACAO", "ASSINATURA", "LIMPEZA",
    "VIGILANCIA", "SEGURANCA", "OFICINA",
    
    # Saúde (Fora do escopo)
    "EXAME", "RADIOLOGIA", "IMAGEM", "GASES MEDICINAIS", "OXIGENIO", 
    "ODONTOLOGICO", "PROTESE", "ORTESE", "CILINDRO",
    
    # Obras e Educação
    "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "ALIMENTACAO ESCOLAR", "MERENDA",
    
    # 🛑 VETOS ADMINISTRATIVOS E DE MODALIDADE 🛑
    "ADESAO", "IRP", "INTENCAO DE REGISTRO", "CREDENCIAMENTO", "LEILAO", "CHAMAMENTO PUBLICO"
]]

WL_MEDICAMENTOS = [normalize(x) for x in ["MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA"]]
WL_NUTRI_MMH = [normalize(x) for x in ["NUTRICAO ENTERAL", "FORMULA INFANTIL", "DIETA ENTERAL", "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "GAZE", "SONDA", "LUVA"]]
WL_TERMOS_VAGOS = [normalize(x) for x in ["SAUDE", "HOSPITAL", "MATERNIDADE", "CLINICA", "FUNDO MUNICIPAL", "SECRETARIA DE"]]

def log_mensagem(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def criar_sessao():
    s = requests.Session()
    s.headers.update({
        'Accept': 'application/json', 
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive'
    })
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def buscar_todos_os_itens(cnpj, ano, seq, session):
    todos_itens = []
    pagina_item = 1
    erro_msg = None
    
    while True:
        url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        try:
            time.sleep(random.uniform(0.5, 1.5))
            
            r = session.get(url, params={'pagina': pagina_item, 'tamanhoPagina': 500}, timeout=30)
            
            if r.status_code != 200: 
                erro_msg = f"HTTP {r.status_code}"
                break
            
            json_resp = r.json()
            
            if isinstance(json_resp, list):
                dados = json_resp
            elif isinstance(json_resp, dict):
                dados = json_resp.get('data', [])
            else:
                dados = []
                
            if not dados: break
            
            todos_itens.extend(dados)
            if len(dados) < 500: break
            pagina_item += 1
            
        except requests.exceptions.ReadTimeout:
            erro_msg = "Timeout (Servidor lento)"
            break
        except Exception as e: 
            erro_msg = f"Erro de ligação: {str(e)[:40]}"
            break
            
    return todos_itens, erro_msg

def processar_licitacao(lic, session, termos_ouro):
    try:
        uo = lic.get('unidadeOrgao', {})
        uf = str(uo.get('ufSigla') or 'BR').upper().strip()
        obj_raw = lic.get('objetoCompra') or ""
        obj_norm = normalize(obj_raw)
        edit = f"{lic.get('numeroCompra')}/{lic.get('anoCompra')}"

        if uf in ESTADOS_BLOQUEADOS: return ('VETO_GEO', None)
        for v in VETOS_ABSOLUTOS:
            if v in obj_norm: return ('VETO_TITULO', None)

        tem_med = any(t in obj_norm for t in WL_MEDICAMENTOS)
        tem_mmh = any(t in obj_norm for t in WL_NUTRI_MMH)
        tem_vago = any(t in obj_norm for t in WL_TERMOS_VAGOS)

        precisa_checar_itens = False
        if tem_med:
            if uf not in UFS_PERMITIDAS_MED: return ('VETO_GEO', None)
        elif tem_mmh:
            if uf not in UFS_PERMITIDAS_MMH: return ('VETO_GEO', None)
        elif tem_vago:
            precisa_checar_itens = True
        else:
            return ('FORA_TEMATICA', None)

        cnpj = lic['orgaoEntidade']['cnpj']
        ano = lic['anoCompra']
        seq = lic['sequencialCompra']
        
        itens_brutos, erro_api = buscar_todos_os_itens(cnpj, ano, seq, session)
        
        if erro_api: 
            return ('ERRO_API', f"{edit} -> {erro_api}")

        teve_match = False
        itens_mapeados = []
        for it in itens_brutos:
            desc_item = normalize(it.get('descricao', ''))
            if not teve_match and any(termo in desc_item for termo in termos_ouro):
                teve_match = True
            
            # ✅ CAPTURA DE ME/EPP
            itens_mapeados.append({
                'n': it.get('numeroItem'), 'd': it.get('descricao', ''),
                'q': it.get('quantidade'), 'u': it.get('unidadeMedida', 'UN'),
                'v_est': it.get('valorUnitarioEstimado', 0), 'sit': 'EM ANDAMENTO',
                'benef': it.get('tipoBeneficio', 0) 
            })

        if precisa_checar_itens and not teve_match:
            return ('VETO_DICIONARIO', None)

        cid = uo.get('municipioNome', '---')
        uasg = uo.get('codigoUnidade', 'N/A')
        unid_nome = uo.get('nomeUnidade', '---')

        dados_finais = {
            'id': f"{cnpj}{ano}{seq}", 
            'dt_enc': lic.get('dataEncerramentoProposta'),
            'uf': uf, 
            'org': lic.get('orgaoEntidade', {}).get('razaoSocial', '---'),
            'cid': cid,
            'uasg': uasg,
            'unid_nome': unid_nome,
            'obj': obj_raw, 
            'edit': edit, 
            'link': f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            'itens': itens_mapeados, 
            'sit_global': 'DIVULGADA'
        }
        return ('CAPTURADO', dados_finais)

    except Exception as e: return ('ERRO_API', "Exceção Interna")

if __name__ == '__main__':
    if os.path.exists(ARQ_LOCK): sys.exit(0)
    with open(ARQ_LOCK, 'w') as f: f.write("lock")

    try:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()
        
        if args.start: data_alvo = args.start
        elif os.path.exists(ARQ_CHECKPOINT):
            with open(ARQ_CHECKPOINT, 'r') as f: data_alvo = f.read().strip()
        else: data_alvo = date.today().strftime('%Y-%m-%d')

        log_mensagem(f"🚀 Iniciando Varredura Antibloqueio: {data_alvo}")
        
        with open(ARQ_DICIONARIO, 'r', encoding='utf-8') as f:
            termos_ouro = [normalize(t) for t in json.load(f)]

        session = criar_sessao()
        banco = {}
        if os.path.exists(ARQDADOS):
            with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                for x in json.load(f): banco[x['id']] = x

        dia_api = data_alvo.replace('-', '')
        pagina = 1
        
        stats = {
            'CAPTURADO': 0, 'VETO_GEO': 0, 'VETO_TITULO': 0, 
            'VETO_DICIONARIO': 0, 'FORA_TEMATICA': 0, 'ERRO_API': 0
        }
        
        while True:
            r = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao", 
                            params={'dataInicial': dia_api, 'dataFinal': dia_api, 'codigoModalidadeContratacao': 6, 'pagina': pagina, 'tamanhoPagina': 50})
            if r.status_code != 200: break
            lics = r.json().get('data', [])
            if not lics: break
            
            log_mensagem(f"📄 Pag {pagina}: Processando {len(lics)} editais no PNCP...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session, termos_ouro): l for l in lics}
                for f in concurrent.futures.as_completed(futuros):
                    status, resultado = f.result()
                    stats[status] += 1
                    
                    if status == 'CAPTURADO':
                        # 🚨 DETETOR DE MUDANÇAS NAS DATAS 🚨
                        if resultado['id'] in banco:
                            lic_antiga = banco[resultado['id']]
                            antiga_data = lic_antiga.get('dt_enc')
                            nova_data = resultado.get('dt_enc')
                            
                            if antiga_data and nova_data and antiga_data != nova_data:
                                resultado['alerta_data'] = True
                                resultado['dt_enc_antiga'] = antiga_data
                                log_mensagem(f"   ⚠️ DATA ALTERADA: {resultado['edit']} (Era {antiga_data})")
                            
                            elif lic_antiga.get('alerta_data'):
                                resultado['alerta_data'] = True
                                resultado['dt_enc_antiga'] = lic_antiga.get('dt_enc_antiga')

                        banco[resultado['id']] = resultado
                        log_mensagem(f"   🎯 ALVO REGISTADO: {resultado['edit']} - {resultado['org'][:30]}")
                        
                    elif status == 'ERRO_API':
                        log_mensagem(f"   ⚠️ API FALHOU: {resultado}")

            if len(lics) < 50: break
            pagina += 1

        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, ensure_ascii=False)
            
        if not args.start:
            proximo = (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
            
        total_analisado = sum(stats.values())
        log_mensagem("="*50)
        log_mensagem(f"📊 RESUMO DIÁRIO: {data_alvo}")
        log_mensagem("="*50)
        log_mensagem(f"✅ CAPTURADOS (Sniper/Medicamentos): {stats['CAPTURADO']}")
        log_mensagem(f"❌ DESCARTADOS - Geografia (Bloqueada): {stats['VETO_GEO']}")
        log_mensagem(f"❌ DESCARTADOS - Veto no Título: {stats['VETO_TITULO']}")
        log_mensagem(f"❌ DESCARTADOS - Veto Dicionário (Sem produtos): {stats['VETO_DICIONARIO']}")
        log_mensagem(f"❌ DESCARTADOS - Fora da Temática: {stats['FORA_TEMATICA']}")
        log_mensagem(f"⚠️ ERROS DE API: {stats['ERRO_API']}")
        log_mensagem("-" * 50)
        log_mensagem(f"TOTAL ANALISADO: {total_analisado} editais")
        log_mensagem("="*50)

    finally:
        if os.path.exists(ARQ_LOCK): os.remove(ARQ_LOCK)
