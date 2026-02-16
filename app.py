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

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQ_LOCK = 'execucao.lock'
MAXWORKERS = 6 # Leve aumento para compensar a carga extra de atualização

def normalize(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t) or '').upper()
                   if unicodedata.category(c) != 'Mn')

def formatar_data_pncp(data_obj):
    if isinstance(data_obj, date):
        return data_obj.strftime('%Y%m%d')
    return data_obj.strftime('%Y%m%d')

def criar_sessao():
    s = requests.Session()
    # Retry agressivo para evitar falhas de conexão
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    itens = []; pag = 1
    while True:
        url = f'https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens'
        try:
            r = session.get(url, params={'pagina': pag, 'tamanhoPagina': 50}, timeout=20)
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

def e_pharma_saude(lic):
    # 1. Verifica no Objeto
    obj = normalize(lic.get('objetoCompra') or lic.get('objeto', ''))
    # 2. Verifica no Nome da Unidade (Captura "Fundo Municipal de Saude", etc.)
    unid = normalize(lic.get('unidadeOrgao', {}).get('nomeUnidade', ''))
    
    termos_gatilho = [
        'MEDICAMENT', 'FARMAC', 'HOSPITAL', 'SAUDE', 'ODONTO', 'ENFERMAGEM',
        'MATERIAL MEDICO', 'INSUMO', 'LUVA', 'SERINGA', 'AGULHA', 'LABORATORI',
        'FRALDA', 'ABSORVENTE', 'REMEDIO', 'SORO', 'HIPERTENSIV', 'INJETAV', 
        'ONCOLOGIC', 'ANALGESIC', 'ANTI-INFLAMAT', 'ANTIBIOTIC', 'ANTIDEPRESSIV', 
        'ANSIOLITIC', 'DIABETIC', 'GLICEMIC', 'CONTROLAD', 'MATERIAL PENSO', 
        'DIETA', 'FORMULA', 'PROTEIC', 'CALORIC', 'GAZE', 'ATADURA', 'MMH'
    ]
    
    if any(t in obj for t in termos_gatilho): return True
    if any(t in unid for t in ['SAUDE', 'HOSPITAL', 'FUNDO MUNICIPAL']): return True
    
    return False

def processar_licitacao(lic_resumo, session):
    # Nota: Removemos a checagem de "já existe" aqui dentro.
    # Se foi chamado, é para processar e atualizar.
    try:
        if not e_pharma_saude(lic_resumo): return None

        cnpj = lic_resumo['orgaoEntidade']['cnpj']
        ano = lic_resumo['anoCompra']
        seq = lic_resumo['sequencialCompra']
        id_lic = f"{cnpj}{ano}{seq}"
        
        # --- BAIXANDO TUDO FRESQUINHO ---
        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        if not itens_raw: return None # Se não tem itens, não serve

        resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq)
        
        # Mapa de Vencedores (Chave: Numero Item -> Valor: Dados Resultado)
        mapa_res = {}
        for r in resultados_raw:
            try: mapa_res[int(r['numeroItem'])] = r
            except: pass

        itens_limpos = []
        for item in itens_raw:
            try:
                num = int(item.get('numeroItem'))
                
                # Captura Inteligente de ME/EPP
                bid = 4
                raw_bid = item.get('tipoBeneficio') or item.get('tipoBeneficioId')
                if isinstance(raw_bid, dict): bid = raw_bid.get('value') or raw_bid.get('id', 4)
                elif raw_bid is not None: bid = raw_bid
                
                res_match = mapa_res.get(num)
                sit_txt = str(item.get('situacaoCompraItemName', '')).upper()
                
                status_final = "ABERTO"
                if res_match: status_final = "HOMOLOGADO"
                elif any(x in sit_txt for x in ["CANCELADO", "FRACASSADO", "DESERTO"]): status_final = sit_txt

                item_obj = {
                    'n': num, 
                    'd': item.get('descricao', ''), 
                    'q': float(item.get('quantidade', 0)),
                    'u': item.get('unidadeMedida', ''), 
                    'v_est': float(item.get('valorUnitarioEstimado', 0)),
                    'benef': bid, 
                    'sit': status_final
                }

                if res_match:
                    # AQUI ESTÁ A CORREÇÃO DO NOME DO FORNECEDOR
                    item_obj['res_forn'] = res_match.get('nomeRazaoSocialFornecedor') or res_match.get('razaoSocial')
                    item_obj['res_val'] = float(res_match.get('valorUnitarioHomologado', 0))

                itens_limpos.append(item_obj)
            except: continue

        unid = lic_resumo.get('unidadeOrgao', {})
        
        # Retorna objeto Slim pronto para salvar
        return {
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
            'itens': itens_limpos, 
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        return None

def buscar_dia_completo(session, data_obj, banco):
    dstr = formatar_data_pncp(data_obj)
    url_pub = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao'
    total_capturados = 0
    pag = 1
    
    print(f"🔎 Varrendo {dstr} em busca de atualizações...")
    
    while True:
        params = {'dataInicial': dstr, 'dataFinal': dstr, 'codigoModalidadeContratacao': 6, 'pagina': pag}
        try:
            r = session.get(url_pub, params=params, timeout=30)
            if r.status_code != 200: break
            dados = r.json()
            lics = dados.get('data', [])
            if not lics: break
            
            # Filtra candidatos
            pharma_lics = [l for l in lics if e_pharma_saude(l)]
            
            # Processamento Paralelo
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
                # Dispara processamento para TODOS os encontrados no filtro (sem checar se já existe)
                # Isso garante que a gente pegue a versão mais recente dos resultados
                futuros = [exe.submit(processar_licitacao, l, session) for l in pharma_lics]
                
                for futuro in concurrent.futures.as_completed(futuros):
                    res = futuro.result()
                    if res:
                        # Atualiza/Sobrescreve no Banco
                        banco[res['id']] = res
                        total_capturados += 1
                        
                        # Log detalhado
                        n_res = sum(1 for i in res['itens'] if 'res_forn' in i)
                        print(f"   DISK-WRITE: {res['uf']} - {res['edit']} | Vencedores: {n_res}")
            
            if len(lics) < 50: break
            pag += 1
        except Exception as e:
            print(f"Erro Paginação: {e}")
            break
    return total_capturados

if __name__ == '__main__':
    print(f"🚀 SNIPER PHARMA V4.0 (Sync Total & Lock)")
    
    # 1. VERIFICAÇÃO DE TRAVA (LOCK)
    if os.path.exists(ARQ_LOCK):
        print(f"⚠️  ALERTA: Arquivo '{ARQ_LOCK}' encontrado.")
        print("   Outro robô parece estar em execução. Abortando para evitar conflito.")
        sys.exit(0) # Encerra com sucesso (0) para não quebrar pipelines, mas não faz nada.
    
    # Cria a trava
    try:
        with open(ARQ_LOCK, 'w') as f: f.write(datetime.now().isoformat())
    except:
        print("Erro ao criar arquivo de trava.")
        sys.exit(1)

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=str); parser.add_argument('--end', type=str)
        args = parser.parse_args()

        session = criar_sessao()
        banco = {}
        
        # Carrega Banco Existente
        if os.path.exists(ARQDADOS):
            try:
                with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
                    d = json.load(f)
                    banco = {i['id']: i for i in d}
                print(f"📦 Banco carregado: {len(banco)} registros.")
            except: 
                print("⚠️ Banco ilegível ou vazio. Iniciando novo.")

        datas = []
        if args.start and args.end:
            dt_ini = datetime.strptime(args.start, '%Y-%m-%d').date()
            dt_fim = datetime.strptime(args.end, '%Y-%m-%d').date()
            for i in range((dt_fim - dt_ini).days + 1): datas.append(dt_ini + timedelta(days=i))
        else:
            # PADRÃO: Retroagir 5 dias para pegar homologações recentes
            hoje = date.today()
            for i in range(6): # 0 a 5 (Hoje + 5 dias para trás)
                datas.append(hoje - timedelta(days=i))
            print(f"📅 Modo Automático: Verificando de {datas[-1]} até {datas[0]}")

        # Execução
        total_atualizados = 0
        for dia in datas:
            total_atualizados += buscar_dia_completo(session, dia, banco)

        # Salvamento
        if total_atualizados > 0:
            print(f"\n💾 Salvando {len(banco)} registros (Atualizados: {total_atualizados})...")
            with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
                json.dump(list(banco.values()), f, ensure_ascii=False)
        else:
            print("\n💤 Nenhuma alteração encontrada nos dias verificados.")

    except Exception as e:
        print(f"❌ Erro fatal: {e}")
    finally:
        # 3. REMOÇÃO DA TRAVA (SEMPRE EXECUTA)
        if os.path.exists(ARQ_LOCK):
            os.remove(ARQ_LOCK)
            print("🔓 Trava removida. Fim.")
