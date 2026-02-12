import requests, json, os, time, urllib3, concurrent.futures, zipfile, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Desativar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
CNPJ_ALVO = "08778201000126"   # DROGAFONTE
ARQ_ZIP = 'dados_pncp.zip'
ARQ_JSON_INTERNO = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'

# === FILTROS ===
KEYWORDS_SAUDE = [
    "MEDICAMENTO", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGICO", 
    "HIGIENE", "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO"
]
BLACKLIST = [
    "ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", 
    "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", 
    "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO"
]

# Lista Exata de Estados Solicitados
UFS_ALVO = [
    "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE",  # Nordeste
    "ES", "MG", "RJ", "SP",                                # Sudeste
    "AM", "PA", "TO", "RO",                                # Norte selecionado
    "GO", "MT", "MS", "DF"                                 # Centro-Oeste
]

def normalize(t): 
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(t):
    txt = normalize(t)
    return not any(b in txt for b in BLACKLIST) and any(k in txt for k in KEYWORDS_SAUDE)

def capturar_vencedores(session, it, url_base):
    """
    Captura os vencedores (resultados) ou retorna o item estimado se não houver resultado.
    """
    num = it.get('numeroItem')
    desc = it.get('descricao', '')
    
    # Filtro de Blacklist no item
    if any(b in normalize(desc) for b in BLACKLIST): return None

    # Objeto padrão: Item Aberto / Em Andamento (Usa valores ESTIMADOS)
    item_padrao = {
        "item": num,
        "desc": desc,
        "qtd": it.get('quantidade'),
        "unitario": it.get('valorUnitarioEstimado'),
        "total": it.get('valorTotalEstimado'),
        "fornecedor": "EM ANDAMENTO / SEM RESULTADO",
        "vitoria": False
    }

    try:
        # Tenta buscar resultados (vencedores homologados)
        url_res = f"{url_base}/{num}/resultados"
        r = session.get(url_res, timeout=15)
        
        if r.status_code == 200:
            data_resp = r.json()
            # Normaliza resposta para lista, pois pode vir dict ou list
            vends = data_resp.get('data', []) if isinstance(data_resp, dict) else data_resp
            if isinstance(vends, dict): vends = [vends]

            # Se encontrou vencedores, retorna a lista com dados HOMOLOGADOS
            if vends and len(vends) > 0:
                resultados = []
                for v in vends:
                    resultados.append({
                        "item": num,
                        "desc": desc,
                        "qtd": v.get('quantidadeHomologada'),
                        "unitario": v.get('valorUnitarioHomologado'),
                        "total": v.get('valorTotalHomologado'),
                        "fornecedor": v.get('nomeRazaoSocialFornecedor'),
                        "vitoria": (CNPJ_ALVO in (v.get('niFornecedor') or ""))
                    })
                return resultados
    except:
        pass # Se der erro na API de resultados, falha silenciosamente e usa o estimado
    
    # Se não tem resultado ou deu erro, retorna o item estimado para não perder a informação
    return [item_padrao]

def set_github_output(key, value):
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"{key}={value}\n")

def run():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    # 1. Checkpoint
    cp = open(ARQ_CHECKPOINT).read().strip() if os.path.exists(ARQ_CHECKPOINT) else "20250101"
    try: data_atual = datetime.strptime(cp, '%Y%m%d')
    except: data_atual = datetime.now()

    if data_atual.date() >= datetime.now().date():
        print("📅 Dados atualizados até hoje.")
        set_github_output("CONTINUAR_EXECUCAO", "false")
        return

    print(f"🚀 Sniper PNCP | Processando: {data_atual.strftime('%d/%m/%Y')}")

    # 2. Carregar e Limpar Banco Antigo
    banco = {}
    if os.path.exists(ARQ_ZIP):
        try:
            with zipfile.ZipFile(ARQ_ZIP, 'r') as z:
                if ARQ_JSON_INTERNO in z.namelist():
                    dados_brutos = json.load(z.open(ARQ_JSON_INTERNO))
                    print(f"📦 Carregados {len(dados_brutos)} registros antigos.")
                    mantidos = 0
                    for l in dados_brutos:
                        if l.get('uf') in UFS_ALVO and eh_relevante(l.get('objeto')):
                            chave = l.get('id') or l.get('id_licitacao')
                            if chave: 
                                banco[chave] = l
                                mantidos += 1
                    print(f"🧹 Base limpa: {mantidos} mantidos.")
        except Exception as e: print(f"⚠️ Erro ZIP: {e}")

    # 3. Coleta do Dia
    pagina = 1
    novos_registros = 0
    
    while True:
        try:
            print(f"  > Varrendo Página {pagina}...")
            url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": data_atual.strftime('%Y%m%d'),
                "dataFinal": data_atual.strftime('%Y%m%d'),
                "codigoModalidadeContratacao": "6", # Pregão
                "pagina": pagina,
                "tamanhoPagina": 50
            }
            resp = session.get(url_pub, params=params, timeout=20)
            if resp.status_code != 200: break
            
            lics = resp.json().get('data', [])
            if not lics: break

            for lic in lics:
                unidade_orgao = lic.get('unidadeOrgao', {})
                uf_sigla = unidade_orgao.get('ufSigla')
                
                if uf_sigla not in UFS_ALVO: continue 

                if eh_relevante(lic.get('objetoCompra') or ""):
                    orgao = lic.get('orgaoEntidade', {})
                    cnpj = orgao.get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    
                    if not (cnpj and ano and seq): continue
                    id_lic = f"{cnpj}{ano}{seq}"
                    
                    if id_lic not in banco:
                        # --- FORMATAÇÃO DE CAMPOS ---
                        num_compra = lic.get('numeroCompra')
                        str_edital = f"Edital nº {num_compra}/{ano}" if (num_compra and ano) else "Edital S/N"
                        str_uasg = str(unidade_orgao.get('codigoUnidade') or "---")
                        
                        # NOVOS CAMPOS: CIDADE E UNIDADE
                        str_cidade = unidade_orgao.get('municipioNome') or ""
                        str_unidade_compradora = unidade_orgao.get('nomeUnidade') or ""

                        # Data de Atualização
                        raw_dt_att = lic.get('dataAtualizacao', '')
                        try:
                            dt_obj = datetime.fromisoformat(str(raw_dt_att))
                            str_att = dt_obj.strftime('%Y%m%d')
                        except:
                            str_att = data_atual.strftime('%Y%m%d')
                        
                        # --- PAGINAÇÃO DE ITENS ---
                        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
                        lista_itens_completa = []
                        pag_item = 1
                        while True:
                            try:
                                r_it = session.get(url_itens, params={"pagina": pag_item, "tamanhoPagina": 50}, timeout=15)
                                if r_it.status_code != 200: break
                                batch = r_it.json()
                                if not batch: break
                                lista_itens_completa.extend(batch)
                                pag_item += 1
                                if pag_item > 200: break
                            except: break

                        itens_finais = []
                        # Processamento paralelo para buscar vencedores de cada item
                        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                            futures = [ex.submit(capturar_vencedores, session, it, url_itens) for it in lista_itens_completa]
                            for f in concurrent.futures.as_completed(futures):
                                res = f.result()
                                if res: itens_finais.extend(res)
                        
                        # Salva se tiver itens (agora inclui itens sem resultado)
                        if itens_finais:
                            banco[id_lic] = {
                                "id": id_lic,
                                "uf": uf_sigla,
                                "cidade": str_cidade,
                                "unidade": str_unidade_compradora,
                                "data": data_atual.strftime('%Y%m%d'),
                                "data_att": str_att,
                                "orgao": orgao.get('razaoSocial'),
                                "objeto": lic.get('objetoCompra'),
                                "edital": str_edital,
                                "uasg": str_uasg,
                                "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                                "itens": itens_finais
                            }
                            novos_registros += 1
            
            pagina += 1
            time.sleep(1)
            
        except Exception as e:
            print(f"Erro Loop: {e}")
            break

    print(f"✅ Fim. Novos: {novos_registros}")

    # 4. Salvar
    with open(ARQ_JSON_INTERNO, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, ensure_ascii=False, indent=2)
    
    with zipfile.ZipFile(ARQ_ZIP, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(ARQ_JSON_INTERNO)
    
    if os.path.exists(ARQ_JSON_INTERNO): os.remove(ARQ_JSON_INTERNO)

    # 5. Checkpoint
    proximo_dia = (data_atual + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia)

    if datetime.strptime(proximo_dia, '%Y%m%d').date() < datetime.now().date():
        set_github_output("CONTINUAR_EXECUCAO", "true")
    else:
        set_github_output("CONTINUAR_EXECUCAO", "false")

if __name__ == "__main__":
    run()
