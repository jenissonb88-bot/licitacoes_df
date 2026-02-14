import requests, json, os, urllib3, unicodedata, re
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures  # Biblioteca para Paralelismo

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_MANUAIS = 'urls.txt'
ARQ_FINISH = 'finish.txt'
MAX_WORKERS = 20  # AUMENTADO PARA 20 (Turbo Máximo)

# === PALAVRAS-CHAVE ===
KEYWORDS_GERAIS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", r"\bLUVA", r"\bGAZE", "ALGODAO",
    "AMOXICILIN", "AMPICILIN", "CEFALEXIN", "CEFTRIAXON", "DIPIRON", "PARACETAMOL",
    "INSULIN", "GLICOSE", "HIDROCORTISON", "FUROSEMID", "OMEPRAZOL", "LOSARTAN",
    "ATENOLOL", "SULFATO", "CLORETO", "EQUIPO", "CATETER", "SONDA", "AVENTAL", 
    "MASCARA", "N95", "ALCOOL", "CURATIVO", "ESPARADRAPO", "PROPE", "TOUCA",
    r"\bEPI\b", r"\bEPIS\b", "PROTECAO INDIVIDUAL", "INSUMO"
]

KEYWORDS_NORDESTE = [
    "DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", "CALORIC", "PROTEIC"
]

# === BLACKLIST BLINDADA MÁXIMA ===
BLACKLIST = [
    "ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", 
    "LANCHE", "ALIMENTICIO", "MOBILIARIO", r"\bTI\b", "INFORMATICA", "PNEU", 
    "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", 
    "COMODATO", "EXAME", "LIMPEZA", "MANUTENCAO", "ASSISTENCIA MEDICA", 
    "PLANO DE SAUDE", "ODONTOLOGICA", "TERCEIRIZACAO", "EQUIPAMENTO",
    "MERENDA", "COZINHA", "COPA", "HIGIENIZACAO", "EXPEDIENTE", "PAPELARIA",
    "LIXEIRA", "LIXO", "RODO", "VASSOURA", "COMPUTADOR", "IMPRESSORA", "TONER",
    "CARTUCHO", "ELETRODOMESTICO", "MECANICA", "PECA", "TECIDO", "FARDAMENTO",
    "UNIFORME", "HIDRAULIC", "ELETRIC", "AGRO", "VETERINARI", "ANIMAL", "MUDA", 
    "SEMENTE", "BELICO", "MILITAR", "ARMAMENTO", "MUNICAO", "SOFTWARE", "SAAS",
    "PISCINA", "CIMENTO", "ASFALTO", "BRINQUEDO", "EVENTO", "SHOW", "FESTA",
    "GRAFICA", "PUBLICIDADE", "MARKETING", "PASSAGEM", "HOSPEDAGEM",
    "AR CONDICIONADO", "TELEFONIA", "INTERNET", "LINK DE DADOS", "SEGURO", "COPO",
    "MATERIAL ESPORTIVO", "ESPORTE", "MATERIAL DE CONSTRUCAO", "MATERIAL ESCOLAR", 
    "MATERIAL DE EXPEDIENTE", "MATERIAL HIDRAULICO", "MATERIAL ELETRICO",
    "DIDATICO", "PEDAGOGICO", "FERRAGEM", "FERRAMENTA", "PINTURA", "TINTA", 
    "MARCENARIA", "MADEIRA", "AGRICOLA", "JARDINAGEM", "ILUMINACAO", "DECORACAO", 
    "AUDIOVISUAL", "FOTOGRAFICO", "MUSICAL", "INSTRUMENTO MUSICAL", "BRINDE", 
    "TROFEU", "MEDALHA", "ELETROPORTATIL", "CAMA MESA E BANHO", "EPI"
]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]
UFS_NORDESTE = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def contem_palavra_relevante(texto, uf):
    if not texto: return False
    for b in BLACKLIST:
        padrao = b if r"\b" in b else r"\b" + b
        if re.search(padrao, texto): return False
    for k in KEYWORDS_GERAIS:
        padrao = k if r"\b" in k else r"\b" + k
        if re.search(padrao, texto): return True
    if uf in UFS_NORDESTE:
        for k in KEYWORDS_NORDESTE:
            padrao = k if r"\b" in k else r"\b" + k
            if re.search(padrao, texto): return True
    return False

def criar_sessao():
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=Retry(total=3, backoff_factor=0.5, status_forcelist=[500,502,503,504]))
    s.mount("https://", adapter)
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens = []
    pag = 1
    # Limite seguro de 200 páginas
    while pag <= 200: 
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=15)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break 
            itens.extend(lista)
            if len(lista) < 50: break 
            pag += 1
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq, itens_raw):
    resultados = []
    urls = [
        f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}/resultados",
        f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    ]
    
    # 1. Busca Geral
    for url in urls:
        pag = 1
        while pag <= 200:
            try:
                r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=10)
                if r.status_code == 200:
                    dados = r.json()
                    lista = dados.get('data', []) if isinstance(dados, dict) else dados
                    if lista:
                        resultados.extend(lista)
                        if len(lista) < 50: break
                        pag += 1
                        continue
                break 
            except: break
        if resultados: break 

    # 2. Gatilho Individual (Infalível)
    for i in itens_raw:
        num = i.get('numeroItem') or i.get('sequencialItem')
        if num is None: continue
        
        ja_tem = any(str(r.get('numeroItem') or r.get('sequencialItem')) == str(num) for r in resultados)
        tem_res = i.get('temResultado', False)
        sit = str(i.get('situacaoCompraItemId'))
        
        if (tem_res or sit in ["2", "3", "4", "5", "6"]) and not ja_tem:
            url_item = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados"
            try:
                r = session.get(url_item, timeout=5)
                if r.status_code == 200:
                    dados = r.json()
                    lista = dados.get('data', []) if isinstance(dados, dict) else dados
                    if lista:
                        for res_item in lista:
                            if 'numeroItem' not in res_item:
                                res_item['numeroItem'] = num
                            resultados.append(res_item)
            except: pass

    return resultados

def capturar_detalhes_completos(itens_raw, resultados_raw):
    itens_map = {}
    mapa_beneficio = {1: "Sim", 2: "Sim", 3: "Sim", 4: "Não", 5: "Não"}
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}
    mapa_ind = {1: "INFORMADO", 2: "FRACASSADO", 3: "DESERTO", 4: "ANULADO", 5: "REVOGADO"}

    for i in itens_raw:
        try:
            num = i.get('numeroItem') or i.get('sequencialItem')
            if num is None: continue
            num = int(num)
            
            qtd = float(i.get('quantidade') or 0)
            unit_est = float(i.get('valorUnitarioEstimado') or 0)
            total_est = float(i.get('valorTotalEstimado') or 0)
            if total_est == 0 and qtd > 0 and unit_est > 0:
                total_est = round(qtd * unit_est, 2)
            
            cod_ben = i.get('tipoBeneficioId') or 5
            cod_sit = i.get('situacaoCompraItemId') or 1
            sit_nome = mapa_sit.get(int(cod_sit), "EM ANDAMENTO")
            
            itens_map[num] = {
                "item": num, "desc": i.get('descricao', 'Sem descrição'), "qtd": qtd,
                "unitario_est": unit_est, "total_est": total_est, "situacao": sit_nome,
                "tem_resultado": False, "fornecedor": sit_nome if int(cod_sit) > 2 else "EM ANDAMENTO",
                "unitario_hom": 0.0, "total_hom": 0.0, "me_epp": mapa_beneficio.get(int(cod_ben), "Não")
            }
        except: continue

    for res in resultados_raw:
        try:
            num = res.get('numeroItem') or res.get('sequencialItem')
            if num is None: continue
            num = int(num)

            ind_res = int(res.get('indicadorResultadoId') or 1)
            
            if ind_res in [2, 3, 4, 5]:
                sit_res = mapa_ind.get(ind_res)
                if num in itens_map:
                    itens_map[num].update({"tem_resultado": True, "situacao": sit_res, "fornecedor": sit_res})
            else:
                forn = res.get('nomeRazaoSocialFornecedor') or res.get('nomeFornecedor') or 'VENCEDOR ANÔNIMO'
                u_hom = float(res.get('valorUnitarioHomologado') or res.get('precoUnitario') or res.get('valorUnitario') or 0)
                t_hom = float(res.get('valorTotalHomologado') or res.get('valorTotal') or 0)
                q_hom = float(res.get('quantidadeHomologada') or 0)
                
                if t_hom == 0 and u_hom > 0:
                    q = q_hom if q_hom > 0 else (itens_map[num]['qtd'] if num in itens_map else 1)
                    t_hom = u_hom * q

                if num in itens_map and (u_hom > 0 or t_hom > 0 or forn != 'VENCEDOR ANÔNIMO'):
                    itens_map[num].update({
                        "tem_resultado": True, "situacao": "HOMOLOGADO",
                        "fornecedor": forn, "unitario_hom": u_hom, "total_hom": t_hom
                    })
        except: continue

    return sorted(list(itens_map.values()), key=lambda x: x['item'])

# === FUNÇÃO: ATUALIZAR LICITAÇÃO EXISTENTE (PARA USO EM PARALELO) ===
def atualizar_licitacao_existente(session, lic, hoje):
    # Retorna: (id_lic, lic_atualizada) ou None se nada mudou
    # Também retorna (id_lic, None) se deve ser excluída (lixo)
    
    id_lic = lic.get('id')
    uf = lic.get('uf', '')
    obj_norm = normalize(lic.get('objeto', ''))
    
    # 1. Filtro de Lixo (Blacklist Retroativa)
    eh_rel = False
    if contem_palavra_relevante(obj_norm, uf):
        eh_rel = True
    else:
        for it in lic.get('itens', []):
            if contem_palavra_relevante(normalize(it.get('desc', '')), uf):
                eh_rel = True
                break
    
    if not eh_rel:
        return (id_lic, None) # Marca para deletar

    # 2. Atualização de Resultados (Se já venceu e está pendente)
    atualizou = False
    try:
        data_enc_str = lic.get('data_encerramento')
        if data_enc_str:
            data_enc = datetime.strptime(data_enc_str[:10], '%Y-%m-%d')
            # Se a data já passou
            if data_enc.date() <= hoje.date():
                tem_pendente = any(i.get('situacao') == 'EM ANDAMENTO' for i in lic.get('itens', []))
                # Se ainda tem item pendente, vai na API buscar atualização
                if tem_pendente:
                    cnpj, ano, seq = id_lic[:14], id_lic[14:18], id_lic[18:]
                    itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
                    resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq, itens_raw)
                    if itens_raw:
                        lic['itens'] = capturar_detalhes_completos(itens_raw, resultados_raw)
                        atualizou = True
    except: pass
    
    return (id_lic, lic) if atualizou else (id_lic, "mantem")

# === FUNÇÃO: PROCESSAR NOVA LICITAÇÃO ===
def processar_licitacao_individual(session, lic):
    try:
        unid = lic.get('unidadeOrgao', {})
        uf = unid.get('ufSigla') or lic.get('unidadeFederativaId')
        if uf not in UFS_ALVO: return None

        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        id_lic = f"{cnpj}{ano}{seq}"
        
        eh_rel = False
        obj_norm = normalize(lic.get('objetoCompra'))
        if contem_palavra_relevante(obj_norm, uf): eh_rel = True
        
        itens_raw = []
        resultados_raw = []

        if not eh_rel:
            itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
            for it in itens_raw:
                if contem_palavra_relevante(normalize(it.get('descricao', '')), uf):
                    eh_rel = True
                    break
        
        if eh_rel:
            if not itens_raw: itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
            resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq, itens_raw)
            detalhes = capturar_detalhes_completos(itens_raw, resultados_raw)
            
            if detalhes:
                return {
                    "id": id_lic, "data_pub": lic.get('dataPublicacaoPncp'),
                    "data_encerramento": lic.get('dataEncerramentoProposta'),
                    "uf": uf, "cidade": unid.get('municipioNome'),
                    "orgao": lic['orgaoEntidade']['razaoSocial'],
                    "unidade_compradora": unid.get('nomeUnidade'),
                    "objeto": lic.get('objetoCompra'),
                    "edital": f"{lic.get('numeroCompra')}/{ano}",
                    "uasg": unid.get('codigoUnidade') or "---",
                    "valor_global": float(lic.get('valorTotalEstimado') or 0),
                    "is_sigiloso": lic.get('niValorTotalEstimado', False),
                    "qtd_itens": len(detalhes), 
                    "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                    "itens": detalhes
                }
    except: pass
    return None

def processar_urls_manuais(session, banco):
    if not os.path.exists(ARQ_MANUAIS): return 0
    with open(ARQ_MANUAIS, 'r') as f:
        urls = [line.strip() for line in f.readlines() if 'editais/' in line]
    if not urls: return 0
    
    count = 0
    for url in urls:
        try:
            parts = url.split('editais/')[1].split('/')
            cnpj, ano, seq = parts[0], parts[1], parts[2]
            id_lic = f"{cnpj}{ano}{seq}"
            api_url = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
            resp = session.get(api_url, timeout=15)
            
            if resp.status_code == 200:
                lic = resp.json()
                if str(lic.get('modalidadeId')) != "6": continue
                res = processar_licitacao_individual(session, lic)
                if res:
                    res['link'] = url 
                    banco[id_lic] = res
                    count += 1
        except: pass
        
    with open(ARQ_MANUAIS, 'w') as f: f.write("")
    return count

def run():
    if os.path.exists(ARQ_FINISH): os.remove(ARQ_FINISH)

    session = criar_sessao()
    banco = {}
    
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw: banco = {i['id']: i for i in json.loads(raw)}
        except: pass

    hoje = datetime.now()

    # === FASE 1: LIMPEZA E ATUALIZAÇÃO DA BASE (AGORA EM PARALELO!) ===
    if banco:
        print(f"🔄 Revisando {len(banco)} itens da base antiga (TURBO ATIVADO)...")
        ids_para_remover = []
        
        # Usamos ThreadPool para atualizar as antigas também!
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futuros = {executor.submit(atualizar_licitacao_existente, session, lic, hoje): id_lic for id_lic, lic in banco.items()}
            
            for futuro in concurrent.futures.as_completed(futuros):
                try:
                    id_lic, resultado = futuro.result()
                    if resultado is None: # Marca para deletar (Lixo)
                        ids_para_remover.append(id_lic)
                    elif resultado != "mantem": # Se houve atualização de dados
                        banco[id_lic] = resultado
                except: pass

        for id_lic in ids_para_remover:
            if id_lic in banco: del banco[id_lic]
            
        print("✅ Base antiga revisada e atualizada.")

    processar_urls_manuais(session, banco)

    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write("20260101")
    
    with open(ARQ_CHECKPOINT, 'r') as f: data_str = f.read().strip()
    data_alvo = datetime.strptime(data_str, '%Y%m%d')

    if data_alvo.date() > hoje.date():
        print("✅ Todas as datas até hoje já foram processadas!")
        with open(ARQ_FINISH, 'w') as f: f.write('done')
        return

    str_data = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Coletando Dia: {data_alvo.strftime('%d/%m/%Y')}")

    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    novos_no_dia = 0
    pagina_pub = 1 

    while True:
        params = {"dataInicial": str_data, "dataFinal": str_data, "codigoModalidadeContratacao": "6", "pagina": pagina_pub, "tamanhoPagina": 50}
        try:
            r = session.get(url_pub, params=params, timeout=25)
            if r.status_code != 200: break
            
            lics = r.json().get('data', [])
            if not lics: break 
            
            print(f"   Lendo página {pagina_pub} do dia (Processamento Paralelo)...")

            # === FASE 2: COLETA DE NOVAS (PARALELO) ===
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futuros = {executor.submit(processar_licitacao_individual, session, lic): lic for lic in lics}
                
                for futuro in concurrent.futures.as_completed(futuros):
                    resultado = futuro.result()
                    if resultado:
                        banco[resultado['id']] = resultado
                        novos_no_dia += 1
            
            pagina_pub += 1 
            
        except Exception as e:
            print(f"Erro na varredura da página {pagina_pub}: {e}")
            break

    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    
    # SALVAMENTO COMPACTADO
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json_minificado = json.dumps(lista, separators=(',', ':'), ensure_ascii=False)
        f.write(f"const dadosLicitacoes = {json_minificado};")

    proximo_dia = (data_alvo + timedelta(days=1))
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia.strftime('%Y%m%d'))
    print(f"💾 Checkpoint movido para: {proximo_dia.strftime('%d/%m/%Y')} | Licitações salvas: {novos_no_dia}")

    if proximo_dia.date() > hoje.date():
        print("🎉 Alcançamos a data atual! Ciclo de varredura finalizado.")
        with open(ARQ_FINISH, 'w') as f: f.write('done')

if __name__ == "__main__":
    run()
