import requests, json, os, urllib3, unicodedata, re
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_MANUAIS = 'urls.txt'
ARQ_FINISH = 'finish.txt'

# === PALAVRAS-CHAVE (Refinadas para não pegar falsos positivos) ===
KEYWORDS_GERAIS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", "LUVA", "GAZE", "ALGODAO",
    "AMOXICILIN", "AMPICILIN", "CEFALEXIN", "CEFTRIAXON", "DIPIRON", "PARACETAMOL",
    "INSULIN", "GLICOSE", "HIDROCORTISON", "FUROSEMID", "OMEPRAZOL", "LOSARTAN",
    "ATENOLOL", "SULFATO", "CLORETO", "EQUIPO", "CATETER", "SONDA", "AVENTAL", 
    "MASCARA", "N95", "ALCOOL", "CURATIVO", "ESPARADRAPO", "PROPE", "TOUCA",
    r"EPI\b", r"EPIS\b", "PROTECAO INDIVIDUAL"
]

KEYWORDS_NORDESTE = [
    "DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", "CALORIC", "PROTEIC"
]

# === BLACKLIST EXPANDIDA MÁXIMA ===
BLACKLIST = [
    "ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", 
    "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", 
    "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", 
    "COMODATO", "EXAME", "LIMPEZA", "MANUTENCAO", "ASSISTENCIA MEDICA", 
    "PLANO DE SAUDE", "ODONTOLOGICA", "TERCEIRIZACAO", "EQUIPAMENTO",
    "MERENDA", "COZINHA", "COPA", "HIGIENIZACAO", "EXPEDIENTE", "PAPELARIA",
    "LIXEIRA", "LIXO", "RODO", "VASSOURA", "COMPUTADOR", "IMPRESSORA", "TONER",
    "CARTUCHO", "ELETRODOMESTICO", "MECANICA", "PECAS", "TECIDO", "FARDAMENTO",
    "UNIFORME", "HIDRAULIC", "ELETRIC", "AGRO", "VETERINARI", "ANIMAL", "MUDAS", 
    "SEMENTES", "BELICO", "MILITAR", "ARMAMENTO", "MUNICAO", "DESCARTAVEIS PARA COPA"
]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]
UFS_NORDESTE = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def contem_palavra_relevante(texto, uf):
    if not texto: return False
    # Filtro da Blacklist
    if any(b in texto for b in BLACKLIST): return False
    
    # Busca usando Regex para evitar que "EPI" capture "RECIPIENTE"
    for k in KEYWORDS_GERAIS:
        if re.search(r'\b' + k, texto): return True
        
    if uf in UFS_NORDESTE:
        for k in KEYWORDS_NORDESTE:
            if re.search(r'\b' + k, texto): return True
            
    return False

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens = []
    pag = 1
    while True:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=15)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            if isinstance(dados, list): break # Se não for paginado, quebra o loop
            pag += 1
            if pag > 100: break
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq, itens_raw):
    resultados = []
    urls = [
        f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}/resultados",
        f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    ]
    
    # 1. Tenta buscar todos os resultados de uma vez
    for url in urls:
        pag = 1
        while True:
            try:
                r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=10)
                if r.status_code == 200:
                    dados = r.json()
                    lista = dados.get('data', []) if isinstance(dados, dict) else dados
                    if lista:
                        resultados.extend(lista)
                        if isinstance(dados, list): break 
                        pag += 1
                        continue
                break
            except: break
        if resultados: break

    # 2. GATILHO INFALÍVEL: Se a lista geral falhou, busca item a item nos finalizados
    if not resultados:
        for i in itens_raw:
            situacao = str(i.get('situacaoCompraItemId'))
            # Se o item estiver Homologado(2), Anulado(3), Revogado(4), Fracassado(5) ou Deserto(6)
            if situacao in ["2", "3", "4", "5", "6"]:
                num = i.get('numeroItem') or i.get('sequencialItem')
                if num:
                    url_item = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num}/resultados"
                    try:
                        r = session.get(url_item, timeout=5)
                        if r.status_code == 200:
                            dados = r.json()
                            lista = dados.get('data', []) if isinstance(dados, dict) else dados
                            if lista:
                                resultados.extend(lista)
                    except: pass

    return resultados

def capturar_detalhes_completos(itens_raw, resultados_raw):
    itens_map = {}
    
    mapa_beneficio = {1: "Sim", 2: "Sim", 3: "Sim", 4: "Não", 5: "Não"}
    mapa_situacao_item = {
        1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 
        4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"
    }
    mapa_indicador_resultado = {
        1: "INFORMADO", 2: "FRACASSADO", 3: "DESERTO", 
        4: "ANULADO", 5: "REVOGADO"
    }

    # 1. Mapeia os Itens Base
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
            
            cod_beneficio = i.get('tipoBeneficioId') or 5
            me_epp = mapa_beneficio.get(int(cod_beneficio), "Não")
            
            cod_situacao = i.get('situacaoCompraItemId') or 1
            sit_nome = mapa_situacao_item.get(int(cod_situacao), "EM ANDAMENTO")
            
            fornecedor_padrao = sit_nome if int(cod_situacao) > 2 else "EM ANDAMENTO"
                
            itens_map[num] = {
                "item": num, "desc": i.get('descricao', 'Sem descrição'), "qtd": qtd,
                "unitario_est": unit_est, "total_est": total_est, "situacao": sit_nome,
                "tem_resultado": False, "fornecedor": fornecedor_padrao,
                "unitario_hom": 0.0, "total_hom": 0.0, "me_epp": me_epp
            }
        except: continue

    # 2. Cruza com os Resultados Oficiais
    for res in resultados_raw:
        try:
            num = res.get('numeroItem') or res.get('sequencialItem')
            if num is None: continue
            num = int(num)

            ind_resultado = res.get('indicadorResultadoId')
            
            # Se a API informa explicitamente que é erro/fracasso
            if ind_resultado in [2, 3, 4, 5]:
                sit_resultado = mapa_indicador_resultado.get(ind_resultado)
                if num in itens_map:
                    itens_map[num].update({
                        "tem_resultado": True,
                        "situacao": sit_resultado,
                        "fornecedor": sit_resultado
                    })
            else:
                fornecedor = res.get('nomeRazaoSocialFornecedor') or res.get('nomeFornecedor') or 'VENCEDOR ANÔNIMO'
                unit_hom = float(res.get('valorUnitarioHomologado') or res.get('precoUnitario') or res.get('valorUnitario') or 0)
                total_hom = float(res.get('valorTotalHomologado') or res.get('valorTotal') or 0)
                qtd_hom = float(res.get('quantidadeHomologada') or 1)
                
                if total_hom == 0 and unit_hom > 0:
                    total_hom = unit_hom * qtd_hom

                if num in itens_map:
                    itens_map[num].update({
                        "tem_resultado": True, 
                        "situacao": "HOMOLOGADO",
                        "fornecedor": fornecedor,
                        "unitario_hom": unit_hom, 
                        "total_hom": total_hom
                    })
        except: continue

    return sorted(list(itens_map.values()), key=lambda x: x['item'])

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

                itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
                resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq, itens_raw)
                
                detalhes = capturar_detalhes_completos(itens_raw, resultados_raw)
                unid = lic.get('unidadeOrgao', {})
                banco[id_lic] = {
                    "id": id_lic, "data_pub": lic.get('dataPublicacaoPncp'),
                    "data_encerramento": lic.get('dataEncerramentoProposta'),
                    "uf": unid.get('ufSigla') or lic.get('unidadeFederativaId'),
                    "cidade": unid.get('municipioNome'),
                    "orgao": lic['orgaoEntidade']['razaoSocial'],
                    "unidade_compradora": unid.get('nomeUnidade'),
                    "objeto": lic.get('objetoCompra'),
                    "edital": f"{lic.get('numeroCompra')}/{ano}",
                    "uasg": unid.get('codigoUnidade') or "---",
                    "valor_global": float(lic.get('valorTotalEstimado') or 0),
                    "is_sigiloso": lic.get('niValorTotalEstimado', False),
                    "qtd_itens": len(detalhes), "link": url, "itens": detalhes
                }
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

    processar_urls_manuais(session, banco)

    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write("20260101")
    
    with open(ARQ_CHECKPOINT, 'r') as f: data_str = f.read().strip()
    data_alvo = datetime.strptime(data_str, '%Y%m%d')
    hoje = datetime.now()

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
        params = {
            "dataInicial": str_data, 
            "dataFinal": str_data, 
            "codigoModalidadeContratacao": "6", 
            "pagina": pagina_pub, 
            "tamanhoPagina": 50
        }
        try:
            r = session.get(url_pub, params=params, timeout=25)
            if r.status_code != 200: break
            
            lics = r.json().get('data', [])
            if not lics: break 
            
            print(f"   Lendo página {pagina_pub} do dia...")

            for lic in lics:
                unid = lic.get('unidadeOrgao', {})
                uf = unid.get('ufSigla') or lic.get('unidadeFederativaId')
                if uf in UFS_ALVO:
                    cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
                    id_lic = f"{cnpj}{ano}{seq}"
                    
                    eh_rel = False
                    obj_norm = normalize(lic.get('objetoCompra'))
                    if contem_palavra_relevante(obj_norm, uf):
                        eh_rel = True
                    
                    itens_raw = []
                    resultados_raw = []

                    if not eh_rel:
                        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
                        for it in itens_raw:
                            desc = normalize(it.get('descricao', ''))
                            if contem_palavra_relevante(desc, uf):
                                eh_rel = True
                                break
                    
                    if not eh_rel and not itens_raw:
                        resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq, itens_raw)
                        for res in resultados_raw:
                            desc = normalize(res.get('descricaoItem', ''))
                            if contem_palavra_relevante(desc, uf):
                                eh_rel = True
                                break

                    if eh_rel:
                        if not itens_raw: itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
                        if not resultados_raw: resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq, itens_raw)
                        
                        detalhes = capturar_detalhes_completos(itens_raw, resultados_raw)
                        
                        if detalhes:
                            banco[id_lic] = {
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
                                "qtd_itens": len(detalhes), "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                                "itens": detalhes
                            }
                            novos_no_dia += 1
            
            pagina_pub += 1 
            
        except Exception as e:
            print(f"Erro na varredura da página {pagina_pub}: {e}")
            break

    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista, indent=4, ensure_ascii=False)};")

    proximo_dia = (data_alvo + timedelta(days=1))
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia.strftime('%Y%m%d'))
    print(f"💾 Checkpoint movido para: {proximo_dia.strftime('%d/%m/%Y')} | Licitações salvas: {novos_no_dia}")

    if proximo_dia.date() > hoje.date():
        print("🎉 Alcançamos a data atual! Ciclo de varredura finalizado.")
        with open(ARQ_FINISH, 'w') as f: f.write('done')

if __name__ == "__main__":
    run()
