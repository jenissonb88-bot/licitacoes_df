import requests, json, os, urllib3, unicodedata, re, gzip
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'  # Armazenamento GZIP
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_MANUAIS = 'urls.txt'      # Inclusão manual
ARQ_EXCLUIDOS = 'excluidos.txt' # Exclusão permanente
ARQ_FINISH = 'finish.txt'
MAX_WORKERS = 10 

# === PALAVRAS-CHAVE (MANTIDAS) ===
KEYWORDS_GERAIS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", 
    "SERINGA", "AGULHA", r"\bLUVA", r"\bGAZE", "ALGODAO",
    "AMOXICILIN", "AMPICILIN", "CEFALEXIN", "CEFTRIAXON", "DIPIRON", "PARACETAMOL",
    "INSULIN", "GLICOSE", "HIDROCORTISON", "FUROSEMID", "OMEPRAZOL", "LOSARTAN",
    "ATENOLOL", "SULFATO", "CLORETO", "EQUIPO", "CATETER", "SONDA", "AVENTAL", 
    "MASCARA", "N95", "ALCOOL", "CURATIVO", "ESPARADRAPO", "PROPE", "TOUCA",
    r"\bEPI\b", r"\bEPIS\b", "PROTECAO INDIVIDUAL", "INSUMO"
]

KEYWORDS_NORDESTE = ["DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", "CALORIC", "PROTEIC"]

# === BLACKLIST (MANTIDA) ===
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
    "TROFEU", "MEDALHA", "ELETROPORTATIL", "CAMA MESA e BANHO", "EPI",
    "GENEROS ALIMENTICIOS", "MATERIAL PERMANENTE"
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
    return resultados

def capturar_detalhes_completos(itens_raw, resultados_raw):
    itens_map = {}
    mapa_sit = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "ANULADO", 4: "REVOGADO", 5: "FRACASSADO", 6: "DESERTO"}
    for i in itens_raw:
        try:
            num = i.get('numeroItem') or i.get('sequencialItem')
            if num is None: continue
            num = int(num)
            cod_sit = i.get('situacaoCompraItemId') or 1
            itens_map[num] = {
                "item": num, "desc": i.get('descricao', 'Sem descrição'), "qtd": float(i.get('quantidade') or 0),
                "unitario_est": float(i.get('valorUnitarioEstimado') or 0), "total_est": float(i.get('valorTotalEstimado') or 0),
                "situacao": mapa_sit.get(int(cod_sit), "EM ANDAMENTO"), "fornecedor": "EM ANDAMENTO", "unitario_hom": 0.0
            }
        except: continue
    for res in resultados_raw:
        try:
            num = int(res.get('numeroItem') or res.get('sequencialItem'))
            if num in itens_map:
                itens_map[num].update({
                    "situacao": "HOMOLOGADO", "fornecedor": res.get('nomeRazaoSocialFornecedor', 'VENCEDOR'),
                    "unitario_hom": float(res.get('valorUnitarioHomologado') or 0)
                })
        except: continue
    return sorted(list(itens_map.values()), key=lambda x: x['item'])

def processar_licitacao_individual(session, lic, forcar=False):
    try:
        unid = lic.get('unidadeOrgao', {})
        uf = unid.get('ufSigla') or lic.get('unidadeFederativaId')
        cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
        id_lic = f"{cnpj}{ano}{seq}"
        
        # Se for manual (forcar=True), ignora filtros de UF e Palavras-chave
        if not forcar:
            if uf not in UFS_ALVO: return None
            eh_rel = contem_palavra_relevante(normalize(lic.get('objetoCompra')), uf)
            if not eh_rel:
                itens_temp = buscar_todos_itens(session, cnpj, ano, seq)
                if any(contem_palavra_relevante(normalize(it.get('descricao', '')), uf) for it in itens_temp):
                    eh_rel = True
            if not eh_rel: return None

        itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
        resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq, itens_raw)
        detalhes = capturar_detalhes_completos(itens_raw, resultados_raw)
        
        return {
            "id": id_lic, "data_pub": lic.get('dataPublicacaoPncp'),
            "data_encerramento": lic.get('dataEncerramentoProposta'),
            "uf": uf, "cidade": unid.get('municipioNome'),
            "orgao": lic['orgaoEntidade']['razaoSocial'],
            "objeto": lic.get('objetoCompra'),
            "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
            "itens": detalhes
        }
    except: return None

def carregar_excluidos():
    if not os.path.exists(ARQ_EXCLUIDOS): return set()
    with open(ARQ_EXCLUIDOS, 'r') as f: return {l.strip() for l in f if l.strip()}

def processar_urls_manuais(session, banco):
    if not os.path.exists(ARQ_MANUAIS): return
    with open(ARQ_MANUAIS, 'r') as f: urls = [l.strip() for l in f if 'editais/' in l]
    for url in urls:
        try:
            p = url.split('editais/')[1].split('/')
            r = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{p[0]}/{p[1]}/{p[2]}", timeout=15)
            if r.status_code == 200:
                res = processar_licitacao_individual(session, r.json(), forcar=True)
                if res: banco[res['id']] = res
        except: pass
    with open(ARQ_MANUAIS, 'w') as f: f.write("")

def run():
    session = criar_sessao()
    excluidos = carregar_excluidos()
    banco = {}
    
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                banco = {i['id']: i for i in json.loads(f.read()) if i['id'] not in excluidos}
        except: pass

    processar_urls_manuais(session, banco)

    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(datetime.now().strftime('%Y%m%d'))
    
    with open(ARQ_CHECKPOINT, 'r') as f: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
    if data_alvo.date() > datetime.now().date(): return

    str_d = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Coletando Dia: {data_alvo.strftime('%d/%m/%Y')}")
    
    try:
        r = session.get("https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao", 
                        params={"dataInicial": str_d, "dataFinal": str_d, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50})
        if r.status_code == 200:
            lics = r.json().get('data', [])
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futuros = {executor.submit(processar_licitacao_individual, session, lic): lic for lic in lics}
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result()
                    if res and res['id'] not in excluidos: banco[res['id']] = res
    except: pass

    lista = sorted(banco.values(), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista, f, separators=(',', ':'), ensure_ascii=False)
    
    with open(ARQ_CHECKPOINT, 'w') as f: f.write((data_alvo + timedelta(days=1)).strftime('%Y%m%d'))

if __name__ == "__main__":
    run()
