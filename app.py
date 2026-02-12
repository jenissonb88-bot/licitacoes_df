import requests, json, os, time, urllib3, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Desativar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES DE ARQUIVOS ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_MANUAIS = 'urls.txt'
ARQ_FINISH = 'finish.txt'

# === PALAVRAS-CHAVE DE BUSCA (A-Z) ===
KEYWORDS_SAUDE = [
    "MEDICAMENTO", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGICO", 
    "HIGIENE", "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO", 
    "SAUDE", "INSUMO", "ODONTOLOGICO", "LABORATORIAL", "ENFERMAGEM",
    "AAS", "ABIRATERONA", "ACEBROFILINA", "ACETILCISTEINA", "ACICLOVIR", 
    "ACIDO FOLICO", "ACIDO TRANEXAMICO", "ACIDO URSODESOXICOLICO", "ACIDO VALPROICO", 
    "ADENOSINA", "ADRENALINA", "ALBENDAZOL", "ALFAST", "ALOPURINOL", "ALPRAZOLAM", 
    "AMBROXOL", "AMINOFILINA", "AMIODARONA", "AMITRIPTILINA", "AMOXICILINA", 
    "CLAVULANATO", "AMPICILINA", "SULBACTAM", "ANASTROZOL", "ANFOTERICINA", 
    "ANLODIPINO", "ATENOLOL", "ATORVASTATINA", "ATRACURIO", "ATROPINA", 
    "AZITROMICINA", "AZTREONAM", "ABSORVENTE", "AGUA BI-DESTILADA", 
    "AGUA PARA INJECAO", "HIPODERMICA", "ALCOOL 70", "ALCOOL GEL", "ALCOOL ABSOLUTO", 
    "ALGODAO HIDROFILO", "ALGODAO ORTOPEDICO", "ATADURA", "CREPOM", "GESSADA", 
    "AVENTAL", "BACLOFENO", "BECLOMETASONA", "BETAMETASONA", "BETAXOLOL", "BICARBONATO", 
    "BIMATOPROSTA", "BIPERIDENO", "BISACODIL", "BISOPROLOL", "BORTEZOMIBE", 
    "BOSENTANA", "BROMAZEPAM", "BROMETO DE IPRATROPIO", "BROMOPRIDA", "BUDESONIDA", 
    "BUPIVACAINA", "BUPROPIONA", "BOLSA COLETORA", "COLOSTOMIA", "CABERGOLINA", 
    "CAPECITABINA", "CAPTOPRIL", "CARBAMAZEPINA", "CARBONATO DE CALCIO", 
    "CARBONATO DE LITIO", "CARBOPLATINA", "CARVEDILOL", "CEFALEXINA", "CEFALOTINA", 
    "CEFAZOLINA", "CEFEPIMA", "CEFTAZIDIMA", "CEFTRIAXONA", "CEFUROXIMA", 
    "CETOCONAZOL", "CETOPROFENO", "CETROTIDE", "CICLOBENZAPRINA", "CICLOPENTOLATO", 
    "CIMETIDINA", "CINARIZINA", "CIPROFLOXACINO", "CISATRACURIO", "CITALOPRAM", 
    "CLINDAMICINA", "CLOMIPRAMINA", "CLONAZEPAM", "CLONIDINA", "CLOPIDOGREL", 
    "CLORETO DE POTASSIO", "CLORETO DE SODIO", "CLORPROMAZINA", "CODEINA", 
    "COLAGENASE", "COMPLEXO B", "CAMPO CIRURGICO", "CATETER", "INTRAVENOSO", 
    "OCULOS", "CLOREXIDINA", "COBERTURA", "COLETOR", "PERFUROCORTANTE", "URINA", 
    "COMPRESSA", "GAZE", "CURATIVO", "DANTROLENO", "EFEDRINA", "ENALAPRIL", 
    "ENOXAPARINA", "ESCINA", "ESCITALOPRAM", "ESMOLOL", "ESOMEPRAZOL", 
    "ESPIRONOLACTONA", "ESTRIOL", "ETILEFRINA", "ETOMIDATO", "EXEMESTANO", 
    "EQUIPO", "ESCOVA", "ESPARADRAPO", "ETER", "FENITOINA", "FENOBARBITAL", 
    "FENTANILA", "FLUCONAZOL", "FLUMAZENIL", "FLUOCINOLONA", "FLUOXETINA", 
    "FOSFATO DE SODIO", "FUROSEMIDA", "FILTRO HME", "FRALDA", "GABAPENTINA", 
    "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICLAZIDA", "GLICOSE", 
    "GLIMEPIRIDA", "GLUCONATO DE CALCIO", "GLUTARALDEIDO", "HALOPERIDOL", 
    "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA", 
    "HIDROXIDO DE ALUMINIO", "HIDROXIUREIA", "HIOSCINA", "ESCOPOLAMINA", 
    "IBUPROFENO", "IMIPRAMINA", "ISOFLURANO", "ISOSSORBIDA", "ISOTRETINOINA", 
    "ITRACONAZOL", "LACTULOSE", "LAMOTRIGINA", "LATANOPROSTA", "LEVETIRACETAM", 
    "LEVOBUPIVACAINA", "LEVODOPA", "LEVOFLOXACINO", "LEVOMEPROMAZINA", 
    "LEVONORGESTREL", "LEVOSIMENDANA", "LEVOTIROXINA", "LIDOCAINA", 
    "LISDEXANFETAMINA", "LORATADINA", "LOSARTANA", "LENCOL HOSPITALAR", 
    "LUVA", "MEROPENEM", "METADONA", "METARAMINOL", "METFORMINA", "METILDOPA", 
    "METILERGOMETRINA", "METILPREDNISOLONA", "METOCLOPRAMIDA", "METOPROLOL", 
    "METRONIDAZOL", "MICONAZOL", "MIDAZOLAM", "MIRTAZAPINA", "MONTELUCASTE", 
    "MORFINA", "MUPIROCINA", "MASCARA", "N95", "NEBULIZACAO", "NALBUFINA", 
    "NALOXONA", "NALTREXONA", "NEOMICINA", "NEOSTIGMINA", "NIFEDIPINO", 
    "NIMESULIDA", "NINTEDANIBE", "NISTATINA", "NITROGLICERINA", "NITROPRUSSIATO", 
    "NOREPINEFRINA", "NORTRIPTILINA", "OCTREOTIDA", "OLANZAPINA", "OMEPRAZOL", 
    "ONDANSETRONA", "OXACILINA", "OXCARBAZEPINA", "OXIBUPROCAINA", "OXITOCINA", 
    "PAMIDRONATO", "PANCURONIO", "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", 
    "PENICILINA", "PERMETRINA", "PILOCARPINA", "PIPERACILINA", "POLIMIXINA", 
    "PREDNISOLONA", "PREDNISONA", "PREGABALINA", "PROMETAZINA", "PROPOFOL", 
    "PROPRANOLOL", "QUETIAPINA", "REMIFENTANILA", "RISPERIDONA", "RIVAROXABANA", 
    "ROCURONIO", "ROPIVACAINA", "ROSUVASTATINA", "SALBUTAMOL", "SERTRALINA", 
    "SEVOFLURANO", "SIMETICONA", "SINVASTATINA", "SUCCINATO", "SUFENTANILA", 
    "SUGAMADEX", "SULFADIAZINA", "SUNITINIBE", "SUXAMETONIO", "PROPE", 
    "SONDA", "SORO", "SUPORTE", "SUPLEMENTO", "TEICOPLANINA", "TEMOZOLOMIDA", 
    "TENOXICAM", "TERBUTALINA", "TIAMINA", "TIGECICLINA", "TIMOLOL", 
    "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", "TRAMADOL", "TRAVOPROSTA", 
    "TROMETAMOL", "TROPICAMIDA", "TOALHA PAPEL", "TORNEIRA", "TOUCA", 
    "TUBO", "VALSARTANA", "VANCOMICINA", "VARFARINA", "VASOPRESSINA", 
    "VENLAFAXINA", "VITAMINA", "VORICONAZOL", "VASELINA"
]

BLACKLIST = ["ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", "COMODATO", "EXAME", "LIMPEZA PREDIAL"]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

# === FUNÇÕES DE APOIO ===

def normalize(t): 
    return ''.join(c for c in unicodedata.normalize('NFD', str(t or "")).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(t):
    txt = normalize(t)
    if any(b in txt for b in BLACKLIST): return False
    return any(k in txt for k in KEYWORDS_SAUDE)

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def salvar_banco_disco(banco):
    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista, indent=4, ensure_ascii=False)};")

def capturar_detalhes(session, cnpj, ano, seq):
    url_base = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
    itens_map = {}

    try:
        r = session.get(f"{url_base}/itens", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for i in r.json():
                num = int(i['numeroItem'])
                qtd = float(i.get('quantidade') or 0)
                unit_est = float(i.get('valorUnitarioEstimado') or 0)
                total_est = float(i.get('valorTotalEstimado') or 0)
                if total_est == 0 and qtd > 0 and unit_est > 0:
                    total_est = round(qtd * unit_est, 2)

                itens_map[num] = {
                    "item": num, "desc": i.get('descricao', 'Sem descrição'), "qtd": qtd,
                    "unitario_est": unit_est, "total_est": total_est, "situacao": "ABERTO",
                    "tem_resultado": False, "fornecedor": "EM ANDAMENTO",
                    "unitario_hom": 0.0, "total_hom": 0.0
                }
    except: pass

    try:
        r = session.get(f"{url_base}/resultados", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for res in r.json():
                num = int(res['numeroItem'])
                if num not in itens_map:
                    itens_map[num] = {
                        "item": num, "desc": res.get('descricaoItem', 'Item Resultado'),
                        "qtd": float(res.get('quantidadeHomologada') or 0),
                        "unitario_est": float(res.get('valorUnitarioHomologado') or 0),
                        "total_est": float(res.get('valorTotalHomologado') or 0),
                        "situacao": "HOMOLOGADO", "tem_resultado": True,
                        "fornecedor": "", "unitario_hom": 0.0, "total_hom": 0.0
                    }
                itens_map[num].update({
                    "tem_resultado": True, "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor', 'VENCEDOR ANÔNIMO'),
                    "unitario_hom": float(res.get('valorUnitarioHomologado') or 0),
                    "total_hom": float(res.get('valorTotalHomologado') or 0)
                })
    except: pass
    return sorted(list(itens_map.values()), key=lambda x: x['item'])

def processar_urls_manuais(session, banco):
    if not os.path.exists(ARQ_MANUAIS): return 0
    with open(ARQ_MANUAIS, 'r') as f:
        urls = [line.strip() for line in f.readlines() if 'pncp.gov.br' in line]
    
    count = 0
    for url in urls:
        try:
            parts = url.split('/editais/')[1].split('/')
            if len(parts) < 3: continue
            cnpj, ano, seq = parts[0], parts[1], parts[2]
            id_lic = f"{cnpj}{ano}{seq}"
            api_url = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
            resp = session.get(api_url, timeout=15)
            if resp.status_code == 200:
                lic = resp.json()
                itens = capturar_detalhes(session, cnpj, ano, seq)
                banco[id_lic] = montar_objeto_licitacao(lic, itens, url)
                count += 1
        except: pass
    return count

def montar_objeto_licitacao(lic, itens, link_manual=None):
    orgao = lic.get('orgaoEntidade', {})
    unidade = lic.get('unidadeOrgao', {})
    cnpj, ano, seq = orgao.get('cnpj'), lic.get('anoCompra'), lic.get('sequencialCompra')
    return {
        "id": f"{cnpj}{ano}{seq}",
        "data_pub": lic.get('dataPublicacaoPncp'),
        "data_encerramento": lic.get('dataEncerramentoProposta'),
        "uf": unidade.get('ufSigla') or lic.get('unidadeFederativaId'),
        "cidade": unidade.get('municipioNome'),
        "orgao": orgao.get('razaoSocial'),
        "unidade_compradora": unidade.get('nomeUnidade'),
        "objeto": lic.get('objetoCompra'),
        "edital": f"{lic.get('numeroCompra')}/{ano}",
        "uasg": unidade.get('codigoUnidade') or "---",
        "valor_global": float(lic.get('valorTotalEstimado') or 0),
        "is_sigiloso": lic.get('niValorTotalEstimado', False),
        "qtd_itens": len(itens),
        "link": link_manual or f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
        "itens": itens
    }

# === EXECUÇÃO PRINCIPAL (UM DIA POR VEZ) ===

def run():
    session = criar_sessao()
    banco = {}
    
    # 1. Carrega banco anterior
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw: banco = {i['id']: i for i in json.loads(raw)}
        except: pass

    # 2. Checkpoint da Data
    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write("20260101")
    
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_str = f.read().strip()
    
    data_alvo = datetime.strptime(data_str, '%Y%m%d')
    hoje = datetime.now()

    # Se já processou tudo até hoje, sinaliza fim
    if data_alvo.date() > hoje.date():
        print(f"✅ Finalizado. Todas as datas processadas até {hoje.strftime('%d/%m')}.")
        with open(ARQ_FINISH, 'w') as f: f.write('true')
        return

    print(f"🚀 [Tarefa] Processando dia: {data_alvo.strftime('%d/%m/%Y')}")

    # 3. Processa Manuais e o Dia Alvo
    processar_urls_manuais(session, banco)
    
    str_data = data_alvo.strftime('%Y%m%d')
    pagina = 1
    novos_no_dia = 0
    
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {"dataInicial": str_data, "dataFinal": str_data, "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50}
        try:
            r = session.get(url, params=params, timeout=20)
            if r.status_code != 200: break
            dados = r.json().get('data', [])
            if not dados: break

            for lic in dados:
                if eh_relevante(lic.get('objetoCompra')):
                    cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
                    id_lic = f"{cnpj}{ano}{seq}"
                    
                    # Busca detalhes (Em bot iterativo, sempre atualizamos para pegar resultados)
                    itens = capturar_detalhes(session, cnpj, ano, seq)
                    if itens:
                        banco[id_lic] = montar_objeto_licitacao(lic, itens)
                        novos_no_dia += 1
            pagina += 1
        except: break

    # 4. Salva e Move Checkpoint
    salvar_banco_disco(banco)
    
    proximo_dia = (data_alvo + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia)
    
    print(f"💾 Checkpoint: {proximo_dia} | Registros no dia: {novos_no_dia}")

if __name__ == "__main__":
    run()
