import requests, json, os, time, urllib3, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_MANUAIS = 'urls.txt'

# === PALAVRAS-CHAVE ===
KEYWORDS_SAUDE = [
    # --- Genéricos ---
    "MEDICAMENTO", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGICO", 
    "HIGIENE", "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO", 
    "SAUDE", "INSUMO", "ODONTOLOGICO", "LABORATORIAL", "ENFERMAGEM",

    # --- Específicos (Sua Lista) ---
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
    "COMPRESSA", "CREME DERMOPROTETOR", "CURATIVO", "DANTROLENO", "EFEDRINA", 
    "ENALAPRIL", "ENOXAPARINA", "ESCINA", "ESCITALOPRAM", "ESMOLOL", "ESOMEPRAZOL", 
    "ESPIRONOLACTONA", "ESTRIOL", "ETILEFRINA", "ETOMIDATO", "EXEMESTANO", "EQUIPO", 
    "MACROGOTAS", "MICROGOTAS", "ESCOVA", "DEGERMACAO", "ESPARADRAPO", "ETER", 
    "FENITOINA", "FENOBARBITAL", "FENTANILA", "FLUCONAZOL", "FLUMAZENIL", 
    "FLUOCINOLONA", "FLUOXETINA", "FOSFATO DE SODIO", "FOSFOENEMA", "FUROSEMIDA", 
    "FILTRO HME", "FILTRO VIRAL", "FITA CIRURGICA", "AUTOCLAVE", "FRALDA", 
    "GABAPENTINA", "GENCITABINA", "GENTAMICINA", "GLIBENCLAMIDA", "GLICLAZIDA", 
    "GLICOSE", "GLIMEPIRIDA", "GLUCONATO DE CALCIO", "GLUTARALDEIDO", "GAZE EM ROLO", 
    "HALOPERIDOL", "HEPARINA", "HIDRALAZINA", "HIDROCLOROTIAZIDA", "HIDROCORTISONA", 
    "HIDROXIDO DE ALUMINIO", "HIDROXIUREIA", "HIOSCINA", "ESCOPOLAMINA", 
    "IBUPROFENO", "IMIPRAMINA", "ISOFLURANO", "ISOSSORBIDA", "ISOTRETINOINA", 
    "ITRACONAZOL", "LACTULOSE", "LAMOTRIGINA", "LATANOPROSTA", "LEVETIRACETAM", 
    "LEVOBUPIVACAINA", "LEVODOPA", "LEVOFLOXACINO", "LEVOMEPROMAZINA", "LEVONORGESTREL", 
    "LEVOSIMENDANA", "LEVOTIROXINA", "LIDOCAINA", "LISDEXANFETAMINA", "LORATADINA", 
    "LOSARTANA", "LENCOL HOSPITALAR", "LUVA CIRURGICA", "LUVA PROCEDIMENTO", 
    "MEROPENEM", "METADONA", "METARAMINOL", "METFORMINA", "METILDOPA", 
    "METILERGOMETRINA", "METILPREDNISOLONA", "METOCLOPRAMIDA", "METOPROLOL", 
    "METRONIDAZOL", "MICONAZOL", "MIDAZOLAM", "MIRTAZAPINA", "MONTELUCASTE", 
    "MORFINA", "MUPIROCINA", "MASCARA CIRURGICA", "MASCARA N95", "NEBULIZACAO", 
    "MONITOR GLICEMIA", "TIRAS", "NALBUFINA", "NALOXONA", "NALTREXONA", "NEOMICINA", 
    "NEOSTIGMINA", "NIFEDIPINO", "NIMESULIDA", "NINTEDANIBE", "NISTATINA", 
    "NITROGLICERINA", "NITROPRUSSIATO", "NOREPINEFRINA", "NORTRIPTILINA", 
    "OCTREOTIDA", "OLANZAPINA", "OMEPRAZOL", "ONDANSETRONA", "OXACILINA", 
    "OXCARBAZEPINA", "OXIBUPROCAINA", "OXITOCINA", "OLEO DERSANI", "OLEO AGE", 
    "PAMIDRONATO", "PANCURONIO", "PANTOPRAZOL", "PARACETAMOL", "PAROXETINA", 
    "PENICILINA", "PERMETRINA", "PILOCARPINA", "PIPERACILINA", "TAZOBACTAM", 
    "POLIMIXINA", "PREDNISOLONA", "PREDNISONA", "PREGABALINA", "PROMETAZINA", 
    "PROPOFOL", "PROPRANOLOL", "QUETIAPINA", "REMIFENTANILA", "RISPERIDONA", 
    "RIVAROXABANA", "ROCURONIO", "ROPIVACAINA", "ROSUVASTATINA", "SACCHAROMYCES", 
    "BOULARDII", "SALBUTAMOL", "SENNA", "SERTRALINA", "SEVOFLURANO", "SIMETICONA", 
    "SINVASTATINA", "SUCCINATO", "SUFENTANILA", "SUGAMADEX", "SULFADIAZINA", 
    "SULFATO DE MAGNESIO", "SULFATO DE ZINCO", "SUNITINIBE", "SUXAMETONIO", 
    "SAPATILHA", "PROPE", "SERINGA INSULINA", "SONDA ASPIRACAO", "SONDA FOLEY", 
    "SONDA NASOGASTRICA", "SORO FISIOLOGICO", "SORO GLICOSADO", "SUPORTE", 
    "SUPLEMENTO", "TEICOPLANINA", "TEMOZOLOMIDA", "TENOXICAM", "TERBUTALINA", 
    "TIAMINA", "TIGECICLINA", "TIMOLOL", "TIORIDAZINA", "TOBRAMICINA", "TOPIRAMATO", 
    "TRAMADOL", "TRAVOPROSTA", "TROMETAMOL", "TROPICAMIDA", "TOALHA PAPEL", 
    "TORNEIRA 3 VIAS", "TOUCA", "TUBO ENDOTRAQUEAL", "TUBO ENSAIO", "VALSARTANA", 
    "VANCOMICINA", "VARFARINA", "VASOPRESSINA", "VENLAFAXINA", "VITAMINA C", 
    "VITAMINA K", "VORICONAZOL", "VASELINA"
]

BLACKLIST = ["ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", "COMODATO", "EXAME", "LIMPEZA PREDIAL"]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

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

# === NOVA FUNÇÃO DE SALVAMENTO INCREMENTAL ===
def salvar_banco_disco(banco):
    """ Salva o banco atual no arquivo JS para garantir persistência """
    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista, indent=4, ensure_ascii=False)};")

def capturar_detalhes(session, cnpj, ano, seq):
    url_base = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
    itens_map = {}

    # 1. Busca ITENS (Edital)
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
                    "item": num,
                    "desc": i.get('descricao', 'Sem descrição'),
                    "qtd": qtd,
                    "unitario_est": unit_est,
                    "total_est": total_est,
                    "situacao": "ABERTO",
                    "tem_resultado": False,
                    "fornecedor": "EM ANDAMENTO",
                    "unitario_hom": 0.0,
                    "total_hom": 0.0
                }
    except: pass

    # 2. Busca RESULTADOS (Homologação)
    try:
        r = session.get(f"{url_base}/resultados", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for res in r.json():
                num = int(res['numeroItem'])
                if num not in itens_map:
                    itens_map[num] = {
                        "item": num,
                        "desc": res.get('descricaoItem', 'Item Resultado'),
                        "qtd": float(res.get('quantidadeHomologada') or 0),
                        "unitario_est": float(res.get('valorUnitarioHomologado') or 0),
                        "total_est": float(res.get('valorTotalHomologado') or 0),
                        "situacao": "HOMOLOGADO",
                        "tem_resultado": True,
                        "fornecedor": "", "unitario_hom": 0.0, "total_hom": 0.0
                    }
                
                itens_map[num]['tem_resultado'] = True
                itens_map[num]['situacao'] = "HOMOLOGADO"
                itens_map[num]['fornecedor'] = res.get('nomeRazaoSocialFornecedor', 'VENCEDOR ANÔNIMO')
                itens_map[num]['unitario_hom'] = float(res.get('valorUnitarioHomologado') or 0)
                itens_map[num]['total_hom'] = float(res.get('valorTotalHomologado') or 0)
    except: pass

    return sorted(list(itens_map.values()), key=lambda x: x['item'])

def processar_urls_manuais(session, banco):
    if not os.path.exists(ARQ_MANUAIS): return 0
    print("🔎 Processando URLs manuais...")
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
            if resp.status_code != 200: continue
            lic = resp.json()
            itens = capturar_detalhes(session, cnpj, ano, seq)
            banco[id_lic] = montar_objeto_licitacao(lic, itens, url)
            count += 1
            print(f"   + Manual Adicionado: {id_lic}")
        except: pass
    
    # Salva logo após processar manuais
    if count > 0: salvar_banco_disco(banco)
    return count

def montar_objeto_licitacao(lic, itens, link_manual=None):
    orgao = lic.get('orgaoEntidade', {})
    unidade = lic.get('unidadeOrgao', {})
    cnpj = orgao.get('cnpj')
    ano = lic.get('anoCompra')
    seq = lic.get('sequencialCompra')
    
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

    modo = os.getenv('MODE', 'DAILY')
    hoje = datetime.now()
    
    if modo == 'FULL':
        dt_inicio = datetime(2026, 1, 1)
        dt_fim = hoje
        print("📆 MODO FULL: Varrendo histórico completo dia a dia...")
    else:
        ontem = hoje - timedelta(days=1)
        dt_inicio = ontem
        dt_fim = ontem
        print(f"📆 MODO DAILY: Varrendo {ontem.strftime('%d/%m/%Y')}.")

    delta = dt_fim - dt_inicio
    dias_para_processar = [dt_inicio + timedelta(days=i) for i in range(delta.days + 1)]
    
    processar_urls_manuais(session, banco)

    # 2. Loop por DIA (Inicia tarefa, processa, salva e finaliza)
    for data_atual in dias_para_processar:
        str_data = data_atual.strftime('%Y%m%d')
        print(f"   > Iniciando varredura do dia: {data_atual.strftime('%d/%m/%Y')}")
        
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
                        cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
                        ano = lic.get('anoCompra')
                        seq = lic.get('sequencialCompra')
                        id_lic = f"{cnpj}{ano}{seq}"

                        # Em FULL mode, atualizamos sempre para pegar resultados novos
                        if modo == 'FULL' or id_lic not in banco:
                            itens = capturar_detalhes(session, cnpj, ano, seq)
                            if itens:
                                banco[id_lic] = montar_objeto_licitacao(lic, itens)
                                novos_no_dia += 1
                                if modo == 'FULL': time.sleep(0.05)
                pagina += 1
            except: break
        
        # === PONTO CRUCIAL: SALVA AO FIM DE CADA DIA ===
        if novos_no_dia > 0 or modo == 'FULL':
            salvar_banco_disco(banco)
            print(f"   💾 [Check-point] Dados do dia {data_atual.strftime('%d/%m')} salvos. (+{novos_no_dia} registros)")
        else:
            print(f"   - Dia {data_atual.strftime('%d/%m')} sem novos registros relevantes.")

    print(f"✅ Processamento Total Concluído.")
    # Salva uma última vez por garantia
    salvar_banco_disco(banco)

if __name__ == "__main__":
    run()
