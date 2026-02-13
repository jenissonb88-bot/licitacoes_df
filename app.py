import requests, json, os, urllib3, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_MANUAIS = 'urls.txt'
ARQ_FINISH = 'finish.txt'

# === PALAVRAS-CHAVE (RADICAIS) ===
KEYWORDS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "HIGIENE", 
    "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO", "SAUDE", "INSUMO",
    "AMOXICILIN", "AMPICILIN", "CEFALEXIN", "CEFTRIAXON", "DIPIRON", "PARACETAMOL",
    "INSULIN", "GLICOSE", "HIDROCORTISON", "FUROSEMID", "OMEPRAZOL", "LOSARTAN",
    "ATENOLOL", "SULFATO", "CLORETO", "EQUIPO", "CATETER", "SONDA", "AVENTAL", 
    "MASCARA", "N95", "ALCOOL", "CURATIVO", "ESPARADRAPO", "PROPE", "TOUCA"
]

# === LISTA NEGRA RESTAURADA E AMPLIADA ===
BLACKLIST = [
    "ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", 
    "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", 
    "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", 
    "COMODATO", "EXAME", "LIMPEZA PREDIAL", "MANUTENCAO", "ASSISTENCIA MEDICA", 
    "PLANO DE SAUDE", "ODONTOLOGICA", "TERCEIRIZACAO"
]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(texto_objeto, itens_raw=[]):
    obj = normalize(texto_objeto)
    if any(b in obj for b in BLACKLIST): return False
    if any(k in obj for k in KEYWORDS): return True
    for it in itens_raw:
        desc_item = normalize(it.get('descricao', ''))
        if any(b in desc_item for b in BLACKLIST): continue # Pula item se for serviço
        if any(k in desc_item for k in KEYWORDS): return True
    return False

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

# Busca itens da forma correta (Paginação de 50)
def buscar_todos_itens(session, cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens = []
    pag = 1
    while True:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=15)
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            itens.extend(data)
            pag += 1
            if pag > 100: break # Limite de segurança
        except: break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    resultados = []
    pag = 1
    while True:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=15)
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            resultados.extend(data)
            pag += 1
        except: break
    return resultados

def capturar_detalhes_completos(session, cnpj, ano, seq, itens_raw):
    itens_map = {}
    
    # Processa os itens que já foram baixados
    for i in itens_raw:
        try:
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
        except: continue

    # Busca Resultados
    resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq)
    for res in resultados_raw:
        try:
            num = int(res['numeroItem'])
            if num in itens_map:
                itens_map[num].update({
                    "tem_resultado": True, "situacao": "HOMOLOGADO",
                    "fornecedor": res.get('nomeRazaoSocialFornecedor', 'VENCEDOR ANÔNIMO'),
                    "unitario_hom": float(res.get('valorUnitarioHomologado') or 0),
                    "total_hom": float(res.get('valorTotalHomologado') or 0)
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
                itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
                detalhes = capturar_detalhes_completos(session, cnpj, ano, seq, itens_raw)
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
        with open(ARQ_FINISH, 'w') as f: f.write('done')
        return

    str_data = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Coletando: {data_alvo.strftime('%d/%m/%Y')}")

    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": str_data, "dataFinal": str_data, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50}
    
    novos_no_dia = 0
    try:
        r = session.get(url_pub, params=params, timeout=25)
        if r.status_code == 200:
            lics = r.json().get('data', [])
            for lic in lics:
                unid = lic.get('unidadeOrgao', {})
                uf = unid.get('ufSigla') or lic.get('unidadeFederativaId')
                if uf in UFS_ALVO:
                    cnpj, ano, seq = lic['orgaoEntidade']['cnpj'], lic['anoCompra'], lic['sequencialCompra']
                    id_lic = f"{cnpj}{ano}{seq}"
                    
                    # Ignora se já existe no banco e não tem modo de forçar atualização
                    if id_lic in banco: continue

                    # Baixa os itens corretamente
                    itens_raw = buscar_todos_itens(session, cnpj, ano, seq)

                    if eh_relevante(lic.get('objetoCompra'), itens_raw):
                        detalhes = capturar_detalhes_completos(session, cnpj, ano, seq, itens_raw)
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
    except Exception as e:
        print(f"Erro na varredura: {e}")

    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista, indent=4, ensure_ascii=False)};")

    proximo_dia = (data_alvo + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia)
    print(f"💾 Checkpoint: {proximo_dia} | Novos: {novos_no_dia}")

if __name__ == "__main__":
    run()
