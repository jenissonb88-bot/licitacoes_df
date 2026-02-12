import requests, json, os, time, urllib3, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_FINISH = 'finish.txt'

# === LISTA DE RADICAIS (BUSCA PARCIAL) ===
# O robô buscará se estas partes de palavras existem nos textos
KEYWORDS = [
    "MEDICAMENT", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGIC", "HIGIENE", 
    "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO", "SAUDE", "INSUMO",
    "AMOXICILIN", "AMPICILIN", "CEFALEXIN", "CEFTRIAXON", "DIPIRON", "PARACETAMOL",
    "INSULIN", "GLICOSE", "HIDROCORTISON", "FUROSEMID", "OMEPRAZOL", "LOSARTAN",
    "ATENOLOL", "SULFATO", "CLORETO", "EQUIPO", "CATETER", "SONDA", "AVENTAL", 
    "MASCARA", "N95", "ALCOOL", "CURATIVO", "ESPARADRAPO", "PROPE", "TOUCA"
]

BLACKLIST = ["OBRA", "VEICULO", "INFORMATICA", "LIMPEZA PREDIAL", "CONSTRUCAO", "ESCOLAR", "PNEU", "REFEICAO"]

UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(texto_objeto, itens_raw=[]):
    """
    Lógica de Relevância:
    1. Se tiver blacklist no objeto, descarta.
    2. Se tiver keyword no objeto, aceita.
    3. Se não, varre cada item. Se encontrar keyword em qualquer item, aceita.
    """
    obj = normalize(texto_objeto)
    if any(b in obj for b in BLACKLIST): return False
    if any(k in obj for k in KEYWORDS): return True
    
    for it in itens_raw:
        desc_item = normalize(it.get('descricao', ''))
        if any(k in desc_item for k in KEYWORDS):
            return True
    return False

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def capturar_detalhes_completos(session, cnpj, ano, seq):
    url_base = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
    itens_map = {}

    # Busca Itens (Edital)
    try:
        r = session.get(f"{url_base}/itens", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for i in r.json():
                num = int(i['numeroItem'])
                qtd = float(i.get('quantidade') or 0)
                unit_est = float(i.get('valorUnitarioEstimado') or 0)
                total_est = float(i.get('valorTotalEstimado') or 0)
                if total_est == 0: total_est = round(qtd * unit_est, 2)

                itens_map[num] = {
                    "item": num, "desc": i.get('descricao', 'Sem descrição'), "qtd": qtd,
                    "unitario_est": unit_est, "total_est": total_est, "situacao": "ABERTO",
                    "tem_resultado": False, "fornecedor": "EM ANDAMENTO",
                    "unitario_hom": 0.0, "total_hom": 0.0
                }
    except: pass

    # Busca Resultados
    try:
        r = session.get(f"{url_base}/resultados", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for res in r.json():
                num = int(res['numeroItem'])
                if num in itens_map:
                    itens_map[num].update({
                        "tem_resultado": True, "situacao": "HOMOLOGADO",
                        "fornecedor": res.get('nomeRazaoSocialFornecedor', 'VENCEDOR ANÔNIMO'),
                        "unitario_hom": float(res.get('valorUnitarioHomologado') or 0),
                        "total_hom": float(res.get('valorTotalHomologado') or 0)
                    })
    except: pass
    return sorted(list(itens_map.values()), key=lambda x: x['item'])

def run():
    session = criar_sessao()
    banco = {}
    
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw: banco = {i['id']: i for i in json.loads(raw)}
        except: pass

    if not os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'w') as f: f.write("20260101")
    
    with open(ARQ_CHECKPOINT, 'r') as f: data_str = f.read().strip()
    data_alvo = datetime.strptime(data_str, '%Y%m%d')
    hoje = datetime.now()

    if data_alvo.date() > hoje.date():
        print("✅ Tudo atualizado.")
        with open(ARQ_FINISH, 'w') as f: f.write('done')
        return

    str_data = data_alvo.strftime('%Y%m%d')
    print(f"🚀 Iniciando Coleta: {data_alvo.strftime('%d/%m/%Y')}")

    url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {"dataInicial": str_data, "dataFinal": str_data, "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50}
    
    try:
        r = session.get(url_pub, params=params, timeout=25)
        if r.status_code == 200:
            lics = r.json().get('data', [])
            for lic in lics:
                unid = lic.get('unidadeOrgao', {})
                uf = unid.get('ufSigla') or lic.get('unidadeFederativaId')
                
                if uf in UFS_ALVO:
                    cnpj = lic['orgaoEntidade']['cnpj']
                    ano = lic['anoCompra']
                    seq = lic['sequencialCompra']
                    
                    # 1. Busca rápida de itens para ver relevância
                    url_itens = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}/itens"
                    ri = session.get(url_itens, params={"pagina":1, "tamanhoPagina":500})
                    itens_raw = ri.json() if ri.status_code == 200 else []

                    if eh_relevante(lic.get('objetoCompra'), itens_raw):
                        print(f"   [!] Encontrada: {lic.get('objetoCompra')[:50]}...")
                        detalhes = capturar_detalhes_completos(session, cnpj, ano, seq)
                        id_lic = f"{cnpj}{ano}{seq}"
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

    except Exception as e:
        print(f"❌ Erro no dia: {e}")

    # Salva Banco
    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista, indent=4, ensure_ascii=False)};")

    # Atualiza Checkpoint para o PRÓXIMO DIA
    proximo_dia = (data_alvo + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia)
    print(f"💾 Checkpoint movido para {proximo_dia}")

if __name__ == "__main__":
    run()
