import requests
import json
import os
import time
import urllib3
import unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
DATA_INICIO_VARREDURA = datetime(2026, 1, 1) 
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_CHECKPOINT = 'checkpoint.txt'

KEYWORDS_SAUDE = ["MEDICAMENTO", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGICO", "HIGIENE", "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO", "SAUDE", "INSUMO"]
BLACKLIST = ["ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO"]
UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t): 
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(t):
    txt = normalize(t)
    return any(k in txt for k in KEYWORDS_SAUDE) and not any(b in txt for b in BLACKLIST)

def criar_sessao():
    session = requests.Session()
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({'User-Agent': 'MonitorSaude/6.3', 'Accept': 'application/json'})
    return session

def main():
    session = criar_sessao()
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                banco_lista = json.loads(f.read().replace('const dadosLicitacoes = ', '').rstrip(';'))
                banco = {str(item['id']): item for item in banco_lista}
        except: banco = {}
    else: banco = {}
    
    cp = open(ARQ_CHECKPOINT).read().strip() if os.path.exists(ARQ_CHECKPOINT) else "20260101"
    data_atual = datetime.strptime(cp, '%Y%m%d')
    hoje = datetime.now()

    if data_atual.date() > hoje.date(): return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP | Analisando: {data_atual.strftime('%d/%m/%Y')}")

    pagina = 1
    while True:
        url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {"dataInicial": ds, "dataFinal": ds, "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50}
        
        try:
            resp = session.get(url_pub, params=params, timeout=25)
            if resp.status_code != 200: break
            dados = resp.json()
            licitacoes = dados.get('data', [])
            if not licitacoes: break

            for lic in licitacoes:
                unid = lic.get('unidadeOrgao', {})
                uf = unid.get('ufSigla') or lic.get('unidadeFederativaId')
                objeto = lic.get('objetoCompra') or ""

                if uf in UFS_ALVO and eh_relevante(objeto):
                    cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    id_lic = f"{cnpj}{ano}{seq}"
                    
                    # Manual PNCP: dataAberturaProposta é o fim do recebimento/início da sessão
                    data_abertura = lic.get('dataAberturaProposta') or lic.get('dataEncerramentoProposta')

                    if id_lic not in banco:
                        banco[id_lic] = {
                            "id": id_lic,
                            "uf": uf,
                            "cidade": unid.get('municipioNome') or "",
                            "orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "uasg": unid.get('codigoUnidade') or "---",
                            "objeto": objeto,
                            "numero": f"{lic.get('numeroCompra')}/{ano}",
                            "quantidade_itens": lic.get('quantidadeItens', 0),
                            "data_pub": lic.get('dataPublicacaoPncp'),
                            "data_abertura": data_abertura, 
                            "valor_total": lic.get('valorTotalEstimado', 0),
                            "api_params": {"cnpj": cnpj, "ano": ano, "seq": seq},
                            "link_pncp": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
                        }
            if pagina >= dados.get('totalPaginas', 1): break
            pagina += 1
        except: break

    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('data_pub', ''), reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista_final, indent=4, ensure_ascii=False)};")

    proximo_dia = (data_atual + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_dia)
    with open('env.txt', 'w') as f: f.write(f"CONTINUAR_EXECUCAO={'true' if (data_atual + timedelta(days=1)).date() <= hoje.date() else 'false'}")

if __name__ == "__main__":
    main()
