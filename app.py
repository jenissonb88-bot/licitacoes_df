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

# Filtros de Negócio
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
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({'User-Agent': 'MonitorSaude/11.0', 'Accept': 'application/json'})
    return session

def buscar_detalhes_hibrido(session, cnpj, ano, seq):
    """
    Busca Itens e Resultados separadamente e faz a fusão dos dados.
    Garante fidelidade: Estimado do Item vs Contratado do Resultado.
    """
    url_base = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
    
    lista_itens = []
    lista_resultados = []

    # 1. Busca Itens (Dados do Edital)
    try:
        res_i = session.get(f"{url_base}/itens", params={"pagina":1, "tamanhoPagina":300}, timeout=20)
        if res_i.status_code == 200: lista_itens = res_i.json()
    except: pass

    # 2. Busca Resultados (Dados da Homologação)
    try:
        res_r = session.get(f"{url_base}/resultados", params={"pagina":1, "tamanhoPagina":300}, timeout=20)
        if res_r.status_code == 200: lista_resultados = res_r.json()
    except: pass

    # Dicionário Mestre para Fusão
    mestre = {}

    # A. Processa Itens Originais
    for i in lista_itens:
        num = i['numeroItem']
        mestre[num] = {
            "item": num,
            "descricao": i.get('descricao', 'Sem descrição'),
            "qtd": i.get('quantidade', 0),
            "val_est_unit": i.get('valorUnitarioEstimado', 0),
            "val_est_total": i.get('valorTotalEstimado', 0),
            "situacao": i.get('situacaoItemNome', 'Aberto'), # Ex: Fracassado, Deserto, Anulado
            "tem_resultado": False,
            # Dados de Resultado (Vazios por enquanto)
            "fornecedor": "",
            "val_contr_unit": 0,
            "val_contr_total": 0,
            "data_resultado": None
        }

    # B. Processa Resultados e funde/cria
    for r in lista_resultados:
        num = r['numeroItem']
        
        # Se item não existia (caso de itens criados apenas na homologação), cria agora
        if num not in mestre:
            mestre[num] = {
                "item": num,
                "descricao": r.get('descricaoItem', 'Item de Resultado'),
                "qtd": r.get('quantidadeHomologada', 0),
                "val_est_unit": r.get('valorUnitarioHomologado', 0), # Assume igual se não tinha estimado
                "val_est_total": r.get('valorTotalHomologado', 0),
                "situacao": "Homologado",
                "tem_resultado": True,
                "fornecedor": "", "val_contr_unit": 0, "val_contr_total": 0, "data_resultado": None
            }
        
        # Atualiza com dados oficiais do resultado
        mestre[num]["tem_resultado"] = True
        mestre[num]["fornecedor"] = r.get('nomeRazaoSocialFornecedor', 'Fornecedor não informado')
        mestre[num]["val_contr_unit"] = r.get('valorUnitarioHomologado', 0)
        mestre[num]["val_contr_total"] = r.get('valorTotalHomologado', 0)
        mestre[num]["data_resultado"] = r.get('dataResultado')
        mestre[num]["situacao"] = "Adjudicado/Homologado"

    # Converte para lista
    return sorted(list(mestre.values()), key=lambda x: x['item'])

def main():
    session = criar_sessao()
    banco = {}
    
    # Carrega dados existentes
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw: banco = {i['id']: i for i in json.loads(raw)}
        except: pass
    
    cp = open(ARQ_CHECKPOINT).read().strip() if os.path.exists(ARQ_CHECKPOINT) else "20260101"
    data_atual = datetime.strptime(cp, '%Y%m%d')
    hoje = datetime.now()

    if data_atual.date() > hoje.date():
        print("📅 Dados atualizados.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP | Varredura: {data_atual.strftime('%d/%m/%Y')}")

    pagina = 1
    while True:
        url_pub = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {"dataInicial": ds, "dataFinal": ds, "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50}
        
        try:
            resp = session.get(url_pub, params=params, timeout=30)
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
                    
                    # === DADOS GERAIS SOLICITADOS ===
                    dt_abertura = lic.get('dataAberturaProposta')
                    dt_encerramento = lic.get('dataEncerramentoProposta')
                    val_total = lic.get('valorTotalEstimado', 0)
                    qtd_itens_total = lic.get('quantidadeItens', 0)
                    
                    # Tratamento "Sigiloso" (Se for 0 e tiver flag de sigilo, ou apenas 0)
                    is_sigiloso = lic.get('niValorTotalEstimado') or (val_total == 0)

                    # Verifica se precisa baixar detalhes (Novo ou Vazio)
                    precisa_baixar = False
                    if id_lic not in banco:
                        precisa_baixar = True
                    elif not banco[id_lic].get('itens'):
                        precisa_baixar = True
                        print(f"   ♻️ Atualizando detalhes: {id_lic}...")

                    if precisa_baixar:
                        itens_detalhados = buscar_detalhes_hibrido(session, cnpj, ano, seq)
                        
                        banco[id_lic] = {
                            "id": id_lic,
                            "orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "unidade_compradora": unid.get('nomeUnidade') or "Não informado",
                            "cidade": unid.get('municipioNome') or "",
                            "uf": uf,
                            "objeto": objeto,
                            "data_abertura_proposta": dt_abertura,
                            "data_encerramento_proposta": dt_encerramento,
                            "valor_total_estimado": val_total,
                            "is_sigiloso": is_sigiloso,
                            "qtd_total_itens": qtd_itens_total,
                            "link_pncp": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                            "api_url": f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}", # Útil para o frontend
                            "itens": itens_detalhados
                        }
                        time.sleep(0.1)

            if pagina >= dados.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e: 
            print(f"Erro Pag {pagina}: {e}")
            break

    # Salva Ordenado por Data de Encerramento (Mais urgente primeiro)
    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('data_encerramento_proposta') or '', reverse=True)
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista_final, indent=4, ensure_ascii=False)};")

    proximo = (data_atual + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
    with open('env.txt', 'w') as f: f.write(f"CONTINUAR_EXECUCAO={'true' if (data_atual + timedelta(days=1)).date() <= hoje.date() else 'false'}")

if __name__ == "__main__":
    main()
