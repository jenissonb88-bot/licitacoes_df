import requests, json, os, time, urllib3, concurrent.futures, unicodedata
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.js'
ARQ_MANUAIS = 'urls.txt'  # Arquivo para entrada manual

# Filtros
KEYWORDS_SAUDE = ["MEDICAMENTO", "FARMACO", "SORO", "VACINA", "HOSPITALAR", "CIRURGICO", "HIGIENE", "DESCARTAVEL", "SERINGA", "AGULHA", "LUVAS", "GAZE", "ALGODAO"]
BLACKLIST = ["ESCOLAR", "CONSTRUCAO", "AUTOMOTIVO", "OBRA", "VEICULO", "REFEICAO", "LANCHE", "ALIMENTICIO", "MOBILIARIO", "TI", "INFORMATICA", "PNEU", "ESTANTE", "CADEIRA", "RODOVIARIO", "PAVIMENTACAO", "SERVICO", "LOCACAO", "COMODATO", "EXAME"]
UFS_ALVO = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "ES", "MG", "RJ", "SP", "AM", "PA", "TO", "RO", "GO", "MT", "MS", "DF"]

def normalize(t): 
    return ''.join(c for c in unicodedata.normalize('NFD', str(t or "")).upper() if unicodedata.category(c) != 'Mn')

def eh_relevante(t):
    txt = normalize(t)
    return not any(b in txt for b in BLACKLIST) and any(k in txt for k in KEYWORDS_SAUDE)

def criar_sessao():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])))
    return s

def capturar_detalhes(session, cnpj, ano, seq):
    url_base = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
    itens_map = {}

    # 1. Busca Itens (Edital)
    try:
        r = session.get(f"{url_base}/itens", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for i in r.json():
                num = i['numeroItem']
                # CÁLCULO MANUAL DO TOTAL (Qtd * Unitário) se a API falhar
                qtd = i.get('quantidade', 0)
                unit = i.get('valorUnitarioEstimado', 0)
                total = i.get('valorTotalEstimado', 0)
                if total == 0 and qtd > 0 and unit > 0:
                    total = round(qtd * unit, 2)

                itens_map[num] = {
                    "item": num,
                    "desc": i.get('descricao', 'Sem descrição'),
                    "qtd": qtd,
                    "unitario": unit,
                    "total": total, # Agora calculado corretamente
                    "situacao": "ABERTO",
                    "tem_resultado": False,
                    "fornecedor": "EM ANDAMENTO",
                    "homologado_unit": 0,
                    "homologado_total": 0
                }
    except: pass

    # 2. Busca Resultados (Homologação)
    try:
        r = session.get(f"{url_base}/resultados", params={"pagina":1, "tamanhoPagina":500}, timeout=20)
        if r.status_code == 200:
            for res in r.json():
                num = res['numeroItem']
                if num not in itens_map:
                    # Item fantasma (só no resultado)
                    itens_map[num] = {
                        "item": num,
                        "desc": res.get('descricaoItem', 'Item Resultado'),
                        "qtd": res.get('quantidadeHomologada', 0),
                        "unitario": res.get('valorUnitarioHomologado', 0),
                        "total": res.get('valorTotalHomologado', 0),
                        "situacao": "HOMOLOGADO",
                        "tem_resultado": True,
                        "fornecedor": "", "homologado_unit": 0, "homologado_total": 0
                    }
                
                # Atualiza com dados do vencedor
                itens_map[num]['tem_resultado'] = True
                itens_map[num]['situacao'] = "HOMOLOGADO"
                itens_map[num]['fornecedor'] = res.get('nomeRazaoSocialFornecedor', 'VENCEDOR ANÔNIMO')
                itens_map[num]['homologado_unit'] = res.get('valorUnitarioHomologado', 0)
                itens_map[num]['homologado_total'] = res.get('valorTotalHomologado', 0)
    except: pass

    return sorted(list(itens_map.values()), key=lambda x: x['item'])

def processar_urls_manuais(session, banco):
    """ Lê urls.txt e força a busca dessas licitações """
    if not os.path.exists(ARQ_MANUAIS): return 0
    print("🔎 Processando URLs manuais...")
    
    with open(ARQ_MANUAIS, 'r') as f:
        urls = [line.strip() for line in f.readlines() if 'pncp.gov.br' in line]
    
    count = 0
    for url in urls:
        try:
            # Extrai IDs da URL
            parts = url.split('/editais/')[1].split('/')
            if len(parts) < 3: continue
            cnpj, ano, seq = parts[0], parts[1], parts[2]
            id_lic = f"{cnpj}{ano}{seq}"
            
            # Se já tem e tem itens, pula (exceto se quiser forçar atualização)
            if id_lic in banco and len(banco[id_lic]['itens']) > 0: continue

            # Busca Cabeçalho
            api_url = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
            resp = session.get(api_url, timeout=15)
            if resp.status_code != 200: continue
            lic = resp.json()

            # Processa igual ao fluxo normal
            itens = capturar_detalhes(session, cnpj, ano, seq)
            
            banco[id_lic] = montar_objeto_licitacao(lic, itens, url)
            count += 1
            print(f"   + Manual Adicionado: {id_lic}")
        except Exception as e:
            print(f"   Erro URL manual {url}: {e}")
    
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
        "valor_global": lic.get('valorTotalEstimado', 0),
        "is_sigiloso": lic.get('niValorTotalEstimado', False),
        "qtd_itens": len(itens),
        "link": link_manual or f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
        "itens": itens
    }

def run():
    session = criar_sessao()
    
    # 1. Carrega Banco Existente
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw: banco = {i['id']: i for i in json.loads(raw)}
        except: pass

    # 2. Define o Período de Busca (Lógica do Agendamento)
    modo = os.getenv('MODE', 'DAILY') # Default para DAILY se não especificado
    
    hoje = datetime.now()
    
    if modo == 'FULL':
        # Busca Quinzenal: de 01/01/2026 até Hoje
        dt_inicio = datetime(2026, 1, 1)
        dt_fim = hoje
        print("📆 MODO COMPLETO (FULL): Varrendo de 01/01/2026 até hoje para atualizar resultados.")
    else:
        # Busca Diária: Apenas o dia anterior (Ontem)
        # Isso garante que pegamos o dia fechado às 08:00 da manhã
        ontem = hoje - timedelta(days=1)
        dt_inicio = ontem
        dt_fim = ontem
        print(f"📆 MODO DIÁRIO: Varrendo apenas {ontem.strftime('%d/%m/%Y')}.")

    # 3. Executa a Varredura
    delta = dt_fim - dt_inicio
    dias_para_processar = [dt_inicio + timedelta(days=i) for i in range(delta.days + 1)]
    
    novos = 0
    
    # Processa URLs Manuais Primeiro
    novos += processar_urls_manuais(session, banco)

    for data_atual in dias_para_processar:
        str_data = data_atual.strftime('%Y%m%d')
        print(f"   > Analisando dia: {str_data}")
        
        pagina = 1
        while True:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": str_data, "dataFinal": str_data,
                "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50
            }
            
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

                        # Lógica de Atualização:
                        # Se MODO=FULL: Atualiza TUDO (para pegar resultados novos em licitações antigas)
                        # Se MODO=DAILY: Só adiciona se não existir
                        if modo == 'FULL' or id_lic not in banco:
                            itens = capturar_detalhes(session, cnpj, ano, seq)
                            if itens: # Só salva se tiver itens
                                banco[id_lic] = montar_objeto_licitacao(lic, itens)
                                novos += 1
                                # Sleep leve para não travar no FULL
                                if modo == 'FULL': time.sleep(0.1) 

                pagina += 1
            except: break

    print(f"✅ Processamento concluído. {novos} registros atualizados/novos.")

    # 4. Salva JS Final
    lista = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento') or '', reverse=True)
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista, indent=4, ensure_ascii=False)};")

if __name__ == "__main__":
    run()
