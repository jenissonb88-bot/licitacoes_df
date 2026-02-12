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
    # Retry mais agressivo para garantir a captura dos itens
    retry = Retry(total=8, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({'User-Agent': 'MonitorSaude/8.1', 'Accept': 'application/json'})
    return session

def buscar_detalhes_item(session, cnpj, ano, seq):
    """
    Busca os itens e resultados com tratamento de erro reforçado.
    """
    url_base = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/{cnpj}/{ano}/{seq}"
    
    try:
        # Tenta buscar itens (Página 1, até 200 itens)
        url_itens = f"{url_base}/itens"
        res_itens = session.get(url_itens, params={"pagina":1, "tamanhoPagina":200}, timeout=20)
        
        # Se falhar, tenta novamente após 2 segundos
        if res_itens.status_code != 200:
            time.sleep(2)
            res_itens = session.get(url_itens, params={"pagina":1, "tamanhoPagina":200}, timeout=20)

        # Se ainda falhar, retorna vazio mas loga o erro
        if res_itens.status_code != 200:
            print(f"   ❌ Erro HTTP {res_itens.status_code} ao buscar itens de {cnpj}/{seq}")
            return []

        itens = res_itens.json()
        if not itens: return [] # Lista vazia retornada pela API

        # Busca Resultados (opcional, não bloqueante)
        try:
            res_resultados = session.get(f"{url_base}/resultados", params={"pagina":1, "tamanhoPagina":200}, timeout=10)
            resultados = res_resultados.json() if res_resultados.status_code == 200 else []
        except:
            resultados = []
        
        # Mapeia vencedores
        mapa_res = {r['numeroItem']: r for r in resultados}
        
        processados = []
        for item in itens:
            res = mapa_res.get(item['numeroItem'])
            
            dados_item = {
                "item": item['numeroItem'],
                "descricao": item['descricao'],
                "qtd": item['quantidade'],
                "val_est_unit": item['valorUnitarioEstimado'],
                "situacao": item.get('situacaoItemNome', 'Aberto')
            }

            if res:
                dados_item.update({
                    "tem_vencedor": True,
                    "fornecedor": res['nomeRazaoSocialFornecedor'],
                    "val_final_unit": res['valorUnitarioHomologado'],
                    "val_final_total": res['valorTotalHomologado']
                })
            else:
                dados_item.update({
                    "tem_vencedor": False,
                    "fornecedor": "Sem Vencedor",
                    "val_final_unit": 0,
                    "val_final_total": 0
                })
            processados.append(dados_item)
            
        return processados
    except Exception as e:
        print(f"   ⚠️ Exceção ao buscar itens: {e}")
        return []

def main():
    session = criar_sessao()
    banco = {}
    
    # Carrega banco e converte para dicionário
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                raw = f.read().replace('const dadosLicitacoes = ', '').rstrip(';')
                if raw:
                    lista = json.loads(raw)
                    banco = {item['id']: item for item in lista}
        except: pass
    
    cp = open(ARQ_CHECKPOINT).read().strip() if os.path.exists(ARQ_CHECKPOINT) else "20260101"
    data_atual = datetime.strptime(cp, '%Y%m%d')
    hoje = datetime.now()

    if data_atual.date() > hoje.date():
        print("📅 Base atualizada.")
        with open('env.txt', 'w') as f: f.write("CONTINUAR_EXECUCAO=false")
        return

    ds = data_atual.strftime('%Y%m%d')
    print(f"🚀 Sniper PNCP | Analisando: {data_atual.strftime('%d/%m/%Y')}")

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
                    
                    data_fim = lic.get('dataEncerramentoProposta') or lic.get('dataAberturaProposta')
                    data_pub = lic.get('dataPublicacaoPncp')

                    # LÓGICA DE RECUPERAÇÃO DE ITENS PERDIDOS
                    # Se não existe no banco OU se existe mas a lista de itens está vazia
                    precisa_baixar = False
                    if id_lic not in banco:
                        precisa_baixar = True
                    elif not banco[id_lic].get('itens_processados'): # Verifica se a lista está vazia
                        precisa_baixar = True
                        print(f"   ♻️ Recuperando itens perdidos: {id_lic}...")
                    
                    if precisa_baixar:
                        itens_detalhados = buscar_detalhes_item(session, cnpj, ano, seq)
                        
                        if itens_detalhados:
                            print(f"   ✅ Itens capturados: {len(itens_detalhados)} para {id_lic}")
                        else:
                            print(f"   ⚠️ Atenção: 0 itens encontrados para {id_lic}")

                        banco[id_lic] = {
                            "id": id_lic,
                            "uf": uf,
                            "cidade": unid.get('municipioNome') or "",
                            "orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "uasg": unid.get('codigoUnidade') or "---",
                            "objeto": objeto,
                            "numero": f"{lic.get('numeroCompra')}/{ano}",
                            "quantidade_itens": len(itens_detalhados), # Atualiza com o real
                            "data_pub": data_pub,
                            "data_fim_prop": data_fim,
                            "valor_total": lic.get('valorTotalEstimado', 0),
                            "link_pncp": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                            "itens_processados": itens_detalhados
                        }
                        time.sleep(0.2) # Pausa pequena para não levar block

            if pagina >= dados.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e: 
            print(f"Erro na página {pagina}: {e}")
            break

    # Salva
    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('data_fim_prop') or '', reverse=True)
    
    os.makedirs('dados', exist_ok=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        f.write(f"const dadosLicitacoes = {json.dumps(lista_final, indent=4, ensure_ascii=False)};")

    proximo = (data_atual + timedelta(days=1)).strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo)
    
    continuar = "true" if datetime.strptime(proximo, '%Y%m%d').date() <= hoje.date() else "false"
    with open('env.txt', 'w') as f: f.write(f"CONTINUAR_EXECUCAO={continuar}")

if __name__ == "__main__":
    main()
