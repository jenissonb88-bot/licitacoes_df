import requests
import json
import gzip
import os
import csv
import concurrent.futures
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz'
ARQ_RELATORIO = 'relatorio_atualizacoes.csv' # Nome do novo relatório
MAXWORKERS = 10  

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/22.1'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def precisa_atualizar(lic):
    return any(not it.get('fornecedor') and it.get('situacao') in ["EM ANDAMENTO", "HOMOLOGADO"] for it in lic.get('itens', []))

def atualizar_licitacao(lid, dados_antigos, session):
    try:
        cnpj = lid[:14]
        ano = lid[14:18]
        seq = lid[18:]
        url_base = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        
        itens_atualizados = []
        mudancas_detalhadas = [] # Lista para o relatório
        houve_mudanca = False
        
        for it in dados_antigos.get('itens', []):
            item_novo = it.copy()
            
            # Tenta atualizar apenas itens sem fornecedor que estão em andamento/homologados
            if not it.get('fornecedor') and it.get('situacao') in ["EM ANDAMENTO", "HOMOLOGADO"]:
                try:
                    num = it['n']
                    r = session.get(f"{url_base}/{num}/resultados", timeout=15)
                    if r.status_code == 200:
                        rl = r.json()
                        res = rl[0] if isinstance(rl, list) and len(rl) > 0 else (rl if isinstance(rl, dict) else None)
                        
                        if res:
                            nf = res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')
                            ni = res.get('niFornecedor')
                            val_homol = float(res.get('valorUnitarioHomologado') or 0.0)
                            
                            if nf:
                                forn_completo = f"{nf} (CNPJ: {ni})" if ni else nf
                                item_novo['fornecedor'] = forn_completo
                                item_novo['situacao'] = "HOMOLOGADO"
                                item_novo['valHomologado'] = val_homol
                                houve_mudanca = True
                                
                                # Registra para o relatório
                                mudancas_detalhadas.append({
                                    'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M'),
                                    'id_processo': lid,
                                    'edital': dados_antigos.get('edital'),
                                    'orgao': dados_antigos.get('orgao'),
                                    'item_num': num,
                                    'descricao': it.get('desc'),
                                    'valor_estimado': it.get('valUnit'),
                                    'valor_homologado': val_homol,
                                    'fornecedor': forn_completo
                                })
                except: pass
            
            itens_atualizados.append(item_novo)
            
        if houve_mudanca:
            dados_novos = dados_antigos.copy()
            dados_novos['itens'] = itens_atualizados
            return dados_novos, mudancas_detalhadas
        return None, []
    except Exception: return None, []

if __name__ == '__main__':
    if not os.path.exists(ARQDADOS): exit()

    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
        banco_raw = json.load(f)

    banco_dict = {item['id']: item for item in banco_raw}
    session = criar_sessao()
    alvos = [lid for lid, d in banco_dict.items() if precisa_atualizar(d)]

    relatorio_final = []

    if alvos:
        print(f"🔍 Analisando {len(alvos)} processos para possíveis atualizações...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
            futuros = {exe.submit(atualizar_licitacao, lid, banco_dict[lid], session): lid for lid in alvos}
            for f in concurrent.futures.as_completed(futuros):
                try:
                    res_dados, res_mudancas = f.result()
                    if res_dados:
                        banco_dict[res_dados['id']] = res_dados
                        relatorio_final.extend(res_mudancas)
                except: pass

        # Salva o Banco de Dados Atualizado
        with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
            json.dump(list(banco_dict.values()), f, ensure_ascii=False)

        # Gera o Relatório CSV se houver atualizações
        if relatorio_final:
            print(f"📊 Sucesso: {len(relatorio_final)} itens foram atualizados com fornecedores!")
            keys = relatorio_final[0].keys()
            with open(ARQ_RELATORIO, 'w', newline='', encoding='utf-8-sig') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys, delimiter=';')
                dict_writer.writeheader()
                dict_writer.writerows(relatorio_final)
            print(f"📁 Relatório gerado: {ARQ_RELATORIO}")
        else:
            print("ℹ️ Nenhum item novo foi homologado no PNCP desde a última verificação.")
