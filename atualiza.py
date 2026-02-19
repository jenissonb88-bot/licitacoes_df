import requests
import json
import gzip
import os
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
ARQDADOS = 'pregacoes_pharma_limpos.json.gz' 
MAXWORKERS = 10  

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/14.0'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def precisa_atualizar(lic):
    # Foca em quem não tem vencedor registrado ainda e tem potencial de ter ganho
    return any(not it.get('fornecedor') and it.get('situacao') in ["EM ANDAMENTO", "HOMOLOGADO"] for it in lic.get('itens', []))

def atualizar_licitacao(lid, dados, session):
    try:
        cnpj, ano, seq = lid[:14], lid[14:18], lid[18:]
        url_base = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        itens_atualizados = []
        houve_mudanca = False
        
        for it in dados.get('itens', []):
            item_novo = it.copy()
            if not it.get('fornecedor') and it.get('situacao') in ["EM ANDAMENTO", "HOMOLOGADO"]:
                try:
                    num = it['n']
                    r = session.get(f"{url_base}/{num}/resultados", timeout=15)
                    if r.status_code == 200 and r.json():
                        rl = r.json()
                        res = rl[0] if isinstance(rl, list) and len(rl) > 0 else (rl if isinstance(rl, dict) else None)
                        if res and res.get('nomeRazaoSocialFornecedor'):
                            nf = res.get('nomeRazaoSocialFornecedor')
                            ni = res.get('niFornecedor')
                            item_novo['fornecedor'] = f"{nf} (CNPJ: {ni})" if ni else nf
                            item_novo['situacao'] = "HOMOLOGADO"
                            item_novo['valHomologado'] = float(res.get('valorUnitarioHomologado') or 0.0)
                            houve_mudanca = True
                except: pass
            
            itens_atualizados.append(item_novo)
            
        if houve_mudanca:
            d_novo = dados.copy()
            d_novo['itens'] = itens_atualizados
            return d_novo
    except: pass
    return None

# --- EXECUÇÃO ---
if not os.path.exists(ARQDADOS): exit()

print("🩺 Auditoria Profunda de Fornecedores Iniciada...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    banco_raw = json.load(f)

banco_dict = {item['id']: item for item in banco_raw}
session = criar_sessao()

alvos = [lid for lid, d in banco_dict.items() if precisa_atualizar(d)]

print(f"📊 Banco Limpo Total: {len(banco_dict)}")
print(f"🎯 Alvos com Fornecedores Pendentes: {len(alvos)}")

if alvos:
    atualizados = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
        futuros = {exe.submit(atualizar_licitacao, lid, banco_dict[lid], session): lid for lid in alvos}
        for f in concurrent.futures.as_completed(futuros):
            lid = futuros[f]
            try:
                res = f.result()
                if res:
                    banco_dict[lid] = res
                    atualizados += 1
            except: pass

    print(f"💾 Salvando... ✅ {atualizados} licitações atualizadas com Vencedores.")

    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco_dict.values()), f, ensure_ascii=False)
else:
    print("🏁 Nenhum item pendente de auditoria.")
