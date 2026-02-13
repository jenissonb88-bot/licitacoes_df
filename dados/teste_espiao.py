import requests, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def criar_sessao():
    s = requests.Session()
    s.verify = False
    return s

def buscar_todos_itens(session, cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens = []
    pag = 1
    while True:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=15)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            itens.extend(lista)
            pag += 1
            if pag > 100: break 
        except Exception as e:
            print(f"Erro ao buscar itens: {e}")
            break
    return itens

def buscar_todos_resultados(session, cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/resultados"
    resultados = []
    pag = 1
    while True:
        try:
            r = session.get(url, params={"pagina": pag, "tamanhoPagina": 50}, timeout=15)
            if r.status_code != 200: break
            dados = r.json()
            lista = dados.get('data', []) if isinstance(dados, dict) else dados
            if not lista: break
            resultados.extend(lista)
            pag += 1
            if pag > 100: break
        except Exception as e:
            print(f"Erro ao buscar resultados: {e}")
            break
    return resultados

def capturar_detalhes_completos(itens_raw, resultados_raw):
    itens_map = {}
    mapa_beneficio = {1: "Sim", 2: "Sim", 3: "Sim", 4: "Não", 5: "Não"}

    # 1. Mapeia os Itens Base
    for i in itens_raw:
        try:
            num = int(i['numeroItem'])
            qtd = float(i.get('quantidade') or 0)
            unit_est = float(i.get('valorUnitarioEstimado') or 0)
            
            cod_beneficio = i.get('tipoBeneficio') or i.get('tipoBeneficioId') or 5
            try: cod_beneficio = int(cod_beneficio)
            except: cod_beneficio = 5
                
            me_epp = mapa_beneficio.get(cod_beneficio, "Não")
            
            sit_item = str(i.get('situacaoCompraItemNome', '')).upper()
            if any(x in sit_item for x in ['CANCELAD', 'FRACASSAD', 'DESERT', 'ANULAD']):
                fornecedor_padrao = sit_item
                situacao_padrao = sit_item
            else:
                fornecedor_padrao = "EM ANDAMENTO (Sem resultado na API)"
                situacao_padrao = "ABERTO"
                
            itens_map[num] = {
                "item": num, 
                "desc": i.get('descricao', 'Sem descrição')[:40] + "...", 
                "qtd": qtd,
                "unit_est": unit_est, 
                "situacao": situacao_padrao,
                "fornecedor": fornecedor_padrao,
                "unit_hom": 0.0, 
                "me_epp": me_epp
            }
        except: continue

    # 2. Cruza com os Resultados
    for res in resultados_raw:
        try:
            num = int(res['numeroItem'])
            fornecedor = res.get('nomeRazaoSocialFornecedor')
            
            if not fornecedor:
                ind_res = str(res.get('indicadorResultadoNome', '')).upper()
                if any(x in ind_res for x in ['CANCELAD', 'FRACASSAD', 'DESERT', 'ANULAD']):
                    fornecedor = ind_res
                else:
                    fornecedor = 'VENCEDOR ANÔNIMO'
                    
            unit_hom = float(res.get('valorUnitarioHomologado') or 0)

            if num in itens_map:
                itens_map[num].update({
                    "situacao": "HOMOLOGADO",
                    "fornecedor": fornecedor,
                    "unit_hom": unit_hom
                })
            else:
                # Caso o item só exista na aba de resultados
                itens_map[num] = {
                    "item": num, 
                    "desc": res.get('descricaoItem', 'Item Resultado')[:40] + "...", 
                    "qtd": float(res.get('quantidadeHomologada') or 0),
                    "unit_est": unit_hom, 
                    "situacao": "HOMOLOGADO", 
                    "fornecedor": fornecedor,
                    "unit_hom": unit_hom, 
                    "me_epp": "Não" 
                }
        except: continue

    return sorted(list(itens_map.values()), key=lambda x: x['item'])

def testar_extracao():
    print("="*60)
    print("🕵️ ROBÔ ESPIÃO PNCP - TESTE DE RESULTADOS")
    print("="*60)
    
    url = input("\nCole o link do edital do PNCP (ex: https://pncp.gov.br/app/editais/00000000000000/2024/1): \n> ").strip()
    
    try:
        parts = url.split('editais/')[1].split('/')
        cnpj, ano, seq = parts[0], parts[1], parts[2]
    except:
        print("❌ URL inválida. Certifique-se de colar o link completo do PNCP.")
        return

    session = criar_sessao()
    
    print(f"\n🔍 Buscando na API do governo...")
    print(f"CNPJ: {cnpj} | ANO: {ano} | SEQ: {seq}\n")
    
    itens_raw = buscar_todos_itens(session, cnpj, ano, seq)
    print(f"✅ Encontrados {len(itens_raw)} itens na aba principal.")
    
    resultados_raw = buscar_todos_resultados(session, cnpj, ano, seq)
    print(f"✅ Encontrados {len(resultados_raw)} resultados homologados.")
    
    detalhes = capturar_detalhes_completos(itens_raw, resultados_raw)
    
    print("\n" + "="*60)
    print("📋 RESUMO DO CRUZAMENTO DE DADOS (PRIMEIROS 15 ITENS):")
    print("="*60)
    
    for d in detalhes[:15]:
        print(f"Item {d['item']:03d} | Qtd: {d['qtd']} | ME/EPP: {d['me_epp']}")
        print(f"Desc : {d['desc']}")
        print(f"Est  : R$ {d['unit_est']:.2f}")
        
        if d['situacao'] == "HOMOLOGADO":
            print(f"Sit. : 🟢 HOMOLOGADO")
            print(f"Venc.: 🏆 {d['fornecedor']}")
            print(f"Hom. : R$ {d['unit_hom']:.2f}")
        elif d['situacao'] == "ABERTO":
            print(f"Sit. : 🟡 {d['fornecedor']}")
        else:
            print(f"Sit. : 🔴 {d['situacao']}")
        print("-" * 60)
        
    if len(detalhes) > 15:
        print(f"... e mais {len(detalhes) - 15} itens ocultos.")

if __name__ == "__main__":
    testar_extracao()
