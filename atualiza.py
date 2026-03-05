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
ARQ_RELATORIO = 'relatorio_atualizacoes.csv'
ARQ_LOG = 'log_atualizacao.txt'
MAXWORKERS = 10

# Mapas de situação (sincronizados com app.py)
MAPA_SITUACAO_ITEM = {1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 4: "DESERTO", 5: "FRACASSADO"}
MAPA_SITUACAO_GLOBAL = {1: "DIVULGADA", 2: "REVOGADA", 3: "ANULADA", 4: "SUSPENSA"}

def criar_sessao():
    s = requests.Session()
    s.headers.update({'Accept': 'application/json', 'User-Agent': 'Sniper Pharma/22.1'})
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

def log_mensagem(msg):
    """Salva log em arquivo e imprime no console"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '
')

def extrair_dados_do_id(lid):
    """
    Extrai CNPJ, Ano e Sequencial do ID.
    ID formatado como: {cnpj14}{ano4}{sequencialN}
    """
    if len(lid) < 18:
        return None, None, None

    cnpj = lid[:14]
    ano = lid[14:18]
    seq = lid[18:]

    if not (cnpj.isdigit() and ano.isdigit() and seq.isdigit()):
        return None, None, None

    return cnpj, ano, seq

def precisa_atualizar(lic):
    """
    Verifica se há itens sem fornecedor homologado
    OU se a situação global/itens podem ter mudado
    """
    itens = lic.get('itens', [])

    # Verifica itens pendentes
    tem_itens_pendentes = any(
        not it.get('res_forn') and it.get('sit') in ["EM ANDAMENTO", "HOMOLOGADO"]
        for it in itens
    )

    # Verifica se situação global não é final (DIVULGADA ou SUSPENSA podem mudar)
    sit_global = lic.get('sit_global', 'DIVULGADA')
    sit_nao_final = sit_global in ["DIVULGADA", "SUSPENSA"]

    return tem_itens_pendentes or sit_nao_final

def buscar_dados_licitacao(cnpj, ano, seq, session):
    """Busca dados gerais da licitação na API"""
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}"
    try:
        r = session.get(url, timeout=20)
        if r.status_code == 200:
            return r.json()
        else:
            log_mensagem(f"   ⚠️ HTTP {r.status_code} ao buscar dados da licitação {cnpj}/{ano}/{seq}")
            return None
    except Exception as e:
        log_mensagem(f"   ❌ Erro ao buscar dados da licitação {cnpj}/{ano}/{seq}: {e}")
        return None

def buscar_itens_api(cnpj, ano, seq, session):
    """Busca todos os itens atualizados da licitação"""
    url_base = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens_api = []
    pagina = 1
    max_paginas = 50

    while pagina <= max_paginas:
        try:
            r = session.get(url_base, params={'pagina': pagina, 'tamanhoPagina': 100}, timeout=20)
            if r.status_code != 200:
                break

            dados = r.json()
            itens_pagina = dados.get('data', []) if isinstance(dados, dict) else (dados if isinstance(dados, list) else [])

            if not itens_pagina:
                break

            for it in itens_pagina:
                if isinstance(it, dict):
                    itens_api.append(it)

            if len(itens_pagina) < 100:
                break
            pagina += 1

        except Exception as e:
            log_mensagem(f"   ⚠️ Erro ao buscar itens página {pagina}: {e}")
            break

    return itens_api

def buscar_resultado_item(cnpj, ano, seq, num_item, session):
    """Busca resultado de um item específico"""
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            rl = r.json()
            if isinstance(rl, list) and len(rl) > 0:
                return rl[0]
            elif isinstance(rl, dict):
                return rl
        return None
    except:
        return None

def atualizar_licitacao_completa(lid, dados_antigos, session):
    """
    Atualiza licitação completa: dados gerais + todos os itens
    Retorna: (dados_atualizados, mudancas_detalhadas, houve_mudanca)
    """
    cnpj, ano, seq = extrair_dados_do_id(lid)
    if not cnpj:
        log_mensagem(f"   ❌ ID inválido: {lid}")
        return None, [], False

    mudancas_detalhadas = []
    houve_mudanca = False

    # 1. BUSCAR DADOS GERAIS ATUALIZADOS
    dados_api = buscar_dados_licitacao(cnpj, ano, seq, session)
    if not dados_api:
        log_mensagem(f"   ⚠️ Não foi possível buscar dados atualizados de {lid}")
        return None, [], False

    # Preparar novo objeto
    dados_novos = dados_antigos.copy()

    # Atualizar situação global
    sit_global_id = dados_api.get('situacaoCompraId', 1)
    nova_sit_global = MAPA_SITUACAO_GLOBAL.get(sit_global_id, "DIVULGADA")
    sit_global_antiga = dados_antigos.get('sit_global', 'DIVULGADA')

    if nova_sit_global != sit_global_antiga:
        dados_novos['sit_global'] = nova_sit_global
        houve_mudanca = True
        log_mensagem(f"   🔄 Situação global alterada: {sit_global_antiga} → {nova_sit_global} ({lid})")

        # Se foi revogada/anulada, registrar no relatório
        if nova_sit_global in ["REVOGADA", "ANULADA"]:
            mudancas_detalhadas.append({
                'tipo': 'SITUACAO_GLOBAL',
                'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M'),
                'id_processo': lid,
                'edital': dados_antigos.get('edit'),
                'orgao': dados_antigos.get('org'),
                'campo_alterado': 'sit_global',
                'valor_anterior': sit_global_antiga,
                'valor_novo': nova_sit_global,
                'item_num': None,
                'descricao': None,
                'valor_estimado': None,
                'valor_homologado': None,
                'fornecedor': None
            })

    # Atualizar valor total estimado (se mudou)
    val_tot_api = float(dados_api.get('valorTotalEstimado') or 0.0)
    val_tot_antigo = dados_antigos.get('val_tot', 0.0)
    if val_tot_api != val_tot_antigo and val_tot_api > 0:
        dados_novos['val_tot'] = val_tot_api
        houve_mudanca = True
        log_mensagem(f"   💰 Valor total atualizado: {val_tot_antigo} → {val_tot_api} ({lid})")

    # 2. BUSCAR ITENS ATUALIZADOS
    itens_api = buscar_itens_api(cnpj, ano, seq, session)
    if not itens_api:
        log_mensagem(f"   ⚠️ Não foi possível buscar itens de {lid}")
        return (dados_novos if houve_mudanca else None), mudancas_detalhadas, houve_mudanca

    # Criar mapa de itens antigos por número
    itens_antigos_map = {it['n']: it for it in dados_antigos.get('itens', []) if 'n' in it}

    # Processar cada item da API
    itens_atualizados = []

    for it_api in itens_api:
        num_item = it_api.get('numeroItem')
        if num_item is None:
            continue

        # Dados básicos do item
        sit_item_id = int(it_api.get('situacaoCompraItem') or 1)
        sit_item_nome = MAPA_SITUACAO_ITEM.get(sit_item_id, "EM ANDAMENTO")

        benef_id = it_api.get('tipoBeneficioId')
        benef_nome_api = str(it_api.get('tipoBeneficioNome', '')).upper()
        benef_final = benef_id if benef_id in [1, 2, 3] else (1 if "EXCLUSIVA" in benef_nome_api else (3 if "COTA" in benef_nome_api else 4))

        # Montar item base
        item_novo = {
            'n': num_item,
            'd': it_api.get('descricao', ''),
            'q': float(it_api.get('quantidade') or 0),
            'u': it_api.get('unidadeMedida', 'UN'),
            'v_est': float(it_api.get('valorUnitarioEstimado') or 0.0),
            'benef': benef_final,
            'sit': sit_item_nome,
            'res_forn': None,
            'res_val': 0.0
        }

        # Buscar resultado/fornecedor se aplicável
        if sit_item_nome in ["EM ANDAMENTO", "HOMOLOGADO"]:
            res = buscar_resultado_item(cnpj, ano, seq, num_item, session)
            if res:
                nf = res.get('nomeRazaoSocialFornecedor') or res.get('razaoSocial')
                ni = res.get('niFornecedor')
                val_homol = float(res.get('valorUnitarioHomologado') or 0.0)

                if nf:
                    forn_completo = f"{nf} (CNPJ: {ni})" if ni else nf
                    item_novo['res_forn'] = forn_completo
                    item_novo['sit'] = "HOMOLOGADO"
                    item_novo['res_val'] = val_homol

        # Comparar com item antigo para detectar mudanças
        item_antigo = itens_antigos_map.get(num_item, {})

        # Detectar mudanças significativas
        mudanca_sit = item_novo['sit'] != item_antigo.get('sit', 'EM ANDAMENTO')
        mudanca_forn = item_novo.get('res_forn') != item_antigo.get('res_forn')
        mudanca_val = abs(item_novo.get('res_val', 0) - item_antigo.get('res_val', 0)) > 0.01

        if mudanca_sit or mudanca_forn or mudanca_val:
            houve_mudanca = True

            # Registrar no relatório apenas se houve homologação nova
            if item_novo.get('res_forn') and not item_antigo.get('res_forn'):
                mudancas_detalhadas.append({
                    'tipo': 'ITEM_HOMOLOGADO',
                    'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M'),
                    'id_processo': lid,
                    'edital': dados_antigos.get('edit'),
                    'orgao': dados_antigos.get('org'),
                    'campo_alterado': 'res_forn/res_val',
                    'valor_anterior': f"{item_antigo.get('sit')} - {item_antigo.get('res_forn', 'N/A')}",
                    'valor_novo': f"{item_novo['sit']} - {item_novo['res_forn']}",
                    'item_num': num_item,
                    'descricao': item_novo['d'],
                    'valor_estimado': item_novo['v_est'],
                    'valor_homologado': item_novo['res_val'],
                    'fornecedor': item_novo['res_forn']
                })
                log_mensagem(f"   ✅ Item {num_item} homologado: {item_novo['res_forn'][:30]}... ({lid})")

        itens_atualizados.append(item_novo)

    # Verificar se algum item foi removido ou adicionado
    nums_api = {it['n'] for it in itens_atualizados if 'n' in it}
    nums_antigos = set(itens_antigos_map.keys())

    if nums_api != nums_antigos:
        houve_mudanca = True
        adicionados = nums_api - nums_antigos
        removidos = nums_antigos - nums_api
        if adicionados:
            log_mensagem(f"   ➕ Itens adicionados: {adicionados} ({lid})")
        if removidos:
            log_mensagem(f"   ➖ Itens removidos: {removidos} ({lid})")

    if houve_mudanca:
        dados_novos['itens'] = itens_atualizados
        return dados_novos, mudancas_detalhadas, True

    return None, [], False

if __name__ == '__main__':
    if not os.path.exists(ARQDADOS):
        log_mensagem(f"❌ Arquivo {ARQDADOS} não encontrado.")
        exit(1)

    log_mensagem("🔄 Iniciando atualização completa de licitações (Modo B: Itens + Situação Global)")

    # Limpar log anterior
    if os.path.exists(ARQ_LOG):
        os.remove(ARQ_LOG)

    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
        banco_raw = json.load(f)

    log_mensagem(f"📦 Banco carregado: {len(banco_raw)} licitações")

    banco_dict = {item['id']: item for item in banco_raw}
    session = criar_sessao()

    # Identificar licitações que precisam de atualização
    alvos = [lid for lid, d in banco_dict.items() if precisa_atualizar(d)]

    if not alvos:
        log_mensagem("ℹ️ Não há licitações pendentes de atualização.")
        exit(0)

    log_mensagem(f"🔍 {len(alvos)} licitações selecionadas para atualização")

    relatorio_final = []
    lic_atualizadas = 0
    itens_homologados = 0
    situacoes_alteradas = 0

    # Processamento paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAXWORKERS) as exe:
        futuros = {exe.submit(atualizar_licitacao_completa, lid, banco_dict[lid], session): lid for lid in alvos}

        for f in concurrent.futures.as_completed(futuros):
            lid = futuros[f]
            try:
                res_dados, res_mudancas, houve_mudanca = f.result()

                if houve_mudanca and res_dados:
                    banco_dict[res_dados['id']] = res_dados
                    lic_atualizadas += 1

                    # Contar tipos de mudança
                    for m in res_mudancas:
                        if m['tipo'] == 'ITEM_HOMOLOGADO':
                            itens_homologados += 1
                        elif m['tipo'] == 'SITUACAO_GLOBAL':
                            situacoes_alteradas += 1

                    relatorio_final.extend(res_mudancas)

            except Exception as e:
                log_mensagem(f"   ❌ Falha no processamento de {lid}: {e}")

    # Salvar banco atualizado
    with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
        json.dump(list(banco_dict.values()), f, ensure_ascii=False)

    log_mensagem(f"💾 Banco salvo: {len(banco_dict)} licitações")
    log_mensagem(f"📊 Resumo: {lic_atualizadas} licitações modificadas")
    log_mensagem(f"   - {itens_homologados} itens homologados")
    log_mensagem(f"   - {situacoes_alteradas} situações globais alteradas")

    # Gerar relatório CSV
    if relatorio_final:
        keys = relatorio_final[0].keys()
        with open(ARQ_RELATORIO, 'w', newline='', encoding='utf-8-sig') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys, delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(relatorio_final)
        log_mensagem(f"📁 Relatório CSV gerado: {ARQ_RELATORIO} ({len(relatorio_final)} registros)")
    else:
        log_mensagem("ℹ️ Nenhuma alteração significativa detectada.")

    log_mensagem("✅ Atualização concluída!")
