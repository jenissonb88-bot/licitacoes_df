import json
import gzip
import os
import unicodedata
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'

# --- CONFIGURAÇÕES ---
# Whitelist: Salva sempre
WHITELIST_OBJETO = ["FRALDA", "ABSORVENTE"]

# Blacklist: Descarta se tiver no objeto
BLACKLIST_OBJETO = [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", 
    "AR CONDICIONADO", "OBRAS", "ENGENHARIA", "CONFECCAO", 
    "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "VETERINARIO", "ANIMAIS", "RACAO",
    "ODONTOLOGICO", "ODONTO", "GENERO ALIMENTICIO", 
    "MATERIAL DE CONSTRUCAO", "MATERIAL ELETRICO", 
    "MATERIAL ESPORTIVO", "LOCACAO DE EQUIPAMENTO", 
    "AQUISICAO DE EQUIPAMENTO", "EXAME LABORATORI", "MERENDA"
]

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

WHITELIST_NORM = [normalize(x) for x in WHITELIST_OBJETO]
BLACKLIST_NORM = [normalize(x) for x in BLACKLIST_OBJETO]

print("🧹 LIMPEZA V6 - DADOS DETALHADOS (ME/EPP & FORNECEDORES)")

# Data de corte: 01/01/2026
data_limite = datetime(2026, 1, 1, 0, 0, 0)

if not os.path.exists(ARQDADOS):
    print(f"❌ Arquivo {ARQDADOS} não encontrado.")
    exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    todos = json.load(f)

limpos = []
duplicatas = set()

for preg in todos:
    id_preg = preg.get('id')
    if id_preg in duplicatas: continue
    duplicatas.add(id_preg)
    
    # 1. Filtro Data
    data_enc = preg.get('dataEnc', '')
    try:
        if data_enc:
            data_enc_dt = datetime.fromisoformat(data_enc.replace('Z', '+00:00'))
            if data_enc_dt.replace(tzinfo=None) < data_limite:
                continue
    except: pass

    # 2. Filtros de Objeto (Whitelist/Blacklist/Limpeza)
    objeto_txt = preg.get('objeto', '')
    objeto_norm = normalize(objeto_txt)
    manter = True 
    
    if any(t in objeto_norm for t in WHITELIST_NORM):
        manter = True
    elif "MATERIAL DE LIMPEZA" in objeto_norm:
        # Regra do alcool
        tem_alcool = False
        for item in preg.get('itensraw', []):
            d = normalize(item.get('descricao', ''))
            if "ALCOOL" in d and "70" in d:
                tem_alcool = True; break
        if not tem_alcool: manter = False
    elif any(t in objeto_norm for t in BLACKLIST_NORM):
        manter = False

    if not manter: continue

    # 3. PROCESSAMENTO AVANÇADO DE ITENS E RESULTADOS
    # Vamos criar um mapa de resultados para ligar ao item
    resultados_map = {}
    raw_res = preg.get('resultadosraw', [])
    if raw_res and isinstance(raw_res, list):
        for res in raw_res:
            num_item = res.get('numeroItem')
            resultados_map[num_item] = {
                'nomeFornecedor': res.get('razaoSocial', 'Fornecedor Desconhecido'),
                'valorHomologado': res.get('valorUnitarioHomologado', 0),
                'situacao': 'HOMOLOGADO' # Se está na lista de resultados, foi adjudicado/homologado
            }

    lista_itens = []
    raw_itens = preg.get('itensraw', [])
    
    tipo_licitacao = "AMPLO" # Default
    cont_me_epp = 0
    total_itens = 0

    if raw_itens and isinstance(raw_itens, list):
        total_itens = len(raw_itens)
        for item in raw_itens:
            n_item = item.get('numeroItem')
            
            # Checa ME/EPP (Campo 'temBeneficioMicroEpp' ou similar)
            is_me_epp = item.get('temBeneficioMicroEpp', False)
            if is_me_epp: cont_me_epp += 1

            # Checa se tem resultado
            dados_res = resultados_map.get(n_item)
            
            situacao_item = "EM_ANDAMENTO"
            fornecedor = None
            val_hom = 0
            
            status_raw = str(item.get('situacaoCompraItemName', '')).upper()
            
            if dados_res:
                situacao_item = "HOMOLOGADO"
                fornecedor = dados_res['nomeFornecedor']
                val_hom = dados_res['valorHomologado']
            elif "CANCELADO" in status_raw or "ANULADO" in status_raw:
                situacao_item = "CANCELADO"
            elif "FRACASSADO" in status_raw or "DESERTO" in status_raw:
                situacao_item = "DESERTO"
            
            lista_itens.append({
                'n': n_item,
                'desc': item.get('descricao', ''),
                'qtd': item.get('quantidade', 0),
                'un': item.get('unidadeMedida', ''),
                'valUnit': item.get('valorUnitarioEstimado', 0),
                'me_epp': is_me_epp,
                'situacao': situacao_item,
                'fornecedor': fornecedor,
                'valHomologado': val_hom
            })

    # Define etiqueta do processo (AMPLO, EXCLUSIVO, PARCIAL)
    if total_itens > 0:
        if cont_me_epp == total_itens:
            tipo_licitacao = "EXCLUSIVO"
        elif cont_me_epp > 0:
            tipo_licitacao = "PARCIAL"
        else:
            tipo_licitacao = "AMPLO"

    limpos.append({
        'id': id_preg,
        'uf': preg.get('uf', ''),
        'cidade': preg.get('cidade', ''),
        'orgao': preg.get('orgao', ''),
        'unidade': preg.get('unidadeCompradora', ''),
        'uasg': preg.get('uasg', ''),
        'edital': preg.get('editaln', ''),
        'valor_estimado': round(preg.get('valorGlobalApi', 0), 2),
        'data_pub': preg.get('dataPub', ''),
        'data_enc': data_enc,
        'objeto': objeto_txt[:250],
        'link': preg.get('link', ''),
        'tipo_licitacao': tipo_licitacao, # Campo Novo
        'itens': lista_itens,
        'resultados_count': len(raw_res)
    })

print(f"📊 Processados: {len(limpos)} pregões ativos.")

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)

print("🎉 LIMPEZA CONCLUÍDA!")
