import json
import gzip
import os
import unicodedata
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'

print("🧹 LIMPEZA V15 - TIPO BENEFICIO ID OFICIAL")

# --- 1. DEFINIÇÃO GEOGRÁFICA ---
ESTADOS_NE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_OUTROS = [
    'ES', 'RJ', 'SP', 'MG',         # Sudeste
    'GO', 'MT', 'MS', 'DF',         # Centro-Oeste
    'AM', 'PA', 'TO'                # Norte Selecionado
]
ESTADOS_ALVO = ESTADOS_NE + ESTADOS_OUTROS

# --- 2. BLACKLIST ---
BLACKLIST = [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", 
    "AR CONDICIONADO", "OBRAS", "ENGENHARIA", "CONFECCAO", 
    "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "ANIMAIS", "RACAO",
    "GENERO ALIMENTICIO", 
    "MATERIAL DE CONSTRUCAO", "MATERIAL ELETRICO", 
    "MATERIAL ESPORTIVO", "LOCACAO DE EQUIPAMENTO", 
    "AQUISICAO DE EQUIPAMENTO", "EXAME LABORATORI", "MERENDA",
    "RECEITUARIO", "PRESTACAO DE SERVICO",
    "ADESAO", "GASES MEDICINAIS"
]

# --- 3. WHITELIST GLOBAL ---
WHITELIST_GLOBAL = [
    "REMEDIO", "FARMACO", 
    "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", 
    "ANSIOLITIC", "DIABETIC", "GLICEMIC", "MEDICAMENT CONTROLAD"
]

# --- 4. WHITELIST REGIONAL (Nordeste) ---
WHITELIST_NE = [
    "FRALDA", "ABSORVENTE", "SORO",
    "MATERIAL PENSO", "MATERIAL MEDICO-HOSPITALAR", 
    "DIETA ENTERAL", "DIETA", "FORMULA", "PROTEIC", 
    "CALORIC", "GAZE", "ATADURA"
]

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

BLACKLIST_NORM = [normalize(x) for x in BLACKLIST]
WHITELIST_GLOBAL_NORM = [normalize(x) for x in WHITELIST_GLOBAL]
WHITELIST_NE_NORM = [normalize(x) for x in WHITELIST_NE]

# DATA DE CORTE: 01/01/2026
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
    
    # 1. Filtro de Data
    data_enc = preg.get('dataEnc', '')
    try:
        if data_enc:
            data_enc_dt = datetime.fromisoformat(data_enc.replace('Z', '+00:00'))
            if data_enc_dt.replace(tzinfo=None) < data_limite:
                continue
    except: pass

    # 2. Filtro Geográfico
    uf = preg.get('uf', '').upper()
    if uf not in ESTADOS_ALVO: continue

    # 3. Filtro Objeto
    objeto_txt = preg.get('objeto', '')
    objeto_norm = normalize(objeto_txt)
    aceitar = False
    
    if any(t in objeto_norm for t in BLACKLIST_NORM): aceitar = False
    elif any(t in objeto_norm for t in WHITELIST_GLOBAL_NORM): aceitar = True
    elif uf in ESTADOS_NE and any(t in objeto_norm for t in WHITELIST_NE_NORM): aceitar = True
    elif "MATERIAL DE LIMPEZA" in objeto_norm:
        tem_alcool = False
        for item in preg.get('itensraw', []):
            d = normalize(item.get('descricao', ''))
            if "ALCOOL" in d and "70" in d:
                tem_alcool = True; break
        if tem_alcool: aceitar = True

    if not aceitar: continue

    # 4. Processamento
    mapa_resultados = {}
    raw_res = preg.get('resultadosraw', [])
    if raw_res and isinstance(raw_res, list):
        for res in raw_res:
            n = res.get('numeroItem')
            mapa_resultados[n] = {
                'fornecedor': res.get('razaoSocial', 'Forn. Desconhecido'),
                'valorHomologado': res.get('valorUnitarioHomologado', 0)
            }
            
    lista_itens = []
    raw_itens = preg.get('itensraw', [])
    
    count_me_epp = 0
    total_validos = 0

    if raw_itens and isinstance(raw_itens, list):
        for item in raw_itens:
            n_item = item.get('numeroItem')
            total_validos += 1
            
            # --- LÓGICA OFICIAL: tipoBeneficioId ---
            # 1=Exclusiva ME/EPP, 2=Subcontratação, 3=Cota Reservada -> SIM (Benefício)
            # 4=Sem Benefício, 5=Não se Aplica -> NÃO (Amplo)
            
            cod_beneficio = 4 # Default: Amplo
            
            # Tenta pegar direto (padrão mais comum)
            if 'tipoBeneficioId' in item:
                cod_beneficio = item['tipoBeneficioId']
            
            # Fallback: Tenta pegar dentro do objeto tipoBeneficio (se existir)
            elif isinstance(item.get('tipoBeneficio'), dict):
                cod_beneficio = item['tipoBeneficio'].get('value') or item['tipoBeneficio'].get('id', 4)
            
            # Converte para int com segurança
            try:
                cod_beneficio = int(cod_beneficio)
            except:
                cod_beneficio = 4

            # Aplica a regra do Manual
            is_me_epp = cod_beneficio in [1, 2, 3]
            
            if is_me_epp: count_me_epp += 1
            
            res = mapa_resultados.get(n_item)
            sit_final = "EM_ANDAMENTO"
            forn_final = None
            val_final = item.get('valorUnitarioEstimado', 0)
            
            if res:
                sit_final = "HOMOLOGADO"
                forn_final = res['fornecedor']
                val_final = res['valorHomologado']
            else:
                st = str(item.get('situacaoCompraItemName', '')).upper()
                if "CANCELADO" in st or "ANULADO" in st: sit_final = "CANCELADO"
                elif "FRACASSADO" in st or "DESERTO" in st: sit_final = "DESERTO"

            lista_itens.append({
                'n': n_item,
                'desc': item.get('descricao', ''),
                'qtd': item.get('quantidade', 0),
                'un': item.get('unidadeMedida', ''),
                'valUnit': item.get('valorUnitarioEstimado', 0),
                'me_epp': is_me_epp,
                'situacao': sit_final,
                'fornecedor': forn_final,
                'valHomologado': val_final if sit_final == 'HOMOLOGADO' else 0
            })

    tipo_lic = "AMPLO"
    if total_validos > 0:
        if count_me_epp == total_validos: tipo_lic = "EXCLUSIVO"
        elif count_me_epp > 0: tipo_lic = "PARCIAL"

    limpos.append({
        'id': id_preg,
        'uf': uf,
        'cidade': preg.get('cidade', ''),
        'orgao': preg.get('orgao', ''),
        'unidade': preg.get('unidadeCompradora', ''),
        'uasg': preg.get('uasg', ''),
        'edital': preg.get('editaln', ''),
        'valor_estimado': round(preg.get('valorGlobalApi', 0), 2),
        'data_pub': preg.get('dataPub', ''),
        'data_enc': data_enc,
        'objeto': objeto_txt[:300],
        'link': preg.get('link', ''),
        'tipo_licitacao': tipo_lic,
        'itens': lista_itens,
        'resultados_count': len(raw_res)
    })

print(f"📊 Processados: {len(limpos)}")

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)

print("🎉 LIMPEZA OK!")
