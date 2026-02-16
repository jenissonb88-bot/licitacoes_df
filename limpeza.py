import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

print("🧹 LIMPEZA V24 - LEITOR SLIM -> SAÍDA WEB PADRÃO")

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CSV ---
catalogo_produtos = set()
csv_ativo = False

if os.path.exists(ARQCSV):
    try:
        encodings = ['utf-8', 'latin-1', 'cp1252']
        for enc in encodings:
            try:
                with open(ARQCSV, 'r', encoding=enc) as f:
                    leitor = csv.reader(f)
                    next(leitor, None) 
                    for linha in leitor:
                        if not linha: continue
                        termos = []
                        if len(linha) > 1: termos.append(linha[1]) 
                        if len(linha) > 5: termos.append(linha[5]) 
                        if len(linha) > 0: termos.append(linha[0]) 

                        for t in termos:
                            t_norm = normalize(t.strip())
                            if len(t_norm) > 3 and t_norm not in ["COMPRIMIDO", "FRASCO", "AMPOLA", "CAIXA", "UNIDADE"]:
                                catalogo_produtos.add(t_norm)
                csv_ativo = True
                print(f"📦 Catálogo CSV carregado: {len(catalogo_produtos)} termos.")
                break
            except: continue
    except: pass

# --- 2. CONFIGURAÇÕES ---
ESTADOS_NE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_OUTROS = ['ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']
ESTADOS_ALVO = ESTADOS_NE + ESTADOS_OUTROS

BLACKLIST_NORM = [normalize(x) for x in [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", 
    "AR CONDICIONADO", "OBRAS", "ENGENHARIA", "CONFECCAO", 
    "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "ANIMAIS", "RACAO",
    "MATERIAL DE CONSTRUCAO", "MATERIAL ELETRICO", 
    "MATERIAL ESPORTIVO", "LOCACAO DE EQUIPAMENTO", 
    "AQUISICAO DE EQUIPAMENTO", "EXAME LABORATORI", 
    "RECEITUARIO", "PRESTACAO DE SERVICO",
    "ADESAO", "GASES MEDICINAIS",
    "CONSIGNACAO", "INTENCAO", "GENEROS ALIMENTICIOS", 
    "ALIMENTACAO ESCOLAR", "PNAE", "COFFEE BREAK", 
    "CAFE REGIONAL", "KIT LANCHE", "GELO", "AGUA MINERAL", 
    "SEGURANCA PUBLICA", "VIDEOMONITORAMENTO", "MERENDA"
]]

WHITELIST_GLOBAL_NORM = [normalize(x) for x in [
    "REMEDIO", "FARMACO", "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", "ANSIOLITIC", "DIABETIC", 
    "GLICEMIC", "MEDICAMENT CONTROLAD", "ATENCAO BASICA", "RENAME", "REMUME", 
    "MAC", "VIGILANCIA EM SAUDE", "ASSISTENCIA FARMACEUTICA", "GESTAO DO SUS"
]]

WHITELIST_NE_NORM = [normalize(x) for x in [
    "FRALDA", "ABSORVENTE", "SORO", "DIETA ENTERAL", "DIETA", "FORMULA", 
    "PROTEIC", "CALORIC", "GAZE", "ATADURA", "MATERIAL PENSO", "MMH", 
    "MATERIAL MEDICO-HOSPITALAR"
]]

data_limite = datetime(2026, 1, 1, 0, 0, 0)

if not os.path.exists(ARQDADOS): print("❌ Base não encontrada."); exit()
with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: todos = json.load(f)

limpos = []
duplicatas = set()

for preg in todos:
    if preg['id'] in duplicatas: continue
    duplicatas.add(preg['id'])
    
    # Mapeamento Chaves Curtas (V3.0) -> Variáveis
    p_dt_enc = preg.get('dt_enc') or preg.get('dataEnc')
    p_uf = preg.get('uf') or ''
    p_obj = preg.get('obj') or preg.get('objeto') or ''
    p_itens = preg.get('itens') or [] # Já vem limpo na V3.0
    
    # Filtro Data
    try:
        if datetime.fromisoformat(p_dt_enc.replace('Z','+00:00')).replace(tzinfo=None) < data_limite: continue
    except: pass

    # Filtro Geo
    if p_uf.upper() not in ESTADOS_ALVO: continue

    obj_norm = normalize(p_obj)
    
    # Filtro Blacklist
    if any(t in obj_norm for t in BLACKLIST_NORM):
        if not ("DIETA" in obj_norm or "FORMULA" in obj_norm): continue

    # --- VALIDAÇÃO ---
    aprovado = False
    
    # 1. Verifica Objeto
    obj_is_global = any(t in obj_norm for t in WHITELIST_GLOBAL_NORM)
    obj_is_ne = any(t in obj_norm for t in WHITELIST_NE_NORM)
    
    # 2. Verifica Itens (CSV)
    item_match_csv = False
    if csv_ativo and len(catalogo_produtos) > 0:
        for item in p_itens:
            # item['d'] é 'descricao' na versão Slim
            desc = item.get('d') or item.get('descricao') or ''
            if any(t in normalize(desc) for t in catalogo_produtos):
                item_match_csv = True; break

    # 3. Decisão
    if p_uf.upper() in ESTADOS_NE:
        if obj_is_global or obj_is_ne or item_match_csv: aprovado = True
    else:
        if obj_is_global: aprovado = True
    
    if not aprovado and "MATERIAL DE LIMPEZA" in obj_norm:
         for item in p_itens:
            desc = item.get('d') or item.get('descricao') or ''
            if "ALCOOL" in normalize(desc) and "70" in normalize(desc):
                aprovado = True; break

    if not aprovado: continue

    # --- MONTAGEM PARA O HTML (Formato Legado) ---
    # Aqui transformamos o formato Slim de volta no formato que o HTML entende
    
    lista_itens_final = []
    count_me = 0
    
    for item in p_itens:
        # Recupera dados Slim
        bid = item.get('benef')
        is_me = False
        try: is_me = int(bid) in [1, 3]
        except: pass
        if is_me: count_me += 1
        
        # Recupera dados de resultado injetados
        sit = item.get('sit', 'ABERTO')
        
        # Reconstrói objeto para HTML
        lista_itens_final.append({
            'n': item.get('n'),
            'desc': item.get('d'),
            'qtd': float(item.get('q', 0)),
            'un': item.get('u'),
            'valUnit': float(item.get('v_est', 0)),
            'me_epp': is_me,
            'situacao': sit,
            'fornecedor': item.get('res_forn'), # Só existe se tiver resultado
            'valHomologado': float(item.get('res_val', 0))
        })

    tipo = "AMPLO"
    if lista_itens_final:
        if count_me == len(lista_itens_final): tipo = "EXCLUSIVO"
        elif count_me > 0: tipo = "PARCIAL"

    # Salva no formato que o HTML espera
    limpos.append({
        'id': preg['id'], 
        'uf': p_uf, 
        'cidade': preg.get('cid') or preg.get('cidade'),
        'orgao': preg.get('org') or preg.get('orgao'), 
        'unidade': preg.get('unid_nome') or preg.get('unidadeCompradora'),
        'uasg': preg.get('uasg'), 
        'edital': preg.get('edit') or preg.get('editaln'),
        'valor_estimado': round(float(preg.get('val_tot') or preg.get('valorGlobalApi') or 0), 2),
        'data_enc': p_dt_enc, 
        'objeto': p_obj[:300], 
        'link': preg.get('link'), 
        'tipo_licitacao': tipo, 
        'itens': lista_itens_final
    })

print(f"✅ FINAL WEB: {len(limpos)} pregões exportados.")
with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)
