import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

print("🧹 LIMPEZA V21 - ME/EPP CORRIGIDO + DADOS DE RESULTADO")

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
                        if len(linha) > 1: termos.append(linha[1]) # Fármaco
                        if len(linha) > 5: termos.append(linha[5]) # Nome Tec
                        if len(linha) > 0: termos.append(linha[0]) # Descrição

                        for t in termos:
                            t_norm = normalize(t.strip())
                            if len(t_norm) > 3 and t_norm not in ["COMPRIMIDO", "FRASCO", "AMPOLA", "CAIXA", "UNIDADE"]:
                                catalogo_produtos.add(t_norm)
                csv_ativo = True
                print(f"📦 Catálogo CSV carregado: {len(catalogo_produtos)} termos.")
                break
            except UnicodeDecodeError: continue
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
    
    # Filtros Básicos
    try:
        if datetime.fromisoformat(preg.get('dataEnc','').replace('Z','+00:00')).replace(tzinfo=None) < data_limite: continue
    except: pass

    uf = preg.get('uf', '').upper()
    if uf not in ESTADOS_ALVO: continue

    obj_norm = normalize(preg.get('objeto', ''))
    
    # Blacklist (com exceção de Dietas)
    if any(t in obj_norm for t in BLACKLIST_NORM):
        if not ("DIETA" in obj_norm or "FORMULA" in obj_norm): continue

    # --- VALIDAÇÃO ---
    aprovado = False
    
    obj_is_global = any(t in obj_norm for t in WHITELIST_GLOBAL_NORM)
    obj_is_ne = any(t in obj_norm for t in WHITELIST_NE_NORM)
    
    item_match_csv = False
    raw_itens = preg.get('itensraw', [])
    if csv_ativo and len(catalogo_produtos) > 0:
        for item in raw_itens:
            if any(t in normalize(item.get('descricao', '')) for t in catalogo_produtos):
                item_match_csv = True; break

    if uf in ESTADOS_NE:
        if obj_is_global or obj_is_ne or item_match_csv: aprovado = True
    else:
        # Fora do NE, exige objeto Global (mais restrito)
        if obj_is_global: aprovado = True
    
    if not aprovado and "MATERIAL DE LIMPEZA" in obj_norm:
         for item in raw_itens:
            if "ALCOOL" in normalize(item.get('descricao', '')) and "70" in normalize(item.get('descricao', '')):
                aprovado = True; break

    if not aprovado: continue

    # --- PROCESSAMENTO DOS ITENS E RESULTADOS ---
    # Cria mapa de resultados indexado pelo numeroItem
    mapa_resultados = {r['numeroItem']: r for r in preg.get('resultadosraw', [])}
    
    lista_itens = []
    count_me = 0
    
    for item in raw_itens:
        # Lógica ME/EPP Reforçada
        bid = 4 # Default Amplo
        
        # Tenta pegar do ID direto
        if item.get('tipoBeneficioId') is not None:
            bid = item.get('tipoBeneficioId')
        # Tenta pegar do objeto
        elif isinstance(item.get('tipoBeneficio'), dict):
            bid = item['tipoBeneficio'].get('value')
        
        # Converte para int com segurança
        try:
            bid_int = int(bid)
            # Códigos 1, 2 e 3 são ME/EPP
            is_me = bid_int in [1, 2, 3]
        except:
            is_me = False 
            
        if is_me: count_me += 1
        
        # Busca resultado se houver
        res = mapa_resultados.get(item['numeroItem'])
        
        # Define Situação
        situacao_txt = "EM_ANDAMENTO"
        sit_compra = str(item.get('situacaoCompraItemName', '')).upper()
        
        if res:
            situacao_txt = "HOMOLOGADO"
        elif "CANCELADO" in sit_compra or "ANULADO" in sit_compra:
            situacao_txt = "CANCELADO"
        elif "FRACASSADO" in sit_compra:
            situacao_txt = "FRACASSADO"
        elif "DESERTO" in sit_compra:
            situacao_txt = "DESERTO"

        lista_itens.append({
            'n': item['numeroItem'],
            'desc': item.get('descricao', ''),
            'qtd': float(item.get('quantidade', 0)),
            'un': item.get('unidadeMedida', ''),
            'valUnit': float(item.get('valorUnitarioEstimado', 0)),
            'me_epp': is_me,
            'situacao': situacao_txt,
            'fornecedor': res.get('razaoSocial') if res else None,
            'valHomologado': float(res.get('valorUnitarioHomologado', 0)) if res else 0
        })

    tipo = "AMPLO"
    if lista_itens:
        if count_me == len(lista_itens): tipo = "EXCLUSIVO"
        elif count_me > 0: tipo = "PARCIAL"

    limpos.append({
        'id': preg['id'], 
        'uf': uf, 
        'cidade': preg.get('cidade', ''),
        'orgao': preg.get('orgao', ''), 
        'unidade': preg.get('unidadeCompradora', ''),
        'uasg': preg.get('uasg', ''), 
        'edital': preg.get('editaln', ''),
        'valor_estimado': round(preg.get('valorGlobalApi', 0), 2),
        'data_enc': preg.get('dataEnc', ''), 
        'objeto': obj_norm[:300], 
        'link': preg.get('link', ''), 
        'tipo_licitacao': tipo, 
        'itens': lista_itens
    })

print(f"✅ APROVADOS: {len(limpos)}")
with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)
