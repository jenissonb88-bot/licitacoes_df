import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

print("🧹 LIMPEZA V19 - AJUSTE TERMOS NE")

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
                    next(leitor, None) # Pula cabeçalho
                    for linha in leitor:
                        if not linha: continue
                        # Colunas: 0 (Descrição), 1 (Fármaco), 5 (Nomes Técnicos)
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
            except UnicodeDecodeError: continue
            except: continue
    except: pass

if not csv_ativo: print(f"⚠️ AVISO: CSV não carregado.")

# --- 2. CONFIGURAÇÃO DE ESTADOS ---
ESTADOS_NE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_OUTROS = ['ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']
ESTADOS_ALVO = ESTADOS_NE + ESTADOS_OUTROS

# --- 3. LISTAS DE FILTRO ---
BLACKLIST_RAW = [
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
]

WHITELIST_GLOBAL_RAW = [
    "REMEDIO", "FARMACO", 
    "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", 
    "ANSIOLITIC", "DIABETIC", "GLICEMIC", "MEDICAMENT CONTROLAD",
    "ATENCAO BASICA", "RENAME", "REMUME", "MAC", 
    "VIGILANCIA EM SAUDE", "ASSISTENCIA FARMACEUTICA", "GESTAO DO SUS"
]

WHITELIST_NE_RAW = [
    "FRALDA", "ABSORVENTE", "SORO",
    "DIETA ENTERAL", "DIETA", "FORMULA", "PROTEIC", 
    "CALORIC", "GAZE", "ATADURA",
    "MATERIAL PENSO", "MMH", "MATERIAL MEDICO-HOSPITALAR"
]

BLACKLIST_NORM = [normalize(x) for x in BLACKLIST_RAW]
WHITELIST_GLOBAL_NORM = [normalize(x) for x in WHITELIST_GLOBAL_RAW]
WHITELIST_NE_NORM = [normalize(x) for x in WHITELIST_NE_RAW]

# DATA DE CORTE: 01/01/2026
data_limite = datetime(2026, 1, 1, 0, 0, 0)

if not os.path.exists(ARQDADOS): print("❌ Base não encontrada."); exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: todos = json.load(f)

limpos = []
duplicatas = set()

for preg in todos:
    if preg['id'] in duplicatas: continue
    duplicatas.add(preg['id'])
    
    # Filtro Data
    try:
        if datetime.fromisoformat(preg.get('dataEnc','').replace('Z','+00:00')).replace(tzinfo=None) < data_limite: continue
    except: pass

    # Filtro Geo
    uf = preg.get('uf', '').upper()
    if uf not in ESTADOS_ALVO: continue

    # Filtro Objeto
    obj_norm = normalize(preg.get('objeto', ''))
    if any(t in obj_norm for t in BLACKLIST_NORM):
        if not ("DIETA" in obj_norm or "FORMULA" in obj_norm): continue

    # Validação (Item CSV ou Objeto Whitelist)
    aprovado = False
    raw_itens = preg.get('itensraw', [])

    if csv_ativo and len(catalogo_produtos) > 0:
        for item in raw_itens:
            if any(t in normalize(item.get('descricao', '')) for t in catalogo_produtos):
                aprovado = True; break
        # Fallback se não achar item
        if not aprovado:
            if any(t in obj_norm for t in WHITELIST_GLOBAL_NORM): aprovado = True
            elif uf in ESTADOS_NE and any(t in obj_norm for t in WHITELIST_NE_NORM): aprovado = True
    else:
        # Sem CSV
        if any(t in obj_norm for t in WHITELIST_GLOBAL_NORM): aprovado = True
        elif uf in ESTADOS_NE and any(t in obj_norm for t in WHITELIST_NE_NORM): aprovado = True
        elif "MATERIAL DE LIMPEZA" in obj_norm:
             for item in raw_itens:
                if "ALCOOL" in normalize(item.get('descricao', '')) and "70" in normalize(item.get('descricao', '')):
                    aprovado = True; break

    if not aprovado: continue

    # Montagem
    mapa_resultados = {r['numeroItem']: r for r in preg.get('resultadosraw', [])}
    lista_itens = []
    count_me = 0
    
    for item in raw_itens:
        bid = item.get('tipoBeneficioId') or (item.get('tipoBeneficio') or {}).get('value', 4)
        is_me = int(bid) in [1, 2, 3] if str(bid).isdigit() else False
        if is_me: count_me += 1
        
        res = mapa_resultados.get(item['numeroItem'])
        
        lista_itens.append({
            'n': item['numeroItem'],
            'desc': item.get('descricao', ''),
            'qtd': item.get('quantidade', 0),
            'un': item.get('unidadeMedida', ''),
            'valUnit': item.get('valorUnitarioEstimado', 0),
            'me_epp': is_me,
            'situacao': "HOMOLOGADO" if res else "EM_ANDAMENTO",
            'fornecedor': res.get('razaoSocial') if res else None,
            'valHomologado': res.get('valorUnitarioHomologado', 0) if res else 0
        })

    tipo = "AMPLO"
    if lista_itens:
        if count_me == len(lista_itens): tipo = "EXCLUSIVO"
        elif count_me > 0: tipo = "PARCIAL"

    limpos.append({
        'id': preg['id'], 'uf': uf, 'cidade': preg.get('cidade', ''),
        'orgao': preg.get('orgao', ''), 'unidade': preg.get('unidadeCompradora', ''),
        'uasg': preg.get('uasg', ''), 'edital': preg.get('editaln', ''),
        'valor_estimado': round(preg.get('valorGlobalApi', 0), 2),
        'data_enc': preg.get('dataEnc', ''), 'objeto': obj_norm[:300], 
        'link': preg.get('link', ''), 'tipo_licitacao': tipo, 'itens': lista_itens
    })

print(f"✅ APROVADOS: {len(limpos)}")
with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)
