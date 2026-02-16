import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

print("🧹 LIMPEZA V16 - VALIDAÇÃO POR CSV + NOVAS REGRAS")

# --- 1. DEFINIÇÃO GEOGRÁFICA ---
ESTADOS_NE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_OUTROS = [
    'ES', 'RJ', 'SP', 'MG',         # Sudeste
    'GO', 'MT', 'MS', 'DF',         # Centro-Oeste
    'AM', 'PA', 'TO'                # Norte Selecionado
]
ESTADOS_ALVO = ESTADOS_NE + ESTADOS_OUTROS

# --- 2. BLACKLIST (Exclusão Imediata pelo Objeto) ---
BLACKLIST = [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", 
    "AR CONDICIONADO", "OBRAS", "ENGENHARIA", "CONFECCAO", 
    "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "ANIMAIS", "RACAO",
    "MATERIAL DE CONSTRUCAO", "MATERIAL ELETRICO", 
    "MATERIAL ESPORTIVO", "LOCACAO DE EQUIPAMENTO", 
    "AQUISICAO DE EQUIPAMENTO", "EXAME LABORATORI", 
    "RECEITUARIO", "PRESTACAO DE SERVICO",
    "ADESAO", "GASES MEDICINAIS",
    # NOVOS TERMOS BLOQUEADOS:
    "CONSIGNACAO", "INTENCAO", "GENEROS ALIMENTICIOS", 
    "ALIMENTACAO ESCOLAR", "PNAE", "COFFEE BREAK", 
    "CAFE REGIONAL", "KIT LANCHE", "GELO", "AGUA MINERAL", 
    "SEGURANCA PUBLICA", "VIDEOMONITORAMENTO", "MERENDA"
]

# --- 3. WHITELIST GLOBAL (Termos de Saúde/Gestão) ---
WHITELIST_GLOBAL = [
    "REMEDIO", "FARMACO", 
    "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", 
    "ANSIOLITIC", "DIABETIC", "GLICEMIC", "MEDICAMENT CONTROLAD",
    # NOVOS TERMOS GLOBAIS:
    "ATENCAO BASICA", "RENAME", "REMUME", "MAC", 
    "VIGILANCIA EM SAUDE", "ASSISTENCIA FARMACEUTICA", "GESTAO DO SUS"
]

# --- 4. WHITELIST REGIONAL (Nordeste) ---
WHITELIST_NE = [
    "FRALDA", "ABSORVENTE", "SORO",
    "DIETA ENTERAL", "DIETA", "FORMULA", "PROTEIC", 
    "CALORIC", "GAZE", "ATADURA",
    # NOVOS TERMOS NE:
    "MATERIAL PENSO", "MMH"
]

# --- FUNÇÕES AUXILIARES ---
def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# Carrega o CSV de Produtos para Memória
catalogo_produtos = set()
if os.path.exists(ARQCSV):
    try:
        # Tenta ler com diferentes encodings para evitar erro
        encodings = ['utf-8', 'latin-1', 'cp1252']
        content = None
        for enc in encodings:
            try:
                with open(ARQCSV, 'r', encoding=enc) as f:
                    content = f.read()
                break
            except UnicodeDecodeError: continue
        
        if content:
            # Assume que pode ser separado por quebra de linha, ponto e virgula ou virgula
            lines = content.splitlines()
            for line in lines:
                # Normaliza e limpa
                termo = normalize(line.replace(';', ' ').replace(',', ' '))
                if len(termo) > 2: # Ignora termos muito curtos (ex: "DE", "A")
                    catalogo_produtos.add(termo)
            print(f"📦 Catálogo CSV carregado: {len(catalogo_produtos)} termos.")
        else:
            print("⚠️ Erro: Não foi possível ler o CSV com nenhum encoding padrão.")
    except Exception as e:
        print(f"⚠️ Erro ao ler CSV: {e}")
else:
    print(f"⚠️ AVISO: Arquivo '{ARQCSV}' não encontrado. A validação por itens será ignorada (perigoso!).")


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

    # 3. Filtro de Objeto (Bloqueio e Liberação Inicial)
    objeto_txt = preg.get('objeto', '')
    objeto_norm = normalize(objeto_txt)
    
    # 3.1: BLACKLIST (Mata imediatamente, exceto se for Dieta/Fórmula explícita)
    if any(t in objeto_norm for t in BLACKLIST_NORM):
        # Exceção de segurança: Se tiver DIETA ou FORMULA no objeto, ignora blacklist de alimento
        if not ("DIETA" in objeto_norm or "FORMULA" in objeto_norm):
            continue 

    # 4. REGRA DE OURO: VALIDAÇÃO CRUZADA COM CSV (Check-in dos Itens)
    # O pregão só entra se pelo menos 1 item bater com o CSV
    raw_itens = preg.get('itensraw', [])
    tem_item_compativel = False
    
    if len(catalogo_produtos) > 0:
        for item in raw_itens:
            desc_item = normalize(item.get('descricao', ''))
            # Verifica se algum termo do CSV está CONTIDO na descrição do item
            for termo_csv in catalogo_produtos:
                if termo_csv in desc_item:
                    tem_item_compativel = True
                    break # Achou um item, salva o pregão
            if tem_item_compativel: break
    else:
        # Se não tiver CSV (fallback), aceita se passou na whitelist do objeto
        if any(t in objeto_norm for t in WHITELIST_GLOBAL_NORM): tem_item_compativel = True
        elif uf in ESTADOS_NE and any(t in objeto_norm for t in WHITELIST_NE_NORM): tem_item_compativel = True
        elif "MATERIAL DE LIMPEZA" in objeto_norm: # Regra do Álcool
             for item in raw_itens:
                d = normalize(item.get('descricao', ''))
                if "ALCOOL" in d and "70" in d:
                    tem_item_compativel = True; break

    # Se varreu todos os itens e nenhum bateu com o CSV, TCHAU!
    if not tem_item_compativel:
        continue

    # 5. Processamento Final (Estrutura de Exibição)
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
    
    count_me_epp = 0
    total_validos = 0

    if raw_itens and isinstance(raw_itens, list):
        for item in raw_itens:
            n_item = item.get('numeroItem')
            total_validos += 1
            
            # Lógica Oficial tipoBeneficioId
            cod_beneficio = 4 
            if 'tipoBeneficioId' in item:
                cod_beneficio = item['tipoBeneficioId']
            elif isinstance(item.get('tipoBeneficio'), dict):
                cod_beneficio = item['tipoBeneficio'].get('value') or item['tipoBeneficio'].get('id', 4)
            
            try: cod_beneficio = int(cod_beneficio)
            except: cod_beneficio = 4

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
