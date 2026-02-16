import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

print("🧹 LIMPEZA V18 - LEITURA CSV OTIMIZADA + FILTROS COMPLETOS")

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CSV (INTELIGENTE) ---
catalogo_produtos = set()
csv_ativo = False

if os.path.exists(ARQCSV):
    try:
        # Tenta detectar encoding (utf-8 ou latin-1 são comuns em Excel/CSV br)
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for enc in encodings:
            try:
                with open(ARQCSV, 'r', encoding=enc) as f:
                    # Usa o módulo CSV para lidar com virgulas dentro de aspas corretamente
                    leitor = csv.reader(f)
                    
                    # Pula o cabeçalho
                    next(leitor, None)
                    
                    for linha in leitor:
                        if not linha: continue
                        
                        # Mapeamento baseado no seu arquivo:
                        # 0: Descrição | 1: Fármaco | 2: Dosagem | 3: Forma | 4: Apresentação | 5: Nomes Técnicos
                        
                        termos_para_adicionar = []
                        
                        # Adiciona Fármaco (Coluna 1 - índice 1)
                        if len(linha) > 1: termos_para_adicionar.append(linha[1])
                        
                        # Adiciona Nomes Técnicos (Coluna 5 - índice 5)
                        if len(linha) > 5: termos_para_adicionar.append(linha[5])
                        
                        # Adiciona Descrição (Coluna 0), mas com cautela
                        if len(linha) > 0: termos_para_adicionar.append(linha[0])

                        for t in termos_para_adicionar:
                            t_norm = normalize(t.strip())
                            # Filtro de ruído: ignora termos curtos ou muito genéricos que possam ter passado
                            if len(t_norm) > 3 and t_norm not in ["COMPRIMIDO", "FRASCO", "AMPOLA", "CAIXA"]:
                                catalogo_produtos.add(t_norm)
                                
                csv_ativo = True
                print(f"📦 Catálogo CSV carregado com sucesso: {len(catalogo_produtos)} termos únicos.")
                break # Se leu com sucesso, para o loop de encodings
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"Erro ao ler linha do CSV: {e}")
                continue
                
    except Exception as e:
        print(f"⚠️ Erro crítico ao abrir CSV: {e}")

if not csv_ativo:
    print(f"⚠️ AVISO CRÍTICO: '{ARQCSV}' não foi carregado corretamente. O robô usará apenas palavras-chave genéricas.")

# --- 2. CONFIGURAÇÃO DE ESTADOS ---
ESTADOS_NE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_OUTROS = [
    'ES', 'RJ', 'SP', 'MG',         # Sudeste
    'GO', 'MT', 'MS', 'DF',         # Centro-Oeste
    'AM', 'PA', 'TO'                # Norte Selecionado
]
ESTADOS_ALVO = ESTADOS_NE + ESTADOS_OUTROS

# --- 3. BLACKLIST (Exclusão Imediata pelo Objeto) ---
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
    # NOVOS TERMOS:
    "CONSIGNACAO", "INTENCAO", "GENEROS ALIMENTICIOS", 
    "ALIMENTACAO ESCOLAR", "PNAE", "COFFEE BREAK", 
    "CAFE REGIONAL", "KIT LANCHE", "GELO", "AGUA MINERAL", 
    "SEGURANCA PUBLICA", "VIDEOMONITORAMENTO", "MERENDA"
]

# --- 4. WHITELIST GLOBAL (Termos de Saúde/Gestão) ---
WHITELIST_GLOBAL_RAW = [
    "REMEDIO", "FARMACO", 
    "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", 
    "ANSIOLITIC", "DIABETIC", "GLICEMIC", "MEDICAMENT CONTROLAD",
    # NOVOS TERMOS GLOBAIS:
    "ATENCAO BASICA", "RENAME", "REMUME", "MAC", 
    "VIGILANCIA EM SAUDE", "ASSISTENCIA FARMACEUTICA", "GESTAO DO SUS"
]

# --- 5. WHITELIST REGIONAL (Nordeste) ---
WHITELIST_NE_RAW = [
    "FRALDA", "ABSORVENTE", "SORO",
    "DIETA ENTERAL", "DIETA", "FORMULA", "PROTEIC", 
    "CALORIC", "GAZE", "ATADURA",
    # NOVOS TERMOS NE:
    "MATERIAL PENSO", "MMH"
]

BLACKLIST_NORM = [normalize(x) for x in BLACKLIST_RAW]
WHITELIST_GLOBAL_NORM = [normalize(x) for x in WHITELIST_GLOBAL_RAW]
WHITELIST_NE_NORM = [normalize(x) for x in WHITELIST_NE_RAW]

# DATA DE CORTE: 01/01/2026
data_limite = datetime(2026, 1, 1, 0, 0, 0)

if not os.path.exists(ARQDADOS):
    print(f"❌ Arquivo {ARQDADOS} não encontrado."); exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    todos = json.load(f)

limpos = []
duplicatas = set()

# Contadores para debug
c_data = 0
c_geo = 0
c_blacklist = 0
c_item_csv = 0

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
                c_data += 1
                continue
    except: pass

    # 2. Filtro Geográfico
    uf = preg.get('uf', '').upper()
    if uf not in ESTADOS_ALVO:
        c_geo += 1
        continue

    # 3. Filtro de Objeto (Bloqueio)
    objeto_txt = preg.get('objeto', '')
    objeto_norm = normalize(objeto_txt)
    
    # Verifica Blacklist
    if any(t in objeto_norm for t in BLACKLIST_NORM):
        # Exceção: Se for DIETA/FORMULA, ignora blacklist de alimentos
        if not ("DIETA" in objeto_norm or "FORMULA" in objeto_norm):
            c_blacklist += 1
            continue 

    # 4. VALIDAÇÃO POR ITEM (O CORAÇÃO DO SISTEMA)
    raw_itens = preg.get('itensraw', [])
    aprovado = False
    
    # Prioridade: Validação via CSV
    if csv_ativo and len(catalogo_produtos) > 0:
        for item in raw_itens:
            desc_item = normalize(item.get('descricao', ''))
            # Verifica se algum termo do CSV está contido na descrição do item
            for termo_csv in catalogo_produtos:
                if termo_csv in desc_item:
                    aprovado = True
                    break 
            if aprovado: break
            
        # Fallback de Segurança:
        # Se não achou no CSV, mas o Objeto é MUITO forte (Ex: "AQUISIÇÃO DE MEDICAMENTOS"),
        # podemos considerar aprovar para não perder editais com descrições ruins nos itens.
        if not aprovado:
            if any(t in objeto_norm for t in WHITELIST_GLOBAL_NORM): aprovado = True
            elif uf in ESTADOS_NE and any(t in objeto_norm for t in WHITELIST_NE_NORM): aprovado = True
            
    else:
        # Modo sem CSV (Usa apenas Whitelist de Objeto)
        if any(t in objeto_norm for t in WHITELIST_GLOBAL_NORM): aprovado = True
        elif uf in ESTADOS_NE and any(t in objeto_norm for t in WHITELIST_NE_NORM): aprovado = True
        elif "MATERIAL DE LIMPEZA" in objeto_norm:
             for item in raw_itens:
                d = normalize(item.get('descricao', ''))
                if "ALCOOL" in d and "70" in d:
                    aprovado = True; break

    if not aprovado:
        c_item_csv += 1
        continue

    # 5. Processamento Final
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
            
            # Lógica ME/EPP
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

print(f"📊 Relatório de Filtros:")
print(f"   - Ignorados por Data (< 2026): {c_data}")
print(f"   - Ignorados por Região: {c_geo}")
print(f"   - Ignorados por Blacklist: {c_blacklist}")
print(f"   - Ignorados por Falta de Item/CSV: {c_item_csv}")
print(f"✅ FINAL: {len(limpos)} pregões aprovados.")

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos, f, ensure_ascii=False)
