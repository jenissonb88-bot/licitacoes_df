import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

# --- CONFIGURAÇÕES DE ARQUIVOS ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

# DATA DE CORTE ABSOLUTA (Nada antes de 2026 entra)
DATA_CORTE_FIXA = datetime(2026, 1, 1)

print("🧹 LIMPEZA V31 - CORTE 2026 E FAXINA TOTAL")

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CATÁLOGO (CSV) ---
catalogo_produtos = set()
csv_ativo = False

if os.path.exists(ARQCSV):
    try:
        for enc in ['latin-1', 'utf-8', 'cp1252']:
            try:
                with open(ARQCSV, 'r', encoding=enc) as f:
                    leitor = csv.reader(f)
                    next(leitor, None) 
                    for linha in leitor:
                        if linha:
                            for i in [0, 1, 5]:
                                if len(linha) > i and linha[i]:
                                    termo = normalize(linha[i].strip())
                                    if len(termo) > 4:
                                        catalogo_produtos.add(termo)
                csv_ativo = True
                break
            except UnicodeDecodeError:
                continue
        print(f"📦 Catálogo CSV: {len(catalogo_produtos)} termos carregados.")
    except Exception as e:
        print(f"⚠️ Erro ao processar CSV: {e}")

# --- 2. DEFINIÇÃO DE FILTROS ---
ESTADOS_ALVO = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', 'ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

BLACKLIST = [normalize(x) for x in [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "OBRAS", "ENGENHARIA", "CONSTRUCAO",
    "REFORMA", "PINTURA", "FROTA", "PECAS PARA CARRO", "PNEU", "COMBUSTIVEL",
    "LIMPEZA PREDIAL", "AR CONDICIONADO", "INFORMATICA", "COMPUTADOR", "SOFTWARE",
    "TONER", "CARTUCHO", "IMPRESSORA", "MOBILIARIO", "ESTANTE", "CADEIRA", "MESA",
    "PAPELARIA", "EXPEDIENTE", "FARDAMENTO", "UNIFORME", "CONFECCAO", "COPA", "COZINHA",
    "ALIMENTAR", "MERENDA", "COFFEE BREAK", "AGUA MINERAL", "GELO", "KIT LANCHE",
    "ESPORTIVO", "BRINQUEDO", "EVENTOS", "SHOW", "PALCO", "SEGURANCA", "VIGILANCIA",
    "LOCACAO", "ASSESSORIA", "CONSULTORIA", "TREINAMENTO", "CURSO", "FUNERARIO",
    "GASES MEDICINAIS", "OXIGENIO", "REFEICAO", "RESTAURANTE", "HOSPEDAGEM"
]]

WHITELIST_PHARMA = [normalize(x) for x in [
    "MEDICAMENTO", "REMEDIO", "FARMACO", "HIPERTENSIV", "INJETAV", "ONCOLOGIC", 
    "ANALGESIC", "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", "ANSIOLITIC", 
    "DIABETIC", "GLICEMIC", "SORO", "FRALDA", "ABSORVENTE", "MMH", "MATERIAL MEDICO"
]]

# --- 3. PROCESSAMENTO ---
if not os.path.exists(ARQDADOS):
    print("❌ Banco de dados não encontrado.")
    exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    banco_bruto = json.load(f)

hoje = datetime.now()
banco_importante = []
contagem = {"filtros": 0, "blacklist": 0, "tempo": 0, "corte_2026": 0}

for preg in banco_bruto:
    p_uf = (preg.get('uf') or '').upper()
    p_obj = normalize(preg.get('obj') or '')
    p_itens = preg.get('itens', [])
    dt_enc_str = preg.get('dt_enc')

    # REGRA 0: CORTE ABSOLUTO 01/01/2026
    try:
        # Tenta converter a data de encerramento para comparação
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA:
            contagem["corte_2026"] += 1; continue
    except:
        # Se não tiver data ou der erro na conversão, remove por precaução (é dado sujo)
        contagem["corte_2026"] += 1; continue

    # REGRA 1: UF ALVO
    if p_uf not in ESTADOS_ALVO:
        contagem["filtros"] += 1; continue

    # REGRA 2: BLACKLIST (Com exceção para Dietas)
    if any(t in p_obj for t in BLACKLIST):
        if not any(t in p_obj for t in ["DIETA", "FORMULA", "NUTRICIONAL", "ENTERAL"]):
            contagem["blacklist"] += 1; continue

    # REGRA 3: VALIDAÇÃO PHARMA (Objeto ou Catálogo CSV)
    e_pharma = any(t in p_obj for t in WHITELIST_PHARMA)
    if not e_pharma and csv_ativo:
        for it in p_itens:
            desc_item = normalize(it.get('d', ''))
            if any(p in desc_item for p in catalogo_produtos):
                e_pharma = True; break
    
    if not e_pharma:
        contagem["filtros"] += 1; continue

    # REGRA 4: DIETA DE DADOS (180/360 DIAS)
    idade_dias = (hoje - dt_enc).days
    total_itens = len(p_itens)
    com_vencedor = sum(1 for i in p_itens if i.get('res_forn'))
    concluido = (total_itens > 0 and com_vencedor == total_itens)

    if concluido and idade_dias > 180:
        contagem["tempo"] += 1; continue
    if not concluido and idade_dias > 360:
        contagem["tempo"] += 1; continue

    # Se chegou aqui, o dado é válido
    banco_importante.append(preg)

# --- 4. SALVAMENTO ---

print(f"♻️  Itens removidos:")
print(f"   - Anteriores a 2026: {contagem['corte_2026']}")
print(f"   - Irrelevantes: {contagem['filtros']}")
print(f"   - Blacklist: {contagem['blacklist']}")
print(f"   - Dieta Temporal: {contagem['tempo']}")
print(f"💾 Banco lapidado: {len(banco_importante)} registros.")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(banco_importante, f, ensure_ascii=False)

# Gera arquivo Monitor Web
web_data = []
for p in banco_importante:
    itens_fmt = []
    count_me = 0
    for it in p['itens']:
        is_me = int(it.get('benef') or 4) in [1, 3]
        if is_me: count_me += 1
        itens_fmt.append({
            'n': it.get('n'), 'desc': it.get('d'), 'qtd': it.get('q'), 'un': it.get('u'),
            'valUnit': it.get('v_est'), 'me_epp': is_me, 'situacao': it.get('sit'),
            'fornecedor': it.get('res_forn'), 'valHomologado': it.get('res_val')
        })

    web_data.append({
        'id': p['id'], 'uf': p['uf'], 'cidade': p['cid'], 'orgao': p['org'],
        'unidade': p['unid_nome'], 'uasg': p['uasg'], 'edital': p['edit'],
        'valor_estimado': round(float(p.get('val_tot', 0)), 2), 'data_enc': p['dt_enc'],
        'objeto': p['obj'][:400], 'link': p['link'], 
        'tipo_licitacao': "EXCLUSIVO" if count_me == len(itens_fmt) else ("PARCIAL" if count_me > 0 else "AMPLO"), 
        'itens': itens_fmt
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(web_data, f, ensure_ascii=False)

print("✅ Operação finalizada!")
