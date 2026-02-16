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
DATA_CORTE_FIXA = datetime(2026, 1, 1)

print("🧹 LIMPEZA V32 - PERITO EM BARREIRA DUPLA")

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CATÁLOGO DROGAFONTE (CSV) ---
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
                            # Captura Descrição, Fármaco e Nome Técnico
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

# --- 2. DEFINIÇÃO DE FILTROS AGRESSIVOS (BARREIRA DUPLA) ---
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
    "GASES MEDICINAIS", "OXIGENIO", "REFEICAO", "RESTAURANTE", "HOSPEDAGEM", "LIXO", "POSTES"
]]

WHITELIST_PHARMA = [normalize(x) for x in [
    "MEDICAMENTO", "REMEDIO", "FARMACO", "HIPERTENSIV", "INJETAV", "ONCOLOGIC", 
    "ANALGESIC", "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", "ANSIOLITIC", 
    "DIABETIC", "GLICEMIC", "SORO", "FRALDA", "ABSORVENTE", "MMH", "MATERIAL MEDICO",
    "INSUMO HOSPITALAR", "SAUDE BUCAL", "ODONTOLOGIC"
]]

# --- 3. PROCESSAMENTO E PURGA ---
if not os.path.exists(ARQDADOS):
    print("❌ Banco de dados não encontrado.")
    exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    try:
        banco_bruto = json.load(f)
    except:
        print("⚠️ Erro ao ler banco. Arquivo pode estar corrompido.")
        exit()

hoje = datetime.now()
banco_importante = []
contagem = {"corte_2026": 0, "uf": 0, "blacklist": 0, "irrelevante": 0, "tempo": 0}

for preg in banco_bruto:
    dt_enc_str = preg.get('dt_enc')
    
    # REGRA 0: CORTE 2026
    try:
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_FIXA:
            contagem["corte_2026"] += 1; continue
    except:
        contagem["corte_2026"] += 1; continue

    # REGRA 1: UF ALVO
    p_uf = (preg.get('uf') or '').upper()
    if p_uf not in ESTADOS_ALVO:
        contagem["uf"] += 1; continue

    # REGRA 2: BLACKLIST (Com exceção para Dietas/Nutrição)
    p_obj = normalize(preg.get('obj') or '')
    if any(t in p_obj for t in BLACKLIST):
        if not any(t in p_obj for t in ["DIETA", "FORMULA", "NUTRICIONAL", "ENTERAL"]):
            contagem["blacklist"] += 1; continue

    # REGRA 3: VALIDAÇÃO PHARMA (Whitelist ou Catálogo CSV)
    p_itens = preg.get('itens', [])
    e_pharma = any(t in p_obj for t in WHITELIST_PHARMA)
    
    if not e_pharma and csv_ativo:
        # Busca profunda nos itens se o objeto for genérico
        for it in p_itens:
            desc_item = normalize(it.get('d', ''))
            if any(p in desc_item for p in catalogo_produtos):
                e_pharma = True; break
    
    if not e_pharma:
        contagem["irrelevante"] += 1; continue

    # REGRA 4: DIETA DE DADOS (180/360 DIAS)
    idade_dias = (hoje - dt_enc).days
    total_itens = len(p_itens)
    com_vencedor = sum(1 for i in p_itens if i.get('res_forn'))
    concluido = (total_itens > 0 and com_vencedor == total_itens)

    if concluido and idade_dias > 180:
        contagem["tempo"] += 1; continue
    if not concluido and idade_dias > 360:
        contagem["tempo"] += 1; continue

    # Se passou por tudo, mantemos
    banco_importante.append(preg)

# --- 4. SALVAMENTO E SINCRONIZAÇÃO ---

# A. Limpeza do Banco Original (Sobrescrita para economizar espaço)
print(f"♻️  Resultados da Faxina:")
print(f"   - Removidos Pré-2026: {contagem['corte_2026']}")
print(f"   - Fora da Região: {contagem['uf']}")
print(f"   - Blacklist (Lixo): {contagem['blacklist']}")
print(f"   - Sem interesse Pharma: {contagem['irrelevante']}")
print(f"   - Antigos (Dieta): {contagem['tempo']}")
print(f"💾 Banco Lapidado: {len(banco_importante)} registros.")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(banco_importante, f, ensure_ascii=False)

# B. Geração do arquivo otimizado para o Monitor Web
web_data = []
for p in banco_importante:
    itens_fmt = []
    count_exclusivo = 0
    
    for it in p['itens']:
        # Identifica se o item é Exclusivo ME/EPP (SIM/NÃO)
        # Códigos 1, 2, 3 no PNCP são reservados/exclusivos para ME/EPP
        benef_id = int(it.get('benef') or 4)
        is_exclusivo = benef_id in [1, 2, 3]
        if is_exclusivo: count_exclusivo += 1
        
        itens_fmt.append({
            'n': it.get('n'),
            'desc': it.get('d'),
            'qtd': it.get('q'),
            'un': it.get('u'),
            'valUnit': float(it.get('v_est', 0)),
            'valHomologado': float(it.get('res_val', 0)),
            'fornecedor': it.get('res_forn'),
            'situacao': it.get('sit', 'ABERTO'),
            'me_epp': is_exclusivo # Boolean para o index.html decidir SIM/NÃO
        })

    # Classificação do Pregão baseada nos itens
    tipo_lic = "AMPLO"
    if itens_fmt:
        if count_exclusivo == len(itens_fmt): tipo_lic = "EXCLUSIVO"
        elif count_exclusivo > 0: tipo_lic = "PARCIAL"

    web_data.append({
        'id': p['id'],
        'uf': p['uf'],
        'uasg': p['uasg'],
        'orgao': p['org'],
        'unidade': p['unid_nome'],
        'edital': p['edit'],
        'cidade': p['cid'],
        'objeto': p['obj'],
        'valor_estimado': round(float(p.get('val_tot', 0)), 2),
        'data_enc': p['dt_enc'],
        'link': p['link'],
        'tipo_licitacao': tipo_lic,
        'itens': itens_fmt
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(web_data, f, ensure_ascii=False)

print("✅ Operação finalizada com sucesso!")
