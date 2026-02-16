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

print("🧹 LIMPEZA V29 - FAXINEIRO DO BANCO DE DADOS")

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CATÁLOGO (CSV) ---
catalogo_produtos = set()
csv_ativo = False

if os.path.exists(ARQ_CSV):
    try:
        with open(ARQ_CSV, 'r', encoding='latin-1') as f:
            leitor = csv.reader(f)
            next(leitor, None) 
            for linha in leitor:
                if linha:
                    # Captura colunas de descrição e fármaco
                    for i in [0, 1, 5]:
                        if len(linha) > i: catalogo_produtos.add(normalize(linha[i]))
        csv_ativo = True
        print(f"📦 Catálogo CSV: {len(catalogo_produtos)} termos carregados.")
    except Exception as e:
        print(f"⚠️ Erro ao ler CSV: {e}")

# --- 2. CONFIGURAÇÕES DE FILTROS ---
ESTADOS_ALVO = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', 'ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO']

BLACKLIST = [normalize(x) for x in [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", "AR CONDICIONADO", "OBRAS", 
    "ENGENHARIA", "CONFECCAO", "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "ANIMAIS", "RACAO", "CONSTRUCAO", "ELETRICO", "ESPORTIVO",
    "LOCACAO", "EXAME", "RECEITUARIO", "SERVICO", "ADESAO", "GASES MEDICINAIS",
    "CONSIGNACAO", "INTENCAO", "ALIMENTAR", "MERENDA", "COFFEE BREAK", "AGUA MINERAL"
]]

WHITELIST_PHARMA = [normalize(x) for x in [
    "REMEDIO", "FARMACO", "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", "ANSIOLITIC", "DIABETIC", 
    "GLICEMIC", "MEDICAMENT", "ATENCAO BASICA", "RENAME", "REMUME", "SAUDE", "HOSPITAL"
]]

# --- 3. PROCESSAMENTO E PURGA ---
if not os.path.exists(ARQDADOS):
    print("❌ Banco de dados não encontrado.")
    exit()

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
    banco_bruto = json.load(f)

hoje = datetime.now()
banco_importante = []
removidos_filtro = 0
removidos_data = 0

for preg in banco_bruto:
    id_lic = preg.get('id')
    p_uf = (preg.get('uf') or '').upper()
    p_obj = normalize(preg.get('obj') or '')
    p_itens = preg.get('itens', [])
    dt_enc_str = preg.get('dt_enc')

    # --- FILTRO 1: RELEVÂNCIA (Whitelist/Blacklist/CSV) ---
    if p_uf not in ESTADOS_ALVO:
        removidos_filtro += 1; continue

    # Se cair na Blacklist e não for Dieta/Fórmula, remove
    if any(t in p_obj for t in BLACKLIST):
        if not any(t in p_obj for t in ["DIETA", "FORMULA"]):
            removidos_filtro += 1; continue

    # Validação de Conteúdo Pharma
    e_importante = any(t in p_obj for t in WHITELIST_PHARMA)
    if not e_importante and csv_ativo:
        for it in p_itens:
            if any(p in normalize(it.get('d')) for p in catalogo_produtos):
                e_importante = True; break
    
    if not e_importante:
        removidos_filtro += 1; continue

    # --- FILTRO 2: VALIDADE TEMPORAL (Dieta de 180/360 dias) ---
    try:
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        idade_dias = (hoje - dt_enc).days
        
        # Verifica se todos os itens têm resultado
        total_itens = len(p_itens)
        com_resultado = sum(1 for i in p_itens if 'res_forn' in i)
        concluido = (total_itens > 0 and com_resultado == total_itens)

        # Regra dos 180 dias (Concluídos) ou 360 dias (Pendentes)
        if concluido and idade_dias > 180:
            removidos_data += 1; continue
        if not concluido and idade_dias > 360:
            removidos_data += 1; continue
    except:
        pass # Se não tiver data, mantemos por segurança

    # Se passou em tudo, é importante
    banco_importante.append(preg)

# --- 4. SALVAMENTO DUPLO (SOBRESCREVENDO O BANCO) ---

# A. Atualiza o Banco de Dados Principal (Purga total do lixo)
print(f"♻️  Purga concluída: {removidos_filtro} itens irrelevantes e {removidos_data} antigos removidos.")
print(f"💾 Sobrescrevendo banco principal com {len(banco_importante)} registros importantes...")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(banco_importante, f, ensure_ascii=False)

# B. Gera o arquivo do Monitor Web (Formatado)
limpos_web = []
for p in banco_importante:
    itens_formatados = []
    count_me = 0
    for it in p['itens']:
        is_me = int(it.get('benef') or 4) in [1, 3]
        if is_me: count_me += 1
        itens_formatados.append({
            'n': it['n'], 'desc': it['d'], 'qtd': it['q'], 'un': it['u'],
            'valUnit': it['v_est'], 'me_epp': is_me, 'situacao': it['sit'],
            'fornecedor': it.get('res_forn'), 'valHomologado': float(it.get('res_val') or 0)
        })

    limpos_web.append({
        'id': p['id'], 'uf': p['uf'], 'cidade': p['cid'], 'orgao': p['org'],
        'unidade': p['unid_nome'], 'uasg': p['uasg'], 'edital': p['edit'],
        'valor_estimado': p['val_tot'], 'data_enc': p['dt_enc'],
        'objeto': p['obj'], 'link': p['link'],
        'tipo_licitacao': "EXCLUSIVO" if count_me == len(itens_formatados) else ("PARCIAL" if count_me > 0 else "AMPLO"),
        'itens': itens_formatados
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(limpos_web, f, ensure_ascii=False)

print("✅ Operação finalizada com sucesso!")
