import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

# --- CONFIGURAÇÕES DE FICHEIROS ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQCSV = 'Exportar Dados.csv'

print("🧹 LIMPEZA V29 - GESTOR DE BANCO DE DADOS")

def normalize(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)).upper()
                   if unicodedata.category(c) != 'Mn')

# --- 1. CARREGAMENTO DO CATÁLOGO (CSV) ---
catalogo_produtos = set()
csv_ativo = False

if os.path.exists(ARQCSV):
    try:
        # Tenta diferentes encodings comuns em ficheiros CSV brasileiros
        for enc in ['latin-1', 'utf-8', 'cp1252']:
            try:
                with open(ARQCSV, 'r', encoding=enc) as f:
                    leitor = csv.reader(f)
                    next(leitor, None) # Pular cabeçalho
                    for linha in leitor:
                        if linha:
                            # Captura colunas de descrição, fármaco e nome técnico
                            for i in [0, 1, 5]:
                                if len(linha) > i and linha[i]:
                                    termo = normalize(linha[i].strip())
                                    if len(termo) > 3:
                                        catalogo_produtos.add(termo)
                csv_ativo = True
                break
            except UnicodeDecodeError:
                continue
        print(f"📦 Catálogo CSV: {len(catalogo_produtos)} termos carregados.")
    except Exception as e:
        print(f"⚠️ Erro ao processar CSV: {e}")

# --- 2. DEFINIÇÃO DE FILTROS ---
ESTADOS_ALVO = [
    'AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', # Nordeste
    'ES', 'RJ', 'SP', 'MG', 'GO', 'MT', 'MS', 'DF', 'AM', 'PA', 'TO' # Outros
]

BLACKLIST = [normalize(x) for x in [
    "TRANSPORTE", "VEICULO", "MANUTENCAO", "LIMPEZA PREDIAL", "AR CONDICIONADO", "OBRAS", 
    "ENGENHARIA", "CONFECCAO", "ESTANTE", "MOBILIARIO", "INFORMATICA", "COMPUTADOR",
    "TONER", "CARTUCHO", "ANIMAIS", "RACAO", "CONSTRUCAO", "ELETRICO", "ESPORTIVO",
    "LOCACAO", "EXAME", "RECEITUARIO", "SERVICO", "ADESAO", "GASES MEDICINAIS",
    "CONSIGNACAO", "INTENCAO", "ALIMENTAR", "MERENDA", "COFFEE BREAK", "AGUA MINERAL",
    "SEGURANCA PUBLICA", "VIDEOMONITORAMENTO", "PNAE", "KIT LANCHE", "GELO"
]]

WHITELIST_PHARMA = [normalize(x) for x in [
    "REMEDIO", "FARMACO", "HIPERTENSIV", "INJETAV", "ONCOLOGIC", "ANALGESIC", 
    "ANTI-INFLAMAT", "ANTIBIOTIC", "ANTIDEPRESSIV", "ANSIOLITIC", "DIABETIC", 
    "GLICEMIC", "MEDICAMENT", "ATENCAO BASICA", "RENAME", "REMUME", "SAUDE", 
    "HOSPITAL", "SORO", "FRALDA", "ABSORVENTE", "DIETA", "FORMULA", "MMH"
]]

# --- 3. PROCESSAMENTO E PURGA ---
if not os.path.exists(ARQDADOS):
    print("❌ Banco de dados não encontrado. Finalizando.")
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

    # --- FILTRO DE RELEVÂNCIA ---
    if p_uf not in ESTADOS_ALVO:
        removidos_filtro += 1; continue

    # Verifica Blacklist (Exceção para Dietas e Fórmulas)
    if any(t in p_obj for t in BLACKLIST):
        if not any(t in p_obj for t in ["DIETA", "FORMULA", "NUTRICIONAL"]):
            removidos_filtro += 1; continue

    # Validação Pharma (Objeto ou Itens via CSV)
    e_pharma = any(t in p_obj for t in WHITELIST_PHARMA)
    if not e_pharma and csv_ativo:
        for it in p_itens:
            desc_item = normalize(it.get('d', ''))
            if any(p in desc_item for p in catalogo_produtos):
                e_pharma = True; break
    
    if not e_pharma:
        removidos_filtro += 1; continue

    # --- FILTRO DE VALIDADE (180/360 DIAS) ---
    try:
        # Tratamento da data de encerramento
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        idade_dias = (hoje - dt_enc).days
        
        # Estado do pregão: Concluído (todos os itens com vencedor) ou Pendente
        total_itens = len(p_itens)
        itens_com_vencedor = sum(1 for i in p_itens if i.get('res_forn'))
        concluido = (total_itens > 0 and itens_com_vencedor == total_itens)

        # Aplicação das réguas de exclusão
        if concluido and idade_dias > 180:
            removidos_data += 1; continue
        if not concluido and idade_dias > 360:
            removidos_data += 1; continue
    except:
        pass # Mantém se a data for inválida

    banco_importante.append(preg)

# --- 4. SALVAMENTO E ATUALIZAÇÃO ---

# A. Atualiza o Banco Principal (Sobrescreve removendo o "lixo")
print(f"♻️  Purga: {removidos_filtro} irrelevantes e {removidos_data} antigos removidos.")
print(f"💾 Atualizando banco principal com {len(banco_importante)} registos...")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f:
    json.dump(banco_importante, f, ensure_ascii=False)

# B. Gera o ficheiro formatado para o Monitor Web
web_data = []
for p in banco_importante:
    itens_formatados = []
    count_me = 0
    for it in p['itens']:
        is_me = int(it.get('benef') or 4) in [1, 3]
        if is_me: count_me += 1
        
        itens_formatados.append({
            'n': it.get('n'),
            'desc': it.get('d'),
            'qtd': float(it.get('q', 0)),
            'un': it.get('u'),
            'valUnit': float(it.get('v_est', 0)),
            'me_epp': is_me,
            'situacao': it.get('sit', 'ABERTO'),
            'fornecedor': it.get('res_forn'),
            'valHomologado': float(it.get('res_val', 0))
        })

    tipo_lic = "AMPLO"
    if itens_formatados:
        if count_me == len(itens_formatados): tipo_lic = "EXCLUSIVO"
        elif count_me > 0: tipo_lic = "PARCIAL"

    web_data.append({
        'id': p['id'],
        'uf': p['uf'],
        'cidade': p['cid'],
        'orgao': p['org'],
        'unidade': p['unid_nome'],
        'uasg': p['uasg'],
        'edital': p['edit'],
        'valor_estimado': round(float(p.get('val_tot', 0)), 2),
        'data_enc': p['dt_enc'],
        'objeto': p['obj'][:400], # Limite para não sobrecarregar o HTML
        'link': p['link'],
        'tipo_licitacao': tipo_lic,
        'itens': itens_formatados
    })

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
    json.dump(web_data, f, ensure_ascii=False)

print("✅ Operação finalizada com sucesso!")
