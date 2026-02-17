import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

# ==============================================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ==============================================================================
ARQDADOS = 'dadosoportunidades.json.gz'          
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'     
ARQ_CATALOGO = 'Exportar Dados.csv'              
DATA_CORTE_2026 = datetime(2026, 1, 1)           

# 🗺️ GEOGRAFIA
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']

# ==============================================================================
# 🧠 INTELIGÊNCIA DE FILTRAGEM
# ==============================================================================

# 1. ITENS UNIVERSAIS NE (Passam por cima de tudo no Nordeste)
# Atenção: Se esses termos aparecerem, a secretaria não importa.
TERMOS_UNIVERSAIS_NE = [
    "FRALDA", "ABSORVENTE", "ALCOOL 70", "ALCOOL ETILICO", "ALCOOL GEL", "ALCOOL EM GEL"
]

# 2. VETOS ABSOLUTOS
VETOS_IMEDIATOS = [
    "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO", 
    "MANUTENCAO", "UNIFORME", "TEXTIL", "REFORMA", "LIMPEZA PREDIAL", 
    "LAVANDERIA", "IMPRESSAO", "CONSULTORIA", "TREINAMENTO", "VIGILANCIA",
    "PORTARIA", "RECEPCAO", "EVENTOS", "BUFFET", "COFFEE BREAK"
]

# 3. TERMOS REGIONALIZADOS
TERMOS_NE_MMH_NUTRI = [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", 
    "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS", "MASCARA", 
    "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO", "DIETA", 
    "NUTRICAO CLINICA", "NUTRICAO PARENTERAL"
]

# 4. TERMOS DE SALVAMENTO
TERMOS_SALVAMENTO = ["MEDICAMENT", "FARMAC", "REMEDIO", "FARMACO", "DROGARIA"]

# 5. TERMOS DE CONTEXTO
CONTEXTO_SAUDE = ["HOSPITALAR", "DIETA", "MEDICAMENTO", "SAUDE", "CLINICA", "PACIENTE", "UBS"]

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# --- CARREGAMENTO DO CATÁLOGO ---
CATALOGO = set()
if os.path.exists(ARQ_CATALOGO):
    try:
        for enc in ['utf-8', 'latin-1', 'cp1252']:
            try:
                with open(ARQ_CATALOGO, 'r', encoding=enc) as f:
                    leitor = csv.reader(f, delimiter=';')
                    next(leitor, None) 
                    for row in leitor:
                        if row:
                            termo = normalize(row[0])
                            if len(termo) > 4: CATALOGO.add(termo)
                break
            except: continue
        print(f"📚 Catálogo Inteligente carregado: {len(CATALOGO)} produtos.")
    except: print("⚠️ Aviso: Catálogo não encontrado.")

def analisar_pertinencia(edital):
    obj_raw = edital.get('obj', '')
    obj = normalize(obj_raw)
    uf = edital.get('uf', '').upper()
    itens = edital.get('itens', [])

    # 🛑 REGRA 0: O MURO DE CONCRETO (Bloqueio Geográfico)
    if uf in ESTADOS_BLOQUEADOS:
        return False

    # 🌟 REGRA SUPERIOR: ITENS UNIVERSAIS NE
    # Se for do NE e tiver Fralda/Absorvente/Alcool, passa DIRETO.
    # Ignora se é Limpeza, Serviço, Educação, etc.
    if uf in NE_ESTADOS:
        if any(univ in obj for univ in TERMOS_UNIVERSAIS_NE):
            return True

    # 🛑 REGRA 1: O VETO DE LIXO (Só se aplica se NÃO for item universal acima)
    for veto in VETOS_IMEDIATOS:
        if veto in obj:
            return False

    # ⚠️ REGRA 2: CASOS ESPECÍFICOS (Contexto)
    if "MEDICINA" in obj or "MEDICO" in obj:
        if ("GASES" in obj or "OXIGENIO" in obj) and not any(s in obj for s in TERMOS_SALVAMENTO):
            return False

    if "FORMULA" in obj or "LEITE" in obj:
        if not any(ctx in obj for ctx in CONTEXTO_SAUDE):
            return False

    # 🎯 REGRA 3: GEOGRAFIA VS ALVOS
    eh_do_nordeste = uf in NE_ESTADOS
    tem_termo_ne = any(t in obj for t in TERMOS_NE_MMH_NUTRI)
    tem_remedio_explicito = any(t in obj for t in TERMOS_SALVAMENTO)

    if eh_do_nordeste and tem_termo_ne:
        return True

    if tem_remedio_explicito:
        return True

    # ⚓ REGRA 4: O "PENTE FINO" (Catálogo)
    match_catalogo = 0
    if CATALOGO:
        for it in itens:
            desc_item = normalize(it.get('d', ''))
            
            # Filtro interno de item
            if any(x in desc_item for x in ["PNEU", "GASOLINA", "ARROZ", "FEIJAO"]):
                continue

            for prod in CATALOGO:
                if prod in desc_item:
                    match_catalogo += 1
                    break
            
            if match_catalogo >= 1:
                return True

    return False

# ==============================================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ==============================================================================

if not os.path.exists(ARQDADOS): 
    print("❌ Arquivo de dados não encontrado.")
    exit()

print(f"🔄 Iniciando Auditoria Completa no Banco de Dados...")
print(f"📅 Data de Corte: {DATA_CORTE_2026.strftime('%d/%m/%Y')}")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
    banco_bruto = json.load(f)

inicial = len(banco_bruto)
banco_final = []
web_data = []

for p in banco_bruto:
    try:
        dt = datetime.fromisoformat(p.get('dt_enc', '').replace('Z', '+00:00')).replace(tzinfo=None)
        if dt < DATA_CORTE_2026: continue 
    except: continue

    if analisar_pertinencia(p):
        
        banco_final.append(p)
        
        itens_fmt = []
        c_ex = 0
        for it in p.get('itens', []):
            is_ex = int(it.get('benef') or 4) in [1, 2, 3]
            if is_ex: c_ex += 1
            
            itens_fmt.append({
                'n': it.get('n'), 
                'desc': it.get('d'), 
                'qtd': it.get('q', 0),
                'un': it.get('u', ''), 
                'valUnit': it.get('v_est', 0),
                'valHomologado': it.get('res_val', 0), 
                'fornecedor': it.get('res_forn'),
                'situacao': it.get('sit', 'ABERTO'), 
                'me_epp': is_ex
            })
            
        web_data.append({
            'id': p.get('id'), 
            'uf': p.get('uf'), 
            'uasg': p.get('uasg'),
            'orgao': p.get('org'), 
            'unidade': p.get('unid_nome'),
            'edital': p.get('edit'), 
            'cidade': p.get('cid'), 
            'objeto': p.get('obj'),
            'valor_estimado': p.get('val_tot', 0), 
            'data_enc': p.get('dt_enc'),
            'link': p.get('link'),
            'tipo_licitacao': "EXCLUSIVO" if c_ex==len(itens_fmt) and len(itens_fmt)>0 else "AMPLO",
            'itens': itens_fmt
        })

web_data.sort(key=lambda x: x['data_enc'], reverse=True)

print("💾 Salvando alterações...")

with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f: 
    json.dump(banco_final, f, ensure_ascii=False)

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: 
    json.dump(web_data, f, ensure_ascii=False)

removidos = inicial - len(banco_final)

print("\n" + "="*40)
print(f"✅ AUDITORIA CONCLUÍDA COM SUCESSO!")
print("="*40)
print(f"   📉 Registros Analisados:    {inicial}")
print(f"   🗑️  Lixo/Antigos Removidos: {removidos}")
print(f"   💎 Banco Limpo (Salvo):     {len(banco_final)}")
print(f"   🌐 Disponível no Monitor:   {len(web_data)}")
print("="*40 + "\n")
