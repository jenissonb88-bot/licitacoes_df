import json
import gzip
import os
import unicodedata
import csv
from datetime import datetime

# --- CONFIGURAÇÕES ---
ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQ_CATALOGO = 'Exportar Dados.csv'
DATA_CORTE_2026 = datetime(2026, 1, 1)

NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

# --- DICIONÁRIOS DE INTELLIGENCE ---

# 1. VETOS ABSOLUTOS (Se tiver isso, morre na hora)
VETOS_IMEDIATOS = [
    "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO", 
    "MANUTENCAO", "UNIFORME", "TEXTIL", "REFORMA", "GASES MEDICINAIS", 
    "OXIGENIO", "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA", "IMPRESSAO"
]

# 2. TERMOS REGIONALIZADOS (Só passa no NE, a menos que tenha remédio junto)
TERMOS_NE_MMH_NUTRI = [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", 
    "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS", "MASCARA", 
    "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO", "DIETA", "NUTRICAO CLINICA"
]

# 3. TERMOS DE SALVAMENTO (Salvam o edital fora do NE)
TERMOS_SALVAMENTO = ["MEDICAMENT", "FARMAC", "REMEDIO", "FARMACO"]

# 4. TERMOS DE CONTEXTO PARA "FÓRMULA/LEITE"
CONTEXTO_SAUDE = ["HOSPITALAR", "DIETA", "MEDICAMENTO", "SAUDE", "CLINICA", "PACIENTE"]

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
                    next(leitor, None) # Pula cabeçalho
                    for row in leitor:
                        if row:
                            # Normaliza o nome do produto e adiciona à memória
                            termo = normalize(row[0])
                            if len(termo) > 4: # Ignora termos muito curtos para evitar falso positivo
                                CATALOGO.add(termo)
                break
            except: continue
        print(f"📚 Catálogo Inteligente carregado: {len(CATALOGO)} produtos.")
    except: print("⚠️ Aviso: Catálogo não encontrado. Operando sem validação de itens.")

def analisar_pertinencia(edital):
    """
    O CÉREBRO DO ROBÔ: Decide se o edital vive ou morre.
    Retorna True (Aprovado) ou False (Reprovado).
    """
    obj = normalize(edital.get('obj', ''))
    uf = edital.get('uf', '').upper()
    itens = edital.get('itens', [])

    # REGRA 1: O VETO IMEDIATO (Filtro de Ruído)
    # Se for "Prestação de Serviço", "Uniforme", "Gases", tchau.
    for veto in VETOS_IMEDIATOS:
        if veto in obj:
            return False

    # REGRA 2: CASOS ESPECÍFICOS (Medicina, Fórmula)
    # "Medicina" sem "Medicamento" geralmente é serviço médico ou gases
    if "MEDICINA" in obj or "MEDICO" in obj:
        if "GASES" in obj and not any(s in obj for s in TERMOS_SALVAMENTO):
            return False

    # "Fórmula" ou "Leite" solto (sem contexto de saúde)
    if "FORMULA" in obj or "LEITE" in obj:
        if not any(ctx in obj for ctx in CONTEXTO_SAUDE):
            return False

    # REGRA 3: GEOGRAFIA VS ALVOS (Nordeste vs Resto)
    eh_do_nordeste = uf in NE_ESTADOS
    tem_termo_ne = any(t in obj for t in TERMOS_NE_MMH_NUTRI)
    tem_remedio_explicito = any(t in obj for t in TERMOS_SALVAMENTO)

    # Cenário A: É do Nordeste e tem termos de MMH/Nutrição? -> APROVADO
    if eh_do_nordeste and tem_termo_ne:
        return True

    # Cenário B: Tem "Medicamento" escrito no objeto? -> APROVADO (Qualquer lugar)
    if tem_remedio_explicito:
        return True

    # REGRA 4: O "PENTE FINO" (A Validação por Catálogo)
    # Se chegou aqui, é um edital "morno" (ex: "Aquisição Saúde" em SP, ou "MMH" no Sul).
    # Vamos olhar os itens um por um.
    
    match_catalogo = 0
    if CATALOGO:
        for it in itens:
            desc_item = normalize(it.get('d', ''))
            # Verifica se algum produto do catálogo está contido na descrição do item
            # Ex: Catálogo tem "DIPIRONA", Item tem "DIPIRONA SODICA" -> Match!
            for prod in CATALOGO:
                if prod in desc_item:
                    match_catalogo += 1
                    break # Achou um match nesse item, vai pro próximo
            
            # SE ACHARMOS PELO MENOS 1 ITEM VÁLIDO, SALVA O EDITAL TODO
            if match_catalogo >= 1:
                return True

    # Se passou por tudo e não foi salvo...
    return False

# --- EXECUÇÃO PRINCIPAL ---

if not os.path.exists(ARQDADOS): 
    print("❌ Arquivo de dados não encontrado.")
    exit()

print(f"🔄 Iniciando Auditoria Completa no Banco de Dados...")

with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
    banco_bruto = json.load(f)

inicial = len(banco_bruto)
banco_final = []
web_data = []

for p in banco_bruto:
    # 1. Filtro de Data (Corte 2026)
    try:
        dt = datetime.fromisoformat(p.get('dt_enc', '').replace('Z', '+00:00')).replace(tzinfo=None)
        if dt < DATA_CORTE_2026: continue
    except: continue

    # 2. APLICAÇÃO DA INTELIGÊNCIA (Auditoria)
    if analisar_pertinencia(p):
        # Se aprovado, adiciona ao banco limpo
        banco_final.append(p)
        
        # Prepara para o site
        itens_fmt = []
        c_ex = 0
        for it in p.get('itens', []):
            is_ex = int(it.get('benef') or 4) in [1, 2, 3]
            if is_ex: c_ex += 1
            itens_fmt.append({
                'n': it.get('n'), 'desc': it.get('d'), 'qtd': it.get('q', 0),
                'un': it.get('u', ''), 'valUnit': it.get('v_est', 0),
                'valHomologado': it.get('res_val', 0), 'fornecedor': it.get('res_forn'),
                'situacao': it.get('sit', 'ABERTO'), 'me_epp': is_ex
            })
            
        web_data.append({
            'id': p.get('id'), 'uf': p.get('uf'), 'uasg': p.get('uasg'),
            'orgao': p.get('org'), 'unidade': p.get('unid_nome'),
            'edital': p.get('edit'), 'cidade': p.get('cid'), 'objeto': p.get('obj'),
            'valor_estimado': p.get('val_tot', 0), 'data_enc': p.get('dt_enc'),
            'link': p.get('link'),
            'tipo_licitacao': "EXCLUSIVO" if c_ex==len(itens_fmt) and len(itens_fmt)>0 else "AMPLO",
            'itens': itens_fmt
        })

# Ordenação
web_data.sort(key=lambda x: x['data_enc'], reverse=True)

# SOBRESCRITA DOS ARQUIVOS (Salva as alterações)
with gzip.open(ARQDADOS, 'wt', encoding='utf-8') as f: 
    json.dump(banco_final, f, ensure_ascii=False)

with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f: 
    json.dump(web_data, f, ensure_ascii=False)

removidos = inicial - len(banco_final)
print(f"✅ Auditoria Inteligente Concluída!")
print(f"   📉 Registros Analisados: {inicial}")
print(f"   🗑️ Lixo Removido:       {removidos}")
print(f"   💾 Banco Limpo Salvo:    {len(banco_final)}")
