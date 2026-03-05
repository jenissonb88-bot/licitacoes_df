import json
import gzip
import os
import unicodedata
import csv
import sys
import concurrent.futures
from datetime import datetime

ARQDADOS = 'dadosoportunidades.json.gz'
ARQLIMPO = 'pregacoes_pharma_limpos.json.gz'
ARQ_CATALOGO = 'Exportar Dados.csv'
DATA_CORTE_2026 = datetime(2026, 1, 1)

# --- GEOGRAFIA ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
UFS_PERMITIDAS_MMH = NE_ESTADOS  # Apenas Nordeste

# --- VETOS ABSOLUTOS ---
VETOS_ABSOLUTOS = [
    "INTENCAO DE REGISTRO DE PRECO",
    "INTENCAO REGISTRO DE PRECO",
    "CREDENCIAMENTO",
    "ADESAO",
    "IRP",
    "LEILAO",
    "ALIENACAO"
]

# --- VETOS OPERACIONAIS (com variações) ---
VETOS_IMEDIATOS_BASE = [
    "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO",
    "ASFALTICO", "ASFALTO", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS",
    "MANUTENCAO PREVENTIVA", "MANUTENCAO CORRETIVA", "UNIFORME", "TEXTIL",
    "REFORMA", "GASES MEDICINAIS", "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA",
    "IMPRESSAO", "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "LIMPEZA URBANA",
    "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL",
    "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA",
    "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO",
    "VESTUARIO", "INFORMATICA", "COMPUTADORES", "EVENTOS", "REPARO",
    "CORRETIVA", "GERADOR", "VEICULO", "AMBULANCIA", "MOTOCICLETA",
    "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO",
    "EQUIPAMENTO E MATERIA PERMANENTE", "RECARGA", "CONFECCAO",
    "EQUIPAMENTOS PERMANENTES", "MATERIAIS PERMANENTES"
]

TERMOS_NE_MMH_NUTRI = [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA",
    "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO",
    "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO",
    "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE",
    "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS",
    "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL",
    "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO",
    "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO",
    "TOUCA DESCARTAVEL", "TUBO ASPIRACAO", "NUTRICAO ENTERAL",
    "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL",
    "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL", "FORMULA ESPECIA",
    "AGULHAS", "SERINGAS", "PARENTERA", "ENTERAL"
]

TERMOS_SALVAMENTO = [
    "MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA",
    "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO",
    "AQUISICAO DE MEDICAMENTO", "AQUISICAO DE MEDICAMENTOS"
]

def normalize(t):
    if not t:
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

# Normalizar listas
VETOS_ABSOLUTOS = [normalize(x) for x in VETOS_ABSOLUTOS]
VETOS_IMEDIATOS = []
for termo in VETOS_IMEDIATOS_BASE:
    n = normalize(termo)
    VETOS_IMEDIATOS.append(n)
    if not n.endswith('S') and not n.endswith('ES'):
        VETOS_IMEDIATOS.append(n + 'S')
VETOS_IMEDIATOS = list(set(VETOS_IMEDIATOS))

TERMOS_NE_MMH_NUTRI = [normalize(x) for x in TERMOS_NE_MMH_NUTRI]
TERMOS_SALVAMENTO = [normalize(x) for x in TERMOS_SALVAMENTO]

def tem_medicamento_no_texto(texto):
    """Verifica se há termos de medicamentos no texto"""
    if not texto:
        return False
    texto_norm = normalize(texto)
    return any(p in texto_norm for p in TERMOS_SALVAMENTO)

def analisar_pertinencia(obj_norm, uf, itens=None):
    """
    Retorna True se deve manter, False se deve descartar
    """
    # 1. VETOS ABSOLUTOS (sempre vetam)
    for veto in VETOS_ABSOLUTOS:
        if veto in obj_norm:
            return False

    # 2. SUPER PASSE (medicamentos)
    tem_med_objeto = tem_medicamento_no_texto(obj_norm)
    tem_med_itens = False
    if not tem_med_objeto and itens:
        for item in itens:
            desc = item.get('d', '')
            if tem_medicamento_no_texto(desc):
                tem_med_itens = True
                break

    if tem_med_objeto or tem_med_itens:
        # Super passe libera, mas mantém bloqueio de estados bloqueados
        if uf in ESTADOS_BLOQUEADOS:
            return False
        return True

    # 3. VETOS IMEDIATOS
    for veto in VETOS_IMEDIATOS:
        if veto in obj_norm:
            return False

    # 4. MMH/NUTRIÇÃO - Apenas Nordeste
    tem_mmh_nutri = any(t in obj_norm for t in TERMOS_NE_MMH_NUTRI)
    if tem_mmh_nutri:
        return uf in UFS_PERMITIDAS_MMH

    return False

def processar_licitacao_limpeza(licitacao):
    if not licitacao:
        return None

    uf = licitacao.get('uf', '').upper()
    obj_bruto = licitacao.get('obj', '')
    obj_norm = normalize(obj_bruto)
    itens = licitacao.get('itens', [])

    # Validação de Pertinência
    if not analisar_pertinencia(obj_norm, uf, itens):
        return None

    # Validação de Data
    dt_enc_str = licitacao.get('dt_enc')
    if not dt_enc_str:
        return None

    try:
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_2026:
            return None
    except:
        return None

    # Chave única para deduplicação
    chave_unica = f"{licitacao.get('id', '')[:14]}_{licitacao.get('edit', '')}"
    qtd_itens = len(itens)

    return (chave_unica, licitacao, dt_enc, qtd_itens)

if __name__ == '__main__':
    if not os.path.exists(ARQDADOS):
        print(f"Arquivo {ARQDADOS} não encontrado. Execute o app.py primeiro.")
        sys.exit(1)

    print("🧹 Iniciando limpeza de dados...")

    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f:
        banco_bruto = json.load(f)

    print(f"📊 Total no banco bruto: {len(banco_bruto)} licitações")

    banco_deduplicado = {}

    # Processamento paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        resultados = executor.map(processar_licitacao_limpeza, banco_bruto)

    for res in resultados:
        if res is None:
            continue

        chave, card, dt_novo, qtd_itens_novo = res

        if chave not in banco_deduplicado:
            banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
        else:
            # Mantém a versão com mais itens, ou data mais recente se igual
            qtd_itens_antigo = banco_deduplicado[chave]['qtd']
            if qtd_itens_novo > qtd_itens_antigo:
                banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
            elif qtd_itens_novo == qtd_itens_antigo:
                if dt_novo > banco_deduplicado[chave]['dt']:
                    banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}

    # Gera lista final
    lista_final = [item['card'] for item in banco_deduplicado.values()]

    print(f"💾 Salvando {len(lista_final)} licitações limpas...")

    with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False)

    print(f"✅ Concluído! {len(lista_final)} licitações validadas e prontas para o Dashboard.")
    print(f"   📉 Rejeitadas: {len(banco_bruto) - len(lista_final)} licitações")
