import json, gzip, os, unicodedata, csv
from datetime import datetime
import concurrent.futures

ARQDADOS, ARQLIMPO, ARQ_CATALOGO = 'dadosoportunidades.json.gz', 'pregacoes_pharma_limpos.json.gz', 'Exportar Dados.csv'
DATA_CORTE_2026 = datetime(2026, 1, 1)

# --- GEOGRAFIA E MAPAS ---
NE_ESTADOS = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
ESTADOS_BLOQUEADOS = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
# Roteamento estrito para MMH e Dietas (inclui tolerância API/Órgãos Federais no DF)
UFS_PERMITIDAS_MMH = NE_ESTADOS + ['DF', ''] 

# NOVOS VETOS INCLUÍDOS AQUI:
VETOS_IMEDIATOS = [
    "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO", "ASFALTICO", "ASFALTO", 
    "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS", "MANUTENCAO PREVENTIVA", "MANUTENCAO CORRETIVA",
    "UNIFORME", "TEXTIL", "REFORMA", "GASES MEDICINAIS", 
    "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA", "IMPRESSAO", "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "REFORMA", "MANUTENCAO PREDIAL", 
    "MANUTENCAO DE EQUIPAMENTOS", "LIMPEZA URBANA", "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL", "DIESEL", "GASOLINA", 
    "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA", "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO", "VESTUARIO", 
    "INFORMATICA", "COMPUTADORES", "IMPRESSAO", "EVENTOS", "REPARO", "CORRETIVA", "GERADOR", "CORRETIVA", "VEICULO", "AMBULANCIA", "MOTOCICLETA",
    "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO"
]

TERMOS_NE_MMH_NUTRI = [
    "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA", "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO", "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO", "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE", 
    "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS", "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL", "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO", "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO", "TOUCA DESCARTAVEL", 
    "TUBO ASPIRACAO", "NUTRICAO ENTERAL", "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL", "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL", "FORMULA ESPECIA", "AGULHAS", "SERINGAS", "CURATIVOS" 
]

TERMOS_SALVAMENTO = [
    "MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA", "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO", "AQUISICAO DE MEDICAMENTO", "SORO"
] # Lista reduzida para fins de performance, os vetos principais já operam acima.

def normalize(t):
    if not t: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(t)).upper() if unicodedata.category(c) != 'Mn')

def analisar_pertinencia(obj_norm, uf):
    # 🌟 SUPER PASSE NACIONAL
    palavras_magicas = ["MEDICAMENTO", "MEDICAMENTOS", "AQUISICAO DE MEDICAMENTOS"]
    tem_super_passe = any(p in obj_norm for p in palavras_magicas)
    
    if tem_super_passe:
        return True # 🟢 Libera para qualquer UF e ignora vetos!

    # 1. Barreira Geográfica Global (se não tiver super passe)
    if uf and uf in ESTADOS_BLOQUEADOS:
        return False

    # 2. Aplicação de Vetos de Palavras
    for veto in VETOS_IMEDIATOS:
        if veto in obj_norm: return False
        
    # 3. Restrição MMH/Nutrição ao Nordeste e DF
    tem_mmh_nutri = any(t in obj_norm for t in TERMOS_NE_MMH_NUTRI)
    if tem_mmh_nutri and (uf not in UFS_PERMITIDAS_MMH):
        return False

    return True

def processar_licitacao_limpeza(licitacao):
    if not licitacao: return None
    
    uf = licitacao.get('uf', '').upper()
    obj_bruto = licitacao.get('obj', '')
    obj_norm = normalize(obj_bruto)
    
    # Validação de Pertinência
    if not analisar_pertinencia(obj_norm, uf):
        return None

    dt_enc_str = licitacao.get('dt_enc')
    if not dt_enc_str: return None
    
    try:
        dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
        if dt_enc < DATA_CORTE_2026: return None
    except:
        return None

    chave_unica = f"{licitacao.get('id', '')[:14]}_{licitacao.get('edit', '')}"
    qtd_itens = len(licitacao.get('itens', []))
    
    return (chave_unica, licitacao, dt_enc, qtd_itens)

if __name__ == '__main__':
    if not os.path.exists(ARQDADOS):
        print(f"Arquivo {ARQDADOS} não encontrado. Execute o app.py primeiro.")
        sys.exit(1)

    with gzip.open(ARQDADOS, 'rt', encoding='utf-8') as f: 
        banco_bruto = json.load(f)

    banco_deduplicado = {}

    print(f"Processando {len(banco_bruto)} licitações com processamento paralelo...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        resultados = executor.map(processar_licitacao_limpeza, banco_bruto)

    for res in resultados:
        if res is None: continue
        chave, card, dt_novo, qtd_itens_novo = res
        
        if chave not in banco_deduplicado:
            banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
        else:
            qtd_itens_antigo = banco_deduplicado[chave]['qtd']
            if qtd_itens_novo > qtd_itens_antigo:
                banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
            elif qtd_itens_novo == qtd_itens_antigo:
                if dt_novo > banco_deduplicado[chave]['dt']:
                    banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}

    print("Salvando base limpa e consolidada...")
    lista_final = [item['card'] for item in banco_deduplicado.values()]
    
    with gzip.open(ARQLIMPO, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False)
        
    print(f"✅ Concluído! {len(lista_final)} licitações limpas, validadas e prontas para o Dashboard.")
