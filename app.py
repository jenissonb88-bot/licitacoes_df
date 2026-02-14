# ... (imports permanecem os mesmos)

# === CONFIGURAÇÕES ===
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 10 

# Data de corte para ENCERRAMENTO (Propostas até...)
DATA_CORTE_ENCERRAMENTO = datetime(2026, 1, 1)

# === LISTAS DE FILTRAGEM INTELIGENTE ===

# 1. REGRA DO NORDESTE (Dieta/Nutrição)
# Palavras que só serão aceitas se a UF for do Nordeste.
# Usamos raízes ("SUPLEMENT") para pegar variações ("SUPLEMENTO", "SUPLEMENTOS").
UFS_NORDESTE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
KEYWORDS_NORDESTE = [
    "DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", 
    "CALORIC", "PROTEIC", "LEITE", "NUTRI"
]

# 2. BLACKLIST (O que excluir imediatamente)
# Nota: "TI" foi removido para não bloquear "AN-TI-BIOTICO". 
# Usamos raízes (ex: "HIDRAULIC") para pegar masc/fem e singular/plural.
BLACKLIST = [
    # Construção e Manutenção Predial
    "CONSTRUCAO", "OBRA", "PAVIMENTACAO", "CIMENTO", "ASFALTO", "TIJOLO",
    "PINTURA", "TINTA", "MARCENARIA", "MADEIRA", "FERRAGEM", "FERRAMENTA",
    "HIDRAULIC", "ELETRIC", "MANUTENCAO PREDIAL", "ALVENARIA", "VIDRO",
    "ILUMINACAO", "LAMPADA", "AR CONDICIONADO", "CLIMATIZACAO",
    
    # Veículos e Transportes
    "AUTOMOTIVO", "VEICULO", "PNEU", "RODOVIARIO", "MECANICA", "PECA", 
    "RODA", "MOTOR", "COMBUSTIVEL", "OLEO LUBRIFICANTE", "OFICINA", 
    "PASSAGEM", "LOCACAO DE VEICULO", "TRANSPORTE",
    
    # Alimentação (Exceto Dieta Enteral tratada na regra NE)
    "REFEICAO", "LANCHE", "ALIMENTICIO", "MERENDA", "COZINHA", "COPA", 
    "BUFFET", "COFFEE", "AÇUCAR", "CAFE", "CESTAS BASICAS", "HORTIFRUTI",
    "PERECIVEIS", "AGUA MINERAL",
    
    # Escritório, Escola e Papelaria
    "ESCOLAR", "DIDATICO", "PEDAGOGICO", "EXPEDIENTE", "PAPELARIA", 
    "LIVRO", "APOSTILA", "BRINQUEDO", "JOGOS",
    "COMPUTADOR", "IMPRESSORA", "TONER", "CARTUCHO", "INFORMATICA", 
    "NOTEBOOK", "TECLADO", "MOUSE", "ESTABILIZADOR", "NOBREAK", "SOFTWARE", "SAAS",
    
    # Mobiliário e Eletro
    "MOBILIARIO", "ESTANTE", "CADEIRA", "MESA", "ARMARIO", "ELETRODOMESTICO", 
    "ELETROPORTATIL", "GELADEIRA", "FOGAO", "VENTILADOR",
    
    # Limpeza e Higiene (Cuidado com itens hospitalares aqui)
    "LIMPEZA PREDIAL", "HIGIENIZACAO", "VASSOURA", "RODO", "LIXEIRA", 
    "SACO DE LIXO", "DETERGENTE", "SABAO", "COPO DESCARTAVEL",
    
    # Serviços e Pessoas
    "TERCEIRIZACAO", "LOCACAO DE MAO DE OBRA", "ASSISTENCIA MEDICA", 
    "PLANO DE SAUDE", "SEGURO", "VIGILANCIA", "PORTARIA", "RECEPCIONISTA",
    "CONSULTORIA", "TREINAMENTO", "EVENTO", "SHOW", "FESTA", "PALCO",
    "HOSPEDAGEM", "PUBLICIDADE", "MARKETING", "GRAFICA", "BANNER",
    
    # Outros não pertinentes
    "VETERINARI", "ANIMAL", "BANHO E TOSA", "RAÇÃO", "AGRO", "AGRICOLA", 
    "SEMENTE", "MUDA", "ADUBO", "JARDINAGEM",
    "BELICO", "MILITAR", "ARMAMENTO", "MUNICAO", "FARDA", "UNIFORME", 
    "TECIDO", "CONFECÇÃO", "VESTUARIO", "CAMA MESA E BANHO",
    "ESPORTE", "MATERIAL ESPORTIVO", "BOLA", "TROFEU", "MEDALHA", 
    "MUSICAL", "INSTRUMENTO", "AUDIOVISUAL", "FOTOGRAFI", "BRINDE"
]

# (O restante das funções normalizar, carregar_keywords_csv, etc. continua igual)

def validar_item(descricao, uf):
    """
    Retorna True se o item for pertinente.
    Aplica lógica de Blacklist, CSV e Regra Nordeste.
    """
    desc_norm = normalizar(descricao)
    
    # 1. Verifica Blacklist (Se tiver termo proibido, rejeita imediatamente)
    for bad in BLACKLIST:
        if bad in desc_norm:
            # Exceção de segurança: Se cair na Blacklist, mas for um item muito específico
            # do CSV (ex: "ACIDO PARA LIMPEZA DE PELE" vs "MATERIAL DE LIMPEZA"), 
            # a Blacklist tem prioridade para limpar o lixo.
            return False

    # 2. Verifica Regra Nordeste (Dieta/Leite/Nutrição)
    for k in KEYWORDS_NORDESTE:
        if k in desc_norm:
            if uf in UFS_NORDESTE:
                return True # É Dieta no Nordeste -> Aprova
            else:
                # Se tem palavra de dieta mas NÃO é nordeste, a gente rejeita aqui?
                # DEPENDE. Se você vende dieta para SP, remova o 'else: return False'.
                # Se você SÓ vende dieta para o Nordeste, mantenha assim.
                return False 

    # 3. Verifica Keywords do CSV (Fármacos)
    for k in KEYWORDS_GLOBAL:
        # Adiciona verificação de fronteira para evitar falsos positivos curtos
        # Ex: "AAS" não pode pegar "SAAS" (Software)
        if k in desc_norm:
            return True

    return False

# ... (Restante do código main)
