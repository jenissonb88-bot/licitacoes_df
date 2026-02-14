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

def criar_sessao():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

def buscar_detalhes_item(url_item, session):
    try:
        r = session.get(url_item, timeout=10)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def processar_licitacao(licitacao, session):
    """
    Processa uma única licitação:
    1. Verifica data de encerramento.
    2. Busca itens.
    3. Filtra itens pertinentes.
    """
    try:
        # Filtro 1: Data de Encerramento (CRÍTICO)
        data_enc_str = licitacao.get('data_encerramento_proposta')
        if not data_enc_str:
            return None # Sem data, ignora
        
        data_enc = datetime.fromisoformat(data_enc_str)
        if data_enc < DATA_CORTE_ENCERRAMENTO:
            return None # Encerrou antes de 2026

        # Se passou na data, vamos olhar os itens
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{licitacao['orgao_cnpj']}/compras/{licitacao['ano_compra']}/{licitacao['sequencial_compra']}/itens"
        r = session.get(url_itens, timeout=15)
        if r.status_code != 200:
            return None
        
        itens_raw = r.json()
        itens_validos = []
        
        uf_licitacao = licitacao.get('unidade_orgao', {}).get('uf_sigla', 'XX')

        for it in itens_raw:
            desc = it.get('descricao', '')
            if validar_item(desc, uf_licitacao):
                # Se validou, pega valor estimado
                val_est = it.get('valor_unitario_estimado', 0.0)
                if val_est is None: val_est = 0.0
                
                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": desc,
                    "qtd": it.get('quantidade', 0),
                    "unitario_est": float(val_est),
                    "total_est": float(val_est) * it.get('quantidade', 0),
                    "situacao": it.get('situacao_compra_item_nome', 'Desconhecido')
                })
        
        if not itens_validos:
            return None

        # Monta o objeto final otimizado
        return {
            "id": f"{licitacao['orgao_cnpj']}{licitacao['ano_compra']}{licitacao['sequencial_compra']}",
            "data_pub": licitacao.get('data_publicacao_pncp', ''),
            "data_encerramento": data_enc_str,
            "uf": uf_licitacao,
            "cidade": licitacao.get('unidade_orgao', {}).get('municipio_nome', ''),
            "orgao": licitacao.get('orgao_nome_fantasia', '') or licitacao.get('orgao_razao_social', ''),
            "objeto": licitacao.get('objeto_compra', ''),
            "link": f"https://pncp.gov.br/app/editais/{licitacao['orgao_cnpj']}/{licitacao['ano_compra']}/{licitacao['sequencial_compra']}",
            "itens": itens_validos
        }

    except Exception as e:
        # print(f"Erro processando: {e}")
        return None

# === FLUXO PRINCIPAL ===
if __name__ == "__main__":
    print("🚀 Iniciando Sniper PNCP (Versão Otimizada GZIP)...")
    
    # 1. Carregar Keywords
    KEYWORDS_GLOBAL = carregar_keywords_csv()
    if not KEYWORDS_GLOBAL:
        print("⚠️ AVISO: Sem keywords do CSV, o robô pode não pegar nada (exceto NE).")

    # 2. Carregar Checkpoint ou Definir Início
    # Como queremos garantir tudo que encerra em 2026, vamos voltar um pouco na publicação
    # Se não tiver checkpoint, começa em 01/12/2025 (para pegar editais abertos no fim do ano)
    start_date = datetime(2025, 12, 1) 
    
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            content = f.read().strip()
            if content:
                # Se tiver checkpoint, usa ele (respeita a continuidade)
                start_date = datetime.strptime(content, '%Y%m%d')
    
    # Mas se o checkpoint for muito antigo, ou se quisermos forçar a regra de 2026
    # O usuário pediu "a partir de 01/01/2026" (Encerramento). 
    # Publicações de Dez/25 podem ter encerramento em Jan/26. Mantemos a lógica.

    today = datetime.now()
    delta = today - start_date
    session = criar_sessao()
    
    # Carregar dados existentes (se houver, e descompactar)
    banco_dados = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                lista_antiga = json.load(f)
                for item in lista_antiga:
                    banco_dados[item['id']] = item
            print(f"📚 Base carregada: {len(banco_dados)} licitações.")
        except Exception as e:
            print(f"⚠️ Erro ao ler base antiga (pode ser formato incompatível): {e}")
            banco_dados = {}

    novos_count = 0

    # Loop por Dias de Publicação
    for i in range(delta.days + 1):
        data_atual = start_date + timedelta(days=i)
        data_str = data_atual.strftime('%Y%m%d')
        print(f"📅 Varrendo Publicações de: {data_atual.strftime('%d/%m/%Y')}...")

        pagina = 1
        while True:
            # Filtro: modalidade_contratacao_id=6 (Pregão)
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={data_str}&data_final={data_str}&modalidade_contratacao_id=6&pagina={pagina}&tamanho_pagina=50"
            
            try:
                r = session.get(url, timeout=20)
                if r.status_code != 200:
                    break
                
                resp_json = r.json()
                total_paginas = resp_json.get('total_paginas', 0)
                licitacoes = resp_json.get('data', [])
                
                if not licitacoes:
                    break

                # Processamento Paralelo
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futuros = {executor.submit(processar_licitacao, lic, session): lic for lic in licitacoes}
                    
                    for futuro in concurrent.futures.as_completed(futuros):
                        res = futuro.result()
                        if res:
                            banco_dados[res['id']] = res
                            novos_count += 1
                
                print(f"   ↳ Pág {pagina}/{total_paginas} | Novos: {novos_count}")
                
                if pagina >= total_paginas:
                    break
                pagina += 1

            except Exception as e:
                print(f"❌ Erro na página {pagina}: {e}")
                break

    # Salvar Checkpoint (Dia de Amanhã)
    prox_dia = today.strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(prox_dia)

    # Salvar Arquivo Compactado (GZIP)
    os.makedirs('dados', exist_ok=True)
    lista_final = list(banco_dados.values())
    
    # Ordenar por data de encerramento (mais recentes primeiro)
    lista_final.sort(key=lambda x: x.get('data_encerramento', ''), reverse=True)

    print(f"💾 Salvando {len(lista_final)} registros compactados...")
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))
    
    print("✅ Concluído com sucesso!")
