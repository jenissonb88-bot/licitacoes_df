import requests
import json
import os
import gzip
import pandas as pd
import unicodedata
import concurrent.futures
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# CONFIGURAÇÕES GERAIS
# ==========================================
ARQ_DADOS = 'dados/oportunidades.json.gz'  # Arquivo comprimido
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 10  # Equilíbrio entre velocidade e segurança

# Data de corte para ENCERRAMENTO das propostas
# O robô ignorará licitações que fecharam antes desta data
DATA_CORTE_ENCERRAMENTO = datetime(2026, 1, 1)

# ==========================================
# REGRAS DE NEGÓCIO E FILTROS
# ==========================================

# 1. REGRA DO NORDESTE (Dieta/Nutrição)
# Estas palavras só serão aceitas se a UF da licitação estiver nesta lista.
UFS_NORDESTE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
KEYWORDS_NORDESTE = [
    "DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", 
    "CALORIC", "PROTEIC", "LEITE", "NUTRI"
]

# 2. BLACKLIST (Lista Negativa)
# Termos que, se encontrados, descartam o item imediatamente.
BLACKLIST = [
    # Construção, Obras e Predial
    "CONSTRUCAO", "OBRA", "PAVIMENTACAO", "CIMENTO", "ASFALTO", "TIJOLO",
    "PINTURA", "TINTA", "MARCENARIA", "MADEIRA", "FERRAGEM", "FERRAMENTA",
    "HIDRAULIC", "ELETRIC", "MANUTENCAO PREDIAL", "ALVENARIA", "VIDRO",
    "ILUMINACAO", "LAMPADA", "AR CONDICIONADO", "CLIMATIZACAO", "PISCINA",
    
    # Veículos, Transportes e Mecânica
    "AUTOMOTIVO", "VEICULO", "PNEU", "RODOVIARIO", "MECANICA", "PECA", 
    "RODA", "MOTOR", "COMBUSTIVEL", "OLEO LUBRIFICANTE", "OFICINA", 
    "PASSAGEM", "LOCACAO DE VEICULO", "TRANSPORTE", "AERONAVE",
    
    # Alimentação (Exceto Dieta Enteral tratada na regra NE)
    "REFEICAO", "LANCHE", "ALIMENTICIO", "MERENDA", "COZINHA", "COPA", 
    "BUFFET", "COFFEE", "AÇUCAR", "CAFE", "CESTAS BASICAS", "HORTIFRUTI",
    "PERECIVEIS", "AGUA MINERAL", "GENERO ALIMENTICIO",
    
    # Escritório, Escola e Papelaria
    "ESCOLAR", "DIDATICO", "PEDAGOGICO", "EXPEDIENTE", "PAPELARIA", 
    "LIVRO", "APOSTILA", "BRINQUEDO", "JOGOS",
    "COMPUTADOR", "IMPRESSORA", "TONER", "CARTUCHO", "INFORMATICA", 
    "NOTEBOOK", "TECLADO", "MOUSE", "ESTABILIZADOR", "NOBREAK", "SOFTWARE", "SAAS",
    "LINK DE DADOS", "TELEFONIA", "INTERNET",
    
    # Mobiliário e Eletro
    "MOBILIARIO", "ESTANTE", "CADEIRA", "MESA", "ARMARIO", "ELETRODOMESTICO", 
    "ELETROPORTATIL", "GELADEIRA", "FOGAO", "VENTILADOR", "CAMA MESA",
    
    # Limpeza e Higiene Predial (Cuidado para não remover higiene pessoal)
    "LIMPEZA PREDIAL", "HIGIENIZACAO", "VASSOURA", "RODO", "LIXEIRA", 
    "SACO DE LIXO", "DETERGENTE", "SABAO EM PO", "COPO DESCARTAVEL",
    
    # Serviços e Pessoas
    "TERCEIRIZACAO", "LOCACAO DE MAO DE OBRA", "ASSISTENCIA MEDICA", 
    "PLANO DE SAUDE", "ODONTOLOGICA", "SEGURO", "VIGILANCIA", "PORTARIA", 
    "RECEPCIONISTA", "CONSULTORIA", "TREINAMENTO", "EVENTO", "SHOW", 
    "FESTA", "PALCO", "HOSPEDAGEM", "PUBLICIDADE", "MARKETING", "GRAFICA", "BANNER",
    
    # Outros não pertinentes
    "VETERINARI", "ANIMAL", "BANHO E TOSA", "RAÇÃO", "AGRO", "AGRICOLA", 
    "SEMENTE", "MUDA", "ADUBO", "JARDINAGEM", "ROÇADEIRA",
    "BELICO", "MILITAR", "ARMAMENTO", "MUNICAO", "FARDA", "UNIFORME", 
    "TECIDO", "CONFECÇÃO", "VESTUARIO", 
    "ESPORTE", "MATERIAL ESPORTIVO", "BOLA", "TROFEU", "MEDALHA", 
    "MUSICAL", "INSTRUMENTO", "AUDIOVISUAL", "FOTOGRAFI", "BRINDE"
]

# ==========================================
# FUNÇÕES AUXILIARES
# ==========================================

def normalizar(texto):
    """Remove acentos, caracteres especiais e converte para maiúsculas."""
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_keywords_csv():
    """Lê o arquivo CSV e extrai a coluna 'Fármaco'."""
    if not os.path.exists(ARQ_CSV):
        print(f"⚠️ AVISO: Arquivo {ARQ_CSV} não encontrado. O robô usará apenas regras do NE.")
        return []

    try:
        try:
            df = pd.read_csv(ARQ_CSV, encoding='utf-8')
        except:
            df = pd.read_csv(ARQ_CSV, encoding='latin1')

        if 'Fármaco' not in df.columns:
            print("❌ ERRO CRÍTICO: Coluna 'Fármaco' não encontrada no CSV.")
            return []
        
        raw_keywords = df['Fármaco'].dropna().unique().tolist()
        keywords = [normalizar(k) for k in raw_keywords if len(str(k)) > 2]
        
        print(f"✅ CSV Carregado: {len(keywords)} fármacos importados para inteligência de busca.")
        return keywords
    except Exception as e:
        print(f"⚠️ Erro ao processar CSV: {e}")
        return []

def validar_item(descricao, uf):
    """
    O CÉREBRO DA TRIAGEM:
    1. Verifica Blacklist (Rejeição Imediata)
    2. Verifica Regra Regional (Nordeste)
    3. Verifica Banco de Dados de Fármacos (CSV)
    """
    desc_norm = normalizar(descricao)
    
    # 1. VERIFICAÇÃO DE BLACKLIST (Exclusão)
    for bad in BLACKLIST:
        if bad in desc_norm:
            return False

    # 2. VERIFICAÇÃO REGIONAL (Regra Nordeste)
    for k in KEYWORDS_NORDESTE:
        if k in desc_norm:
            if uf in UFS_NORDESTE:
                return True
            else:
                return False 

    # 3. VERIFICAÇÃO DE FÁRMACOS (Inclusão via CSV)
    for k in KEYWORDS_GLOBAL:
        if k in desc_norm:
            return True

    return False

def criar_sessao():
    """Cria uma sessão HTTP resiliente com retries."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

def processar_licitacao(licitacao, session):
    try:
        # --- FILTRO 1: DATA DE ENCERRAMENTO ---
        data_enc_str = licitacao.get('data_encerramento_proposta')
        if not data_enc_str: return None
        
        try:
            data_enc = datetime.fromisoformat(data_enc_str)
        except ValueError:
            return None

        # REGRA: Só aceita licitações que encerram a partir de 2026
        if data_enc < DATA_CORTE_ENCERRAMENTO:
            return None 

        # --- FILTRO 2: BUSCA DE ITENS ---
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{licitacao['orgao_cnpj']}/compras/{licitacao['ano_compra']}/{licitacao['sequencial_compra']}/itens"
        
        r = session.get(url_itens, timeout=15)
        if r.status_code != 200: return None
        
        itens_raw = r.json()
        itens_validos = []
        uf_licitacao = licitacao.get('unidade_orgao', {}).get('uf_sigla', 'XX')

        for it in itens_raw:
            desc = it.get('descricao', '')
            
            # --- FILTRO 3: VALIDAÇÃO DO ITEM ---
            if validar_item(desc, uf_licitacao):
                val_est = it.get('valor_unitario_estimado', 0.0) or 0.0
                qtd = it.get('quantidade', 0) or 0

                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": desc,
                    "qtd": qtd,
                    "unitario_est": float(val_est),
                    "total_est": float(val_est) * float(qtd),
                    "situacao": it.get('situacao_compra_item_nome', 'Desconhecido')
                })
        
        if not itens_validos: return None

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
        return None

# ==========================================
# FLUXO PRINCIPAL
# ==========================================
if __name__ == "__main__":
    print("🚀 INICIANDO SNIPER PNCP - HEALTHCARE EDITION")
    print(f"📅 Filtro de Encerramento: A partir de {DATA_CORTE_ENCERRAMENTO.strftime('%d/%m/%Y')}")

    KEYWORDS_GLOBAL = carregar_keywords_csv()
    if not KEYWORDS_GLOBAL:
        print("⚠️ ATENÇÃO: Operando apenas com Regras do Nordeste (Sem CSV).")

    # Data de início da varredura (Publicação)
    start_date = datetime(2025, 12, 1) 
    
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            content = f.read().strip()
            if content:
                try:
                    start_date = datetime.strptime(content, '%Y%m%d')
                    print(f"🔄 Retomando varredura a partir de: {start_date.strftime('%d/%m/%Y')}")
                except:
                    print("⚠️ Checkpoint inválido, iniciando do zero.")

    today = datetime.now()
    delta = today - start_date
    session = criar_sessao()
    
    banco_dados = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                lista_antiga = json.load(f)
                for item in lista_antiga:
                    banco_dados[item['id']] = item
            print(f"📚 Base de dados carregada: {len(banco_dados)} processos já capturados.")
        except Exception:
            banco_dados = {}

    novos_count = 0

    for i in range(delta.days + 1):
        data_atual = start_date + timedelta(days=i)
        data_str = data_atual.strftime('%Y%m%d')
        print(f"\n🔍 Analisando Publicações de: {data_atual.strftime('%d/%m/%Y')}...")

        pagina = 1
        while True:
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={data_str}&data_final={data_str}&modalidade_contratacao_id=6&pagina={pagina}&tamanho_pagina=50"
            
            try:
                r = session.get(url, timeout=20)
                if r.status_code != 200: break
                
                resp_json = r.json()
                total_paginas = resp_json.get('total_paginas', 0)
                licitacoes = resp_json.get('data', [])
                
                if not licitacoes: break

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futuros = {executor.submit(processar_licitacao, lic, session): lic for lic in licitacoes}
                    
                    for futuro in concurrent.futures.as_completed(futuros):
                        res = futuro.result()
                        if res:
                            banco_dados[res['id']] = res
                            novos_count += 1
                            print(".", end="", flush=True)
                
                print(f" [Pág {pagina}/{total_paginas}]", end="\r")
                if pagina >= total_paginas: break
                pagina += 1

            except Exception as e:
                print(f"❌ Erro na página {pagina}: {e}")
                break

    prox_dia = today.strftime('%Y%m%d')
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(prox_dia)

    os.makedirs('dados', exist_ok=True)
    lista_final = list(banco_dados.values())
    lista_final.sort(key=lambda x: x.get('data_encerramento', ''), reverse=True)

    print(f"\n\n💾 Salvando Base de Dados Compactada...")
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"✅ CONCLUÍDO! Total: {len(lista_final)} | Novos: {novos_count}")
