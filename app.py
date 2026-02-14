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
# CONFIGURAÇÕES
# ==========================================
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
ARQ_EXCLUIDOS = 'excluidos.txt'
MAX_WORKERS = 10 

# Data base para o início da varredura (Divulgação)
DATA_INICIO_DIVULGACAO = datetime(2026, 1, 1)

# Simulação do Hoje (Ajuste conforme sua necessidade)
HOJE = datetime(2026, 2, 14) 

# ==========================================
# REGRAS DE FILTRAGEM (Mantidas conforme sua última lista)
# ==========================================
UFS_NORDESTE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
KEYWORDS_NORDESTE = ["DIETA", "ENTERAL", "SUPLEMENT", "FORMULA", "CALORIC", "PROTEIC", "LEITE", "NUTRI"]

BLACKLIST = [
    "CONSTRUCAO", "OBRA", "PAVIMENTACAO", "CIMENTO", "ASFALTO", "TIJOLO", "PINTURA", "TINTA", 
    "MARCENARIA", "MADEIRA", "FERRAGEM", "FERRAMENTA", "HIDRAULIC", "ELETRIC", "MANUTENCAO PREDIAL", 
    "ALVENARIA", "VIDRO", "ILUMINACAO", "LAMPADA", "AR CONDICIONADO", "CLIMATIZACAO", "PISCINA",
    "AUTOMOTIVO", "VEICULO", "PNEU", "RODOVIARIO", "MECANICA", "PECA", "RODA", "MOTOR", "COMBUSTIVEL", 
    "OLEO LUBRIFICANTE", "OFICINA", "PASSAGEM", "LOCACAO DE VEICULO", "TRANSPORTE", "AERONAVE",
    "REFEICAO", "LANCHE", "ALIMENTICIO", "MERENDA", "COZINHA", "COPA", "BUFFET", "COFFEE", "AÇUCAR", 
    "CAFE", "CESTAS BASICAS", "HORTIFRUTI", "PERECIVEIS", "AGUA MINERAL", "GENERO ALIMENTICIO",
    "ESCOLAR", "DIDATICO", "PEDAGOGICO", "EXPEDIENTE", "PAPELARIA", "LIVRO", "APOSTILA", "BRINQUEDO", 
    "JOGOS", "COMPUTADOR", "IMPRESSORA", "TONER", "CARTUCHO", "INFORMATICA", "NOTEBOOK", "TECLADO", 
    "MOUSE", "ESTABILIZADOR", "NOBREAK", "SOFTWARE", "SAAS", "LINK DE DADOS", "TELEFONIA", "INTERNET",
    "MOBILIARIO", "ESTANTE", "CADEIRA", "MESA", "ARMARIO", "ELETRODOMESTICO", "ELETROPORTATIL", 
    "GELADEIRA", "FOGAO", "VENTILADOR", "CAMA MESA", "LIMPEZA PREDIAL", "HIGIENIZACAO", "VASSOURA", 
    "RODO", "LIXEIRA", "SACO DE LIXO", "DETERGENTE", "SABAO EM PO", "COPO DESCARTAVEL",
    "TERCEIRIZACAO", "LOCACAO DE MAO DE OBRA", "ASSISTENCIA MEDICA", "PLANO DE SAUDE", "ODONTOLOGICA", 
    "SEGURO", "VIGILANCIA", "PORTARIA", "RECEPCIONISTA", "CONSULTORIA", "TREINAMENTO", "EVENTO", 
    "SHOW", "FESTA", "PALCO", "HOSPEDAGEM", "PUBLICIDADE", "MARKETING", "GRAFICA", "BANNER",
    "VETERINARI", "ANIMAL", "BANHO E TOSA", "RAÇÃO", "AGRO", "AGRICOLA", "SEMENTE", "MUDA", "ADUBO", 
    "JARDINAGEM", "ROÇADEIRA", "BELICO", "MILITAR", "ARMAMENTO", "MUNICAO", "FARDA", "UNIFORME", 
    "TECIDO", "CONFECÇÃO", "VESTUARIO", "ESPORTE", "MATERIAL ESPORTIVO", "BOLA", "TROFEU", "MEDALHA", 
    "MUSICAL", "INSTRUMENTO", "AUDIOVISUAL", "FOTOGRAFI", "BRINDE"
]

def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_keywords_csv():
    if not os.path.exists(ARQ_CSV): return []
    try:
        try: df = pd.read_csv(ARQ_CSV, encoding='utf-8')
        except: df = pd.read_csv(ARQ_CSV, encoding='latin1')
        raw = df['Fármaco'].dropna().unique().tolist()
        return [normalizar(k) for k in raw if len(str(k)) > 2]
    except: return []

def carregar_ids_excluidos():
    ids = set()
    if os.path.exists(ARQ_EXCLUIDOS):
        with open(ARQ_EXCLUIDOS, 'r') as f:
            for linha in f:
                if linha.strip(): ids.add(linha.strip())
    return ids

def validar_item(descricao, uf):
    desc_norm = normalizar(descricao)
    for bad in BLACKLIST:
        if bad in desc_norm: return False
    for k in KEYWORDS_NORDESTE:
        if k in desc_norm: return uf in UFS_NORDESTE
    for k in KEYWORDS_GLOBAL:
        if k in desc_norm: return True
    return False

def processar_licitacao(lic, session, ids_banidos):
    lic_id = f"{lic['orgao_cnpj']}{lic['ano_compra']}{lic['sequencial_compra']}"
    if lic_id in ids_banidos: return None

    # Agora não filtramos mais a data de encerramento agressivamente aqui, 
    # pois o filtro principal é na data de DIVULGAÇÃO (na chamada da API)
    
    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{lic['orgao_cnpj']}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
    try:
        r = session.get(url_itens, timeout=15)
        if r.status_code != 200: return None
        
        itens_validos = []
        uf = lic.get('unidade_orgao', {}).get('uf_sigla', 'XX')
        for it in r.json():
            if validar_item(it.get('descricao', ''), uf):
                val = it.get('valor_unitario_estimado') or 0.0
                qtd = it.get('quantidade') or 0
                itens_validos.append({
                    "item": it.get('numero_item'),
                    "desc": it.get('descricao'),
                    "qtd": qtd,
                    "unitario_est": float(val),
                    "total_est": float(val) * float(qtd),
                    "situacao": it.get('situacao_compra_item_nome', 'Desconhecido')
                })
        
        if not itens_validos: return None

        return {
            "id": lic_id,
            "data_pub": lic.get('data_publicacao_pncp', ''),
            "data_encerramento": lic.get('data_encerramento_proposta', ''),
            "uf": uf,
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome', ''),
            "orgao": lic.get('orgao_nome_fantasia', '') or lic.get('orgao_razao_social', ''),
            "objeto": lic.get('objeto_compra', ''),
            "link": f"https://pncp.gov.br/app/editais/{lic['orgao_cnpj']}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_validos
        }
    except: return None

if __name__ == "__main__":
    KEYWORDS_GLOBAL = carregar_keywords_csv()
    IDS_EXCLUIDOS = carregar_ids_excluidos()

    data_alvo = DATA_INICIO_DIVULGACAO
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass

    session = requests.Session()
    session.headers.update({"User-Agent": "SniperBot/1.0"})
    
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                lista = json.load(f)
                banco = {i['id']: i for i in lista if i['id'] not in IDS_EXCLUIDOS}
        except: pass

    # --- LÓGICA DE BACKLOG ATÔMICO ---
    dias_atraso = (HOJE - data_alvo).days
    trigger_next = "false"

    if dias_atraso >= 0:
        # Se houver atraso ou for o dia de hoje, processa o dia do checkpoint
        print(f"📂 Varrendo Divulgações de: {data_alvo.strftime('%d/%m/%Y')}")
        d_str = data_alvo.strftime('%Y%m%d')
        
        pag = 1
        novos = 0
        while True:
            # Filtro por Data Inicial/Final de DIVULGAÇÃO (conforme padrão API)
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
            r = session.get(url, timeout=20)
            if r.status_code != 200: break
            resp = r.json()
            data = resp.get('data', [])
            if not data: break

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = {exe.submit(processar_licitacao, l, session, IDS_EXCLUIDOS): l for l in data}
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res:
                        banco[res['id']] = res
                        novos += 1
                        print(".", end="", flush=True)

            if pag >= resp.get('total_paginas', 0): break
            pag += 1
        
        # Avança o checkpoint
        proximo_checkpoint = data_alvo + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo_checkpoint.strftime('%Y%m%d'))
        
        # Se ainda estiver no passado, ativa o gatilho para o próximo job
        if (HOJE - proximo_checkpoint).days >= 0:
            trigger_next = "true"

    # Salva o banco
    os.makedirs('dados', exist_ok=True)
    lista_final = sorted(list(banco.values()), key=lambda x: x.get('data_pub', ''), reverse=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            print(f"trigger_next={trigger_next}", file=f)
