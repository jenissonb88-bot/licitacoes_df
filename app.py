import requests
import json
import os
import gzip
import pandas as pd
import unicodedata
import concurrent.futures
import re
from datetime import datetime, timedelta

# ==========================================
# CONFIGURAÇÕES
# ==========================================
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
ARQ_EXCLUIDOS = 'excluidos.txt'
ARQ_INCLUIR = 'incluir.txt'
MAX_WORKERS = 10 

# Data base para o início da varredura (Divulgação)
DATA_INICIO_DIVULGACAO = datetime(2026, 1, 1)

# Simulação do Hoje (Ajuste para datetime.now() em produção real)
HOJE = datetime(2026, 2, 14) 

# ==========================================
# FILTROS E REGRAS
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
        df = pd.read_csv(ARQ_CSV, encoding='utf-8') if 'utf-8' else pd.read_csv(ARQ_CSV, encoding='latin1')
        return [normalizar(k) for k in df['Fármaco'].dropna().unique().tolist() if len(str(k)) > 2]
    except: return []

def carregar_ids_excluidos():
    ids = set()
    if os.path.exists(ARQ_EXCLUIDOS):
        with open(ARQ_EXCLUIDOS, 'r') as f:
            for l in f: 
                if l.strip(): ids.add(l.strip())
    return ids

def validar_item(descricao, uf):
    desc = normalizar(descricao)
    for b in BLACKLIST: 
        if b in desc: return False
    for k in KEYWORDS_NORDESTE:
        if k in desc: return uf in UFS_NORDESTE
    for k in KEYWORDS_GLOBAL:
        if k in desc: return True
    return False

def processar_licitacao(lic, session, ids_banidos, ignorar_filtros=False):
    lic_id = f"{lic['orgao_cnpj']}{lic['ano_compra']}{lic['sequencial_compra']}"
    if lic_id in ids_banidos: return None

    base_url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{lic['orgao_cnpj']}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}"
    try:
        ri = session.get(f"{base_url}/itens", timeout=15)
        if ri.status_code != 200: return None
        
        itens_validos = []
        uf = lic.get('unidade_orgao', {}).get('uf_sigla', 'XX')
        for it in ri.json():
            if ignorar_filtros or validar_item(it.get('descricao', ''), uf):
                val = it.get('valor_unitario_estimado') or 0.0
                qtd = it.get('quantidade') or 0
                itens_validos.append({
                    "item": it.get('numero_item'), "desc": it.get('descricao'),
                    "qtd": qtd, "unitario_est": float(val), "total_est": float(val) * float(qtd),
                    "situacao": it.get('situacao_compra_item_nome', 'Desconhecido')
                })
        
        if not itens_validos: return None

        return {
            "id": lic_id, "data_pub": lic.get('data_publicacao_pncp', ''),
            "data_enc": lic.get('data_encerramento_proposta', ''), "uf": uf,
            "orgao": lic.get('orgao_nome_fantasia') or lic.get('orgao_razao_social'),
            "objeto": lic.get('objeto_compra'), "itens": itens_validos,
            "link": f"https://pncp.gov.br/app/editais/{lic['orgao_cnpj']}/{lic['ano_compra']}/{lic['sequencial_compra']}"
        }
    except: return None

def processar_inclusoes_manuais(session, banco, ids_banidos):
    if not os.path.exists(ARQ_INCLUIR): return
    with open(ARQ_INCLUIR, 'r') as f: urls = f.readlines()
    for url in urls:
        m = re.search(r"editais/(\d+)/(\d+)/(\d+)", url.strip())
        if m:
            cnpj, ano, seq = m.groups()
            rb = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}")
            if rb.status_code == 200:
                res = processar_licitacao(rb.json(), session, ids_banidos, ignorar_filtros=True)
                if res: banco[res['id']] = res
    with open(ARQ_INCLUIR, 'w') as f: f.write("")

if __name__ == "__main__":
    KEYWORDS_GLOBAL = carregar_keywords_csv()
    IDS_EXCLUIDOS = carregar_ids_excluidos()
    session = requests.Session()
    session.headers.update({"User-Agent": "SniperBot/1.0"})

    data_alvo = DATA_INICIO_DIVULGACAO
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f: data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')

    banco = {}
    if os.path.exists(ARQ_DADOS):
        with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
            for i in json.load(f): 
                if i['id'] not in IDS_EXCLUIDOS: banco[i['id']] = i

    processar_inclusoes_manuais(session, banco, IDS_EXCLUIDOS)

    trigger_next = "false"
    if (HOJE - data_alvo).days >= 0:
        d_str = data_alvo.strftime('%Y%m%d')
        pag = 1
        while True:
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
            r = session.get(url, timeout=20)
            if r.status_code != 200: break
            resp = r.json()
            if not resp.get('data'): break
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futuros = {exe.submit(processar_licitacao, l, session, IDS_EXCLUIDOS): l for l in resp['data']}
                for f in concurrent.futures.as_completed(futuros):
                    res = f.result(); 
                    if res: banco[res['id']] = res
            if pag >= resp.get('total_paginas', 0): break
            pag += 1
        
        proximo = data_alvo + timedelta(days=1)
        with open(ARQ_CHECKPOINT, 'w') as f: f.write(proximo.strftime('%Y%m%d'))
        if (HOJE - proximo).days >= 0: trigger_next = "true"

    lista_final = sorted(banco.values(), key=lambda x: x['data_pub'], reverse=True)
    os.makedirs('dados', exist_ok=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f: print(f"trigger_next={trigger_next}", file=f)
