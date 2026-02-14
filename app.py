import requests
import json
import os
import gzip
import pandas as pd
import unicodedata
import concurrent.futures
import sys
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# CONFIGURAÇÕES & DATES
# ==========================================
ARQ_DADOS = 'dados/oportunidades.json.gz'
ARQ_CHECKPOINT = 'checkpoint.txt'
ARQ_CSV = 'Exportar Dados.csv'
MAX_WORKERS = 10 

# Data base inicial do backlog (fixa)
DATA_INICIO_VARREDURA = datetime(2025, 12, 1)

# Filtro: Só aceita licitações que encerram a partir de:
DATA_CORTE_ENCERRAMENTO = datetime(2026, 1, 1)

# Simulação do "Hoje" (Para seu teste: 14/02/2026)
# Em produção real, você usaria: HOJE = datetime.now()
HOJE = datetime(2026, 2, 14) 

# ==========================================
# LISTAS DE FILTRAGEM (MANTIDAS)
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

# ==========================================
# FUNÇÕES DE APOIO
# ==========================================
def normalizar(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper()

def carregar_keywords_csv():
    if not os.path.exists(ARQ_CSV): return []
    try:
        try: df = pd.read_csv(ARQ_CSV, encoding='utf-8')
        except: df = pd.read_csv(ARQ_CSV, encoding='latin1')
        
        if 'Fármaco' not in df.columns: return []
        raw = df['Fármaco'].dropna().unique().tolist()
        return [normalizar(k) for k in raw if len(str(k)) > 2]
    except: return []

def validar_item(descricao, uf):
    desc_norm = normalizar(descricao)
    for bad in BLACKLIST:
        if bad in desc_norm: return False
    for k in KEYWORDS_NORDESTE:
        if k in desc_norm:
            return True if uf in UFS_NORDESTE else False
    for k in KEYWORDS_GLOBAL:
        if k in desc_norm: return True
    return False

def criar_sessao():
    s = requests.Session()
    s.headers.update({"User-Agent": "SniperBot/1.0"})
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def processar_licitacao(lic, session):
    try:
        # Filtro Data Encerramento
        data_enc_str = lic.get('data_encerramento_proposta')
        if not data_enc_str: return None
        data_enc = datetime.fromisoformat(data_enc_str)
        if data_enc < DATA_CORTE_ENCERRAMENTO: return None

        # Busca Itens
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{lic['orgao_cnpj']}/compras/{lic['ano_compra']}/{lic['sequencial_compra']}/itens"
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
            "id": f"{lic['orgao_cnpj']}{lic['ano_compra']}{lic['sequencial_compra']}",
            "data_pub": lic.get('data_publicacao_pncp', ''),
            "data_encerramento": data_enc_str,
            "uf": uf,
            "cidade": lic.get('unidade_orgao', {}).get('municipio_nome', ''),
            "orgao": lic.get('orgao_nome_fantasia', '') or lic.get('orgao_razao_social', ''),
            "objeto": lic.get('objeto_compra', ''),
            "link": f"https://pncp.gov.br/app/editais/{lic['orgao_cnpj']}/{lic['ano_compra']}/{lic['sequencial_compra']}",
            "itens": itens_validos
        }
    except: return None

def atualizar_resultados(banco, session):
    """
    VARREDURA QUINZENAL:
    Verifica se itens 'EM ANDAMENTO' já têm resultado.
    """
    print("🔍 Iniciando Varredura Quinzenal de Resultados...")
    atualizados = 0
    for lic_id, lic in banco.items():
        # Se a licitação já encerrou há mais de 2 dias, vale checar
        try:
            dt_enc = datetime.fromisoformat(lic['data_encerramento'])
            if dt_enc > datetime.now(): continue # Ainda não encerrou
        except: continue

        url_resultados = f"https://pncp.gov.br/api/pncp/v1/orgaos/{lic['id'][:14]}/compras/{lic['id'][14:18]}/{lic['id'][18:]}/itens"
        
        try:
            r = session.get(url_resultados, timeout=10)
            if r.status_code == 200:
                itens_novos = {it['numero_item']: it for it in r.json()}
                
                for item_salvo in lic['itens']:
                    novo_dado = itens_novos.get(item_salvo['item'])
                    if novo_dado:
                        # Atualiza status
                        if item_salvo['situacao'] != novo_dado.get('situacao_compra_item_nome'):
                            item_salvo['situacao'] = novo_dado.get('situacao_compra_item_nome')
                            atualizados += 1
                        
                        # Se tiver resultado homologado, pega vencedor (Lógica simplificada)
                        if 'HOMOLOGADO' in str(item_salvo['situacao']).upper():
                            item_salvo['vencedor'] = novo_dado.get('tem_resultado', False)
                            # (Aqui poderia expandir para buscar o nome do vencedor em outro endpoint se necessário)
        except: pass
    
    print(f"✅ Varredura Concluída. {atualizados} itens atualizados.")

# ==========================================
# MAIN - FLUXO ATÔMICO
# ==========================================
if __name__ == "__main__":
    print(f"🚀 SNIPER PNCP - DATA BASE: {HOJE.strftime('%d/%m/%Y')}")
    
    # 1. Carregar Keywords
    KEYWORDS_GLOBAL = carregar_keywords_csv()

    # 2. Ler Checkpoint (Onde parei?)
    data_alvo = DATA_INICIO_VARREDURA
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                data_alvo = datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass

    # 3. Determinar o MODO DE OPERAÇÃO
    session = criar_sessao()
    
    # Carrega banco atual
    banco = {}
    if os.path.exists(ARQ_DADOS):
        try:
            with gzip.open(ARQ_DADOS, 'rt', encoding='utf-8') as f:
                lista = json.load(f)
                banco = {i['id']: i for i in lista}
        except: pass

    dias_atraso = (HOJE - data_alvo).days

    if dias_atraso > 3:
        # --- MODO BACKLOG (Atrasado) ---
        # Processa APENAS 1 dia (data_alvo) e para.
        print(f"⚠️ MODO BACKLOG: Processando apenas {data_alvo.strftime('%d/%m/%Y')} (Atraso: {dias_atraso} dias)")
        datas_para_processar = [data_alvo]
        proximo_checkpoint = data_alvo + timedelta(days=1)
        salvar_checkpoint = True

    else:
        # --- MODO ROTINA (Em dia) ---
        # Processa os últimos 3 dias para garantir
        print(f"✅ MODO ROTINA: Varrendo últimos 3 dias até {HOJE.strftime('%d/%m/%Y')}")
        datas_para_processar = [HOJE - timedelta(days=i) for i in range(3)]
        proximo_checkpoint = HOJE # Mantém checkpoint no dia atual
        salvar_checkpoint = True # Atualiza para hoje
        
        # --- VARREDURA QUINZENAL (Só no modo rotina) ---
        if HOJE.day in [1, 15]:
            atualizar_resultados(banco, session)

    # 4. Execução da Coleta
    novos = 0
    for data_proc in datas_para_processar:
        d_str = data_proc.strftime('%Y%m%d')
        print(f"📂 Coletando dia: {data_proc.strftime('%d/%m/%Y')}...")
        
        pag = 1
        while True:
            url = f"https://pncp.gov.br/api/pncp/v1/compras?data_inicial={d_str}&data_final={d_str}&modalidade_contratacao_id=6&pagina={pag}&tamanho_pagina=50"
            try:
                r = session.get(url, timeout=20)
                if r.status_code != 200: break
                resp = r.json()
                data = resp.get('data', [])
                if not data: break

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                    futures = {exe.submit(processar_licitacao, l, session): l for l in data}
                    for f in concurrent.futures.as_completed(futures):
                        res = f.result()
                        if res:
                            banco[res['id']] = res
                            novos += 1
                            print(".", end="", flush=True)

                if pag >= resp.get('total_paginas', 0): break
                pag += 1
            except Exception as e:
                print(f"Erro Pág {pag}: {e}")
                break
        print(f" -> OK")

    # 5. Salvar Tudo
    print(f"\n💾 Salvando... (Total no banco: {len(banco)})")
    
    # Salvar Dados
    os.makedirs('dados', exist_ok=True)
    lista_final = sorted(list(banco.values()), key=lambda x: x.get('data_encerramento', ''), reverse=True)
    with gzip.open(ARQ_DADOS, 'wt', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False, separators=(',', ':'))

    # Salvar Checkpoint (Só se estiver em modo Backlog ou se for dia de atualização)
    if salvar_checkpoint:
        with open(ARQ_CHECKPOINT, 'w') as f:
            f.write(proximo_checkpoint.strftime('%Y%m%d'))
            
    print(f"🏁 Finalizado. Checkpoint movido para: {proximo_checkpoint.strftime('%d/%m/%Y')}")
