import csv
import json
import re
import os

# 1. Palavras que são LIXO absoluto no início da frase (Embalagens/Medidas)
STOPWORDS_IGNORAR = {
    "FRASCO", "FRASCOS", "AMPOLA", "AMPOLAS", "BOLSA", "BOLSAS", "CAIXA", "CX", 
    "COMPRIMIDO", "COMPRIMIDOS", "CPR", "CAPSULA", "CAPSULAS", "INJETAVEL", 
    "GOTAS", "XAROPE", "SOLUCAO", "MG", "ML", "KG", "LITRO", "LITROS", 
    "UNIDADE", "UN", "MEDICAMENTO"
}

# 2. Palavras Genéricas que exigem TERMOS COMPOSTOS (Ex: se achar "AGUA", deve extrair "AGUA PARA INJECAO")
GATILHOS_COMPOSTOS = {
    "AGUA", "TUBO", "COBERTURA", "SUPORTE", "DETERGENTE", "FITA", 
    "KIT", "KITS", "CONJUNTO", "APARELHO", "SISTEMA", "CATETER", "SONDA",
    "LUVA", "LUVAS", "MASCARA", "AVENTAL", "ATADURA", "SERINGA", "AGULHA"
}

def limpar_texto(texto):
    # Remove pontuações e deixa apenas letras e espaços
    return re.sub(r'[^A-Z\s]', '', str(texto).upper().strip())

def extrair_termo_ouro(descricao):
    palavras = limpar_texto(descricao).split()
    if not palavras:
        return None

    # Pula as Stopwords iniciais (Ex: "FRASCO DE DIPIRONA" -> pula "FRASCO" e "DE")
    idx = 0
    while idx < len(palavras) and (palavras[idx] in STOPWORDS_IGNORAR or palavras[idx] == "DE"):
        idx += 1

    if idx >= len(palavras):
        return None
    
    palavra_alvo = palavras[idx]

    # Se a palavra principal for um Gatilho Composto, extraímos as próximas 2 palavras também
    if palavra_alvo in GATILHOS_COMPOSTOS:
        # Ex: ['AGUA', 'PARA', 'INJECAO', '10ML'] -> "AGUA PARA INJECAO"
        termo_composto = " ".join(palavras[idx : idx+3])
        return termo_composto
    else:
        # Se for uma palavra forte (ex: DIPIRONA), extrai só ela
        return palavra_alvo

def gerar():
    arquivo_csv = 'Exportar Dados.csv'
    if not os.path.exists(arquivo_csv):
        print(f"❌ Erro: Ficheiro {arquivo_csv} não encontrado.")
        return

    termos_ouro = set()
    encodings = ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']
    
    for enc in encodings:
        try:
            with open(arquivo_csv, mode='r', encoding=enc) as f:
                amostra = f.read(1024)
                f.seek(0)
                delimitador = ';' if ';' in amostra else ','
                reader = csv.DictReader(f, delimiter=delimitador)
                headers = {h.strip().upper(): h for h in reader.fieldnames if h}
                col_desc = next((h_real for h_upper, h_real in headers.items() if 'DESCRI' in h_upper), None)
                
                for row in reader:
                    desc = str(row.get(col_desc, '')).strip()
                    termo = extrair_termo_ouro(desc)
                    if termo and len(termo) > 2:
                        termos_ouro.add(termo)
            break
        except Exception:
            continue

    # Adicionamos os sinónimos vitais garantidos
    termos_ouro.update([
        "GLICOSE INJETAVEL", "DEXTROSE", "AMINOACIDO", "HEMODIALISE", 
        "TERAPIA RENAL", "FOSFORO", "VITAMINA C", "ACIDO ASCORBICO"
    ])

    lista_ordenada = sorted(list(termos_ouro))
    
    with open('dicionario_ouro.json', 'w', encoding='utf-8') as f:
        json.dump(lista_ordenada, f, ensure_ascii=False, indent=2)

    print(f"✅ Sucesso! 'dicionario_ouro.json' gerado com {len(lista_ordenada)} termos de alta precisão.")

if __name__ == '__main__':
    gerar()
