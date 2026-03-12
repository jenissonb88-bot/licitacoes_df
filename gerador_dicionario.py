import csv
import json
import os

def normalizar(texto):
    if not texto: return ""
    # Remove acentos e caracteres especiais simples para busca
    from unicodedata import normalize
    return "".join(c for c in normalize('NFD', str(texto).upper()) if not (ord(c) >= 768 and ord(c) <= 879))

def gerar():
    arquivo_csv = 'Exportar Dados.csv'
    termos_ouro = set()
    
    if not os.path.exists(arquivo_csv):
        print(f"❌ Erro: Ficheiro {arquivo_csv} não encontrado.")
        return

    # Encodings comuns para arquivos vindos do Excel/Windows
    for enc in ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']:
        try:
            with open(arquivo_csv, mode='r', encoding=enc) as f:
                # Detecta se o separador é ; ou ,
                cabecalho = f.readline()
                f.seek(0)
                separador = ';' if ';' in cabecalho else ','
                
                reader = csv.DictReader(f, delimiter=separador)
                
                for row in reader:
                    # ✅ EXTRAÇÃO CIRÚRGICA: Usando as colunas que você me enviou
                    farmaco = normalizar(row.get('Fármaco', ''))
                    forma = normalizar(row.get('Forma Farmacêutica', ''))
                    
                    if farmaco and len(farmaco) > 3:
                        # 1. Adiciona o Princípio Ativo isolado
                        termos_ouro.add(farmaco)
                        
                        # 2. Adiciona a combinação (Princípio + Forma) para Precisão Máxima
                        if forma:
                            termos_ouro.add(f"{farmaco} {forma}")
                break # Se leu com sucesso, para de tentar encodings
        except Exception as e:
            continue

    # 3. Limpeza de termos genéricos que podem ter vindo da coluna Fármaco
    LIXO = {"TAM", "UNICO", "PCT", "UND", "FD", "NAT", "COM", "PARA", "GERAL"}
    termos_finais = [t for t in termos_ouro if t not in LIXO and len(t) > 3]

    with open('dicionario_ouro.json', 'w', encoding='utf-8') as f:
        json.dump(sorted(list(set(termos_finais))), f, ensure_ascii=False, indent=2)

    print(f"✅ Dicionário de Ouro gerado com {len(termos_finais)} termos extraídos das colunas oficiais!")

if __name__ == '__main__':
    gerar()
