import csv
import json
import os
import re

def normalizar(texto):
    if not texto: return ""
    from unicodedata import normalize
    # Remove acentos e limpa espaços extras
    texto_limpo = "".join(c for c in normalize('NFD', str(texto).upper()) if not (ord(c) >= 768 and ord(c) <= 879))
    return texto_limpo.strip()

def eh_termo_valido(termo):
    """Verifica se o termo é um nome real e não uma dosagem/medida."""
    if not termo or len(termo) < 3: return False
    
    # Se começar com número, geralmente é dosagem (Ex: 05MG, 10ML) -> DESCARTA
    if re.match(r'^\d', termo): return False
    
    # Se for apenas unidades de medida comuns -> DESCARTA
    MEDIDAS = {"MG", "ML", "G", "KG", "UND", "PCT", "AMP", "CPR", "CX", "CAPS"}
    if termo in MEDIDAS: return False
    
    # Se contiver muitos números misturados (Ex: A1, C1, 40MG/ML) -> DESCARTA
    if sum(c.isdigit() for c in termo) > 3: return False
    
    return True

def gerar():
    arquivo_csv = 'Exportar Dados.csv'
    termos_ouro = set()
    
    if not os.path.exists(arquivo_csv):
        print(f"❌ Erro: Ficheiro {arquivo_csv} não encontrado.")
        return

    for enc in ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']:
        try:
            with open(arquivo_csv, mode='r', encoding=enc) as f:
                cabecalho = f.readline()
                f.seek(0)
                separador = ';' if ';' in cabecalho else ','
                reader = csv.DictReader(f, delimiter=separador)
                
                for row in reader:
                    # ✅ Captura as colunas oficiais
                    farmaco_bruto = row.get('Fármaco', '')
                    forma_bruta = row.get('Forma Farmacêutica', '')
                    
                    farmaco = normalizar(farmaco_bruto)
                    forma = normalizar(forma_bruta)
                    
                    # Só processa se o fármaco for um nome válido (não dosagem)
                    if eh_termo_valido(farmaco):
                        # 1. Adiciona o Princípio Ativo limpo (ex: ACICLOVIR)
                        termos_ouro.add(farmaco)
                        
                        # 2. Adiciona a combinação (ex: ACICLOVIR CREME)
                        if forma and len(forma) > 2:
                            termos_ouro.add(f"{farmaco} {forma}")
                            
                break 
        except Exception:
            continue

    # 3. Filtro final de segurança contra termos genéricos de material médico
    LIXO_EXTRA = {"PARA", "COM", "SEM", "USO", "GERAL", "TIPO", "ACOMPANHA"}
    termos_finais = [t for t in termos_ouro if t not in LIXO_EXTRA]

    with open('dicionario_ouro.json', 'w', encoding='utf-8') as f:
        json.dump(sorted(list(set(termos_finais))), f, ensure_ascii=False, indent=2)

    print(f"✅ Dicionário REFINADO gerado com {len(termos_finais)} termos puros!")

if __name__ == '__main__':
    gerar()
