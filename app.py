import os
import requests
import pandas as pd
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class ColetaPNCP:
    def __init__(self):
        self.base_url = "https://pncp.gov.br/api/search"
        self.data_corte = "2026-01-01"
        self.token = os.getenv("PNCP_API_TOKEN", "") # Caso o PNCP passe a exigir ou para APIs do ComprasGov
        
        # Palavras-chave focadas em Saúde, Medicamentos e Higiene
        self.palavras_chave = [
            "medicamento", "hospitalar", "farmaco", "seringa", 
            "higiene", "odonto", "clinico", "laboratorial"
        ]
        
        # Configuração de resiliência de rede (Retry)
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 429, 500, 502, 503, 504 ])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def formatar_data(self, data_str):
        """Converte data para o formato exigido pela API ou valida a data de corte."""
        try:
            return datetime.strptime(data_str, "%Y-%m-%d").strftime("%Y%m%d")
        except ValueError:
            return None

    def buscar_licitacoes(self):
        """Realiza a busca paginada na API do PNCP."""
        print(f"[*] Iniciando coleta de licitações a partir de {self.data_corte}...")
        resultados_totais = []
        
        headers = {
            "Accept": "application/json",
            "User-Agent": "ColetaPNCP-Bot/1.0"
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        for termo in self.palavras_chave:
            print(f"[*] Buscando termo: {termo}")
            pagina = 1
            tem_mais = True
            
            while tem_mais:
                # Parâmetros de busca baseados no Manual PNCP V1
                params = {
                    "q": termo,
                    "tipos_documento": "edital",
                    "data_inicial": self.formatar_data(self.data_corte),
                    "pagina": pagina,
                    "tamanhoPagina": 50 # Maximizando a coleta por requisição
                }

                try:
                    response = self.session.get(self.base_url, params=params, headers=headers, timeout=30)
                    response.raise_for_status()
                    dados = response.json()
                    
                    items = dados.get("items", [])
                    if not items:
                        tem_mais = False
                        continue
                        
                    for item in items:
                        # Estruturação rigorosa dos dados para o front-end
                        licitacao = {
                            "orgao_nome": item.get("orgaoEntidade", {}).get("razaoSocial"),
                            "orgao_cnpj": item.get("orgaoEntidade", {}).get("cnpj"),
                            "uf": item.get("unidadeOrgao", {}).get("ufSigla"),
                            "municipio": item.get("unidadeOrgao", {}).get("municipioNome"),
                            "numero_edital": item.get("numeroCompra"),
                            "ano_compra": item.get("anoCompra"),
                            "objeto": item.get("objetoCompra"),
                            "data_publicacao": item.get("dataPublicacaoPncp"),
                            "link_pncp": item.get("linkSistemaOrigem"),
                            "status": item.get("situacaoCompraNome"),
                            "valor_estimado": item.get("valorTotalEstimado"),
                            "chave_pncp": f"{item.get('orgaoEntidade', {}).get('cnpj')}-{item.get('anoCompra')}-{item.get('numeroCompra')}"
                        }
                        resultados_totais.append(licitacao)
                    
                    # Controle de paginação
                    total_paginas = dados.get("totalPaginas", 1)
                    if pagina >= total_paginas:
                        tem_mais = False
                    else:
                        pagina += 1
                        
                except requests.exceptions.RequestException as e:
                    print(f"[!] Erro de conexão ao buscar '{termo}' na página {pagina}: {e}")
                    tem_mais = False

        return self.remover_duplicatas(resultados_totais)

    def remover_duplicatas(self, dados):
        """Remove editais duplicados encontrados por múltiplas palavras-chave."""
        print("[*] Removendo duplicatas e limpando base preliminar...")
        df = pd.DataFrame(dados)
        if df.empty:
            return df
        
        df = df.drop_duplicates(subset=['chave_pncp'])
        print(f"[*] Total de licitações únicas capturadas: {len(df)}")
        return df

    def salvar_dados(self, df):
        """Salva os dados brutos para a próxima etapa (limpeza.py)."""
        os.makedirs("data", exist_ok=True)
        arquivo_saida = "data/licitacoes_brutas.json"
        df.to_json(arquivo_saida, orient="records", force_ascii=False, indent=4)
        print(f"[*] Dados salvos com sucesso em {arquivo_saida}")

if __name__ == "__main__":
    coletor = ColetaPNCP()
    df_licitacoes = coletor.buscar_licitacoes()
    if not df_licitacoes.empty:
        coletor.salvar_dados(df_licitacoes)
    else:
        print("[!] Nenhuma licitação encontrada para os parâmetros informados.")
