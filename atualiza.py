import json
import os
import sys
import gzip
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class AtualizadorPNCP:
    def __init__(self):
        self.api_base_url = "https://pncp.gov.br/api/pncp/v1"
        self.arq_entrada = "pregacoes_pharma_limpos.json.gz"
        self.arq_checkpoint = "checkpoint_atualizacao.json"
        self.max_workers = 5  # Mantido baixo para evitar banimento por IP no PNCP
        
        # Mapeamento de Status do PNCP (Baseado no Manual V1)
        self.mapa_situacao = {
            1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 
            4: "DESERTO", 5: "FRACASSADO"
        }

    def criar_sessao(self):
        """Cria uma sessão HTTP resiliente com tentativas automáticas (Backoff)."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2, # Espera 2s, 4s, 8s entre falhas
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=self.max_workers, pool_maxsize=self.max_workers)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': 'ColetaPNCP-Bot/2.0 (GitHub Actions)',
            'Accept': 'application/json'
        })
        return session

    def extrair_chaves(self, chave_composta):
        """Extrai CNPJ, Ano e Sequencial do ID gerado nos passos anteriores."""
        try:
            partes = chave_composta.split('_')
            cnpj = partes[0]
            edit_parts = partes[1].split('/')
            numero = edit_parts[0]
            ano = edit_parts[1]
            return cnpj, ano, numero
        except Exception:
            return None, None, None

    def buscar_detalhes_item(self, session, cnpj, ano, sequencial, num_item):
        """Busca o valor final e o fornecedor se o item estiver homologado."""
        url_resultados = f"{self.api_base_url}/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{num_item}/resultados"
        
        try:
            resp = session.get(url_resultados, timeout=15)
            if resp.status_code == 200:
                dados = resp.json()
                if dados:
                    vencedor = dados[0] # Assume o primeiro resultado como o adjudicado
                    return {
                        "valor_final": vencedor.get("valorTotalHomologado", 0.0),
                        "fornecedor_nome": vencedor.get("nomeRazaoSocialFornecedor", "N/A"),
                        "fornecedor_cnpj": vencedor.get("niFornecedor", "N/A"),
                        "quantidade_homologada": vencedor.get("quantidadeHomologada", 0)
                    }
        except requests.exceptions.RequestException:
            pass
        return None

    def processar_licitacao(self, licitacao):
        """Atualiza o estado global da licitação e os seus itens."""
        session = self.criar_sessao() # Sessão local para thread safety
        chave = licitacao.get('id', '') + '_' + licitacao.get('edit', '')
        cnpj, ano, sequencial = self.extrair_chaves(chave)
        
        if not cnpj:
            return {'id': chave, 'erro': 'ID inválido'}

        url_compra = f"{self.api_base_url}/orgaos/{cnpj}/compras/{ano}/{sequencial}"
        
        try:
            resp_compra = session.get(url_compra, timeout=15)
            resp_compra.raise_for_status()
            dados_compra = resp_compra.json()
            
            licitacao_atualizada = {
                "chave": chave,
                "status_global": dados_compra.get("situacaoCompraNome"),
                "valor_total_estimado": dados_compra.get("valorTotalEstimado"),
                "itens_atualizados": []
            }

            # Itera sobre os itens do seu portfólio que deram "Match"
            # (Assumindo que os itens vêm no dicionário da licitação)
            itens_interesse = licitacao.get('itens', [])
            for item in itens_interesse:
                num_item = item.get('numeroItem')
                if not num_item: continue

                # Identifica se o item está homologado
                url_item = f"{url_compra}/itens/{num_item}"
                resp_item = session.get(url_item, timeout=15)
                
                info_item = {
                    "numero_item": num_item,
                    "descricao": item.get('descricao', ''),
                    "status_item": "DESCONHECIDO",
                    "valor_final": None,
                    "fornecedor_vencedor": None,
                    "cnpj_vencedor": None
                }

                if resp_item.status_code == 200:
                    dados_item = resp_item.json()
                    cod_situacao = dados_item.get("situacaoCompraItem", 1)
                    info_item["status_item"] = self.mapa_situacao.get(cod_situacao, "DESCONHECIDO")
                    info_item["valor_estimado"] = dados_item.get("valorTotalEstimado")

                    # Se estiver homologado (2), vai buscar quem ganhou
                    if cod_situacao == 2:
                        resultados = self.buscar_detalhes_item(session, cnpj, ano, sequencial, num_item)
                        if resultados:
                            info_item["valor_final"] = resultados["valor_final"]
                            info_item["fornecedor_vencedor"] = resultados["fornecedor_nome"]
                            info_item["cnpj_vencedor"] = resultados["fornecedor_cnpj"]

                licitacao_atualizada["itens_atualizados"].append(info_item)

            return {"id": chave, "sucesso": True, "dados": licitacao_atualizada}

        except Exception as e:
            return {"id": chave, "sucesso": False, "erro": str(e)}

    def carregar_checkpoint(self):
        if os.path.exists(self.arq_checkpoint):
            with open(self.arq_checkpoint, 'r') as f:
                return json.load(f)
        return {"processados": []}

    def salvar_checkpoint(self, processados):
        with open(self.arq_checkpoint, 'w') as f:
            json.dump({"processados": processados}, f)

    def executar(self):
        if not os.path.exists(self.arq_entrada):
            logger.error("Ficheiro base não encontrado.")
            sys.exit(1)

        with gzip.open(self.arq_entrada, 'rt', encoding='utf-8') as f:
            licitacoes = json.load(f)

        checkpoint = self.carregar_checkpoint()
        processados_ids = set(checkpoint.get("processados", []))
        
        licitacoes_pendentes = [lic for lic in licitacoes if (lic.get('id','') + '_' + lic.get('edit','')) not in processados_ids]
        logger.info(f"Oportunidades a atualizar: {len(licitacoes_pendentes)} (Ignoradas no Checkpoint: {len(processados_ids)})")

        resultados_sucesso = []
        resultados_falhos = []
        processados_atual = list(processados_ids)

        # Paralelismo controlado para o PNCP
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futuros = {executor.submit(self.processar_licitacao, lic): lic for lic in licitacoes_pendentes}
            
            for i, future in enumerate(as_completed(futuros)):
                res = future.result()
                if res.get("sucesso"):
                    resultados_sucesso.append(res["dados"])
                    processados_atual.append(res["id"])
                else:
                    resultados_falhos.append(res)
                    logger.warning(f"Erro a processar {res['id']}: {res.get('erro')}")

                # Guardar Checkpoint a cada 50 itens
                if (i + 1) % 50 == 0:
                    self.salvar_checkpoint(processados_atual)
                    logger.info(f"💾 Checkpoint guardado: {len(processados_atual)} processados.")

        # Guardar Ficheiro Final para o Dashboard (Painel de Exibição)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_saida = f"dados_painel_final_{timestamp}.json"
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump({
                "metadados": {"atualizacao": datetime.now().isoformat(), "total": len(resultados_sucesso)},
                "licitacoes": resultados_sucesso
            }, f, ensure_ascii=False, indent=4)

        self.salvar_checkpoint(processados_atual)
        logger.info(f"✅ Atualização concluída. Sucesso: {len(resultados_sucesso)} | Falhas: {len(resultados_falhos)}")
        logger.info(f"📊 Ficheiro pronto para o Painel de Exibição gerado: {arquivo_saida}")

if __name__ == '__main__':
    atualizador = AtualizadorPNCP()
    atualizador.executar()
