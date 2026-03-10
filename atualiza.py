import json
import os
import sys
import gzip
import logging
import re
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
        self.max_workers = 5  # Mantido seguro para não sofrer timeout do PNCP
        
        # Mapeamento oficial de status
        self.mapa_situacao = {
            1: "EM ANDAMENTO", 2: "HOMOLOGADO", 3: "CANCELADO", 
            4: "DESERTO", 5: "FRACASSADO"
        }

    def criar_sessao(self):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2,
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
        """
        [CORREÇÃO APLICADA] Extrai as chaves diretamente do Número de Controle do PNCP,
        ignorando a formatação do edital que causa Erros 400.
        """
        try:
            # Pega apenas a parte antes do primeiro sublinhado
            numero_controle = chave_composta.split('_')[0]
            
            # Limpa qualquer caractere não numérico
            numero_controle = re.sub(r'\D', '', numero_controle)

            # O número de controle PNCP tem sempre 14(CNPJ) + 4(Ano) + Sequencial
            if len(numero_controle) < 19:
                return None, None, None

            cnpj = numero_controle[:14]
            ano = numero_controle[14:18]
            sequencial = numero_controle[18:]
            
            return cnpj, ano, sequencial
        except Exception:
            return None, None, None

    def buscar_detalhes_item(self, session, cnpj, ano, sequencial, num_item):
        url_resultados = f"{self.api_base_url}/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{num_item}/resultados"
        try:
            resp = session.get(url_resultados, timeout=15)
            if resp.status_code == 200:
                dados = resp.json()
                if dados:
                    vencedor = dados[0]
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
        session = self.criar_sessao()
        
        # Garante que as chaves sejam tratadas como strings
        id_lic = str(licitacao.get('id', ''))
        edit_lic = str(licitacao.get('edit', ''))
        chave = f"{id_lic}_{edit_lic}"
        
        cnpj, ano, sequencial = self.extrair_chaves(chave)
        
        if not cnpj:
            return {'id': chave, 'erro': 'ID inválido (Número de controle malformado)'}

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

            itens_interesse = licitacao.get('itens', [])
            for item in itens_interesse:
                num_item = item.get('numeroItem')
                if not num_item: continue

                url_item = f"{url_compra}/itens/{num_item}"
                resp_item = session.get(url_item, timeout=15)
                
                info_item = {
                    "numero_item": num_item,
                    "descricao": item.get('descricao', ''),
                    "status_item": "DESCONHECIDO",
                    "valor_estimado": None,
                    "valor_final": None,
                    "fornecedor_vencedor": None,
                    "cnpj_vencedor": None
                }

                if resp_item.status_code == 200:
                    dados_item = resp_item.json()
                    cod_situacao = dados_item.get("situacaoCompraItem", 1)
                    info_item["status_item"] = self.mapa_situacao.get(cod_situacao, "DESCONHECIDO")
                    info_item["valor_estimado"] = dados_item.get("valorTotalEstimado")

                    if cod_situacao == 2:
                        resultados = self.buscar_detalhes_item(session, cnpj, ano, sequencial, num_item)
                        if resultados:
                            info_item["valor_final"] = resultados["valor_final"]
                            info_item["fornecedor_vencedor"] = resultados["fornecedor_nome"]
                            info_item["cnpj_vencedor"] = resultados["fornecedor_cnpj"]

                licitacao_atualizada["itens_atualizados"].append(info_item)

            # Mescla os metadados originais (UF, Órgão, etc) com a nova atualização
            lic_final = licitacao.copy()
            lic_final.update(licitacao_atualizada)

            return {"id": chave, "sucesso": True, "dados": lic_final}

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
        
        licitacoes_pendentes = [lic for lic in licitacoes if f"{str(lic.get('id',''))}_{str(lic.get('edit',''))}" not in processados_ids]
        logger.info(f"Oportunidades a atualizar: {len(licitacoes_pendentes)} (Ignoradas no Checkpoint: {len(processados_ids)})")

        resultados_sucesso = []
        resultados_falhos = []
        processados_atual = list(processados_ids)

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

                if (i + 1) % 50 == 0:
                    self.salvar_checkpoint(processados_atual)
                    logger.info(f"💾 Checkpoint guardado: {len(processados_atual)} processados.")

        # Guardar com um nome fixo para o front-end consumir diretamente
        arquivo_saida = "dados_painel_final_latest.json"
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump({
                "metadados": {"atualizacao": datetime.now().isoformat(), "total": len(resultados_sucesso)},
                "licitacoes": resultados_sucesso
            }, f, ensure_ascii=False, indent=4)

        self.salvar_checkpoint(processados_atual)
        logger.info(f"✅ Atualização concluída. Sucesso: {len(resultados_sucesso)} | Falhas: {len(resultados_falhos)}")
        logger.info(f"📊 Ficheiro para o painel gerado em: {arquivo_saida}")

if __name__ == '__main__':
    atualizador = AtualizadorPNCP()
    atualizador.executar()
