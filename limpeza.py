import json
import gzip
import os
import unicodedata
import re
import sys
import concurrent.futures
from datetime import datetime

class LimpadorPNCP:
    def __init__(self):
        # Configurações de Ficheiros
        self.arq_dados = 'dadosoportunidades.json.gz' # Certifique-se que o app.py gera este nome
        self.arq_limpo = 'pregacoes_pharma_limpos.json.gz'
        self.data_corte = datetime(2026, 1, 1)

        # Geografia
        self.ne_estados = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
        self.estados_bloqueados = ['RS', 'SC', 'PR', 'AP', 'AC', 'RO', 'RR']
        self.ufs_permitidas_mmh = self.ne_estados 

        # Regras de Negócio (Já normalizadas na inicialização)
        self.vetos_absolutos = self._normalizar_lista([
            "INTENCAO DE REGISTRO DE PRECO", "INTENCAO REGISTRO DE PRECO",
            "CREDENCIAMENTO", "ADESAO", "IRP", "LEILAO", "ALIENACAO"
        ])

        self.vetos_imediatos = self._gerar_vetos_imediatos([
            "PRESTACAO DE SERVICO", "SERVICO ESPECIALIZADO", "LOCACAO", "INSTALACAO",
            "ASFALTICO", "ASFALTO", "MANUTENCAO PREDIAL", "MANUTENCAO DE EQUIPAMENTOS",
            "MANUTENCAO PREVENTIVA", "MANUTENCAO CORRETIVA", "UNIFORME", "TEXTIL",
            "REFORMA", "GASES MEDICINAIS", "CILINDRO", "LIMPEZA PREDIAL", "LAVANDERIA",
            "IMPRESSAO", "OBRAS", "CONSTRUCAO", "PAVIMENTACAO", "LIMPEZA URBANA",
            "RESIDUOS SOLIDOS", "LOCACAO DE VEICULOS", "TRANSPORTE", "COMBUSTIVEL",
            "DIESEL", "GASOLINA", "PNEUS", "PECAS AUTOMOTIVAS", "OFICINA", "VIGILANCIA",
            "SEGURANCA", "BOMBEIRO", "SALVAMENTO", "RESGATE", "VIATURA", "FARDAMENTO",
            "VESTUARIO", "INFORMATICA", "COMPUTADORES", "EVENTOS", "REPARO",
            "CORRETIVA", "GERADOR", "VEICULO", "AMBULANCIA", "MOTOCICLETA",
            "MECANICA", "FERRO FUNDIDO", "CONTRATACAO DE SERVICO",
            "EQUIPAMENTO E MATERIA PERMANENTE", "RECARGA", "CONFECCAO",
            "EQUIPAMENTOS PERMANENTES", "MATERIAIS PERMANENTES"
        ])

        self.termos_ne_mmh_nutri = self._normalizar_lista([
            "MATERIAL MEDIC", "INSUMO HOSPITALAR", "MMH", "SERINGA", "AGULHA",
            "GAZE", "ATADURA", "SONDA", "CATETER", "EQUIPO", "LUVAS DE PROCEDIMENTO",
            "MASCARA", "MASCARA CIRURGICA", "PENSO", "MATERIAL PENSO",
            "MATERIAL-MEDICO", "MATERIAIS-MEDICO", "FRALDA", "ABSORVENTE",
            "MEDICO-HOSPITALAR", "CURATIV", "CURATIVO", "CURATIVOS",
            "LUVA DE PROCEDIMENTO", "COMPRESSA GAZE", "AVENTAL DESCARTAVEL",
            "GESSADA", "CAMPO OPERATORIO", "CLOREXIDINA", "COLETOR PERFURO",
            "ESPARADRAPO", "FITA MICROPORE", "GLUTARALDEIDO", "SONDA NASO",
            "TOUCA DESCARTAVEL", "TUBO ASPIRACAO", "NUTRICAO ENTERAL",
            "FORMULA INFANTIL", "SUPLEMENTO ALIMENTAR", "DIETA ENTERAL",
            "DIETA PARENTERAL", "NUTRICAO CLINICA", "ENTERAL", "FORMULA ESPECIA",
            "AGULHAS", "SERINGAS", "PARENTERA", "ENTERAL"
        ])

        self.termos_salvamento = self._normalizar_lista([
            "MEDICAMENT", "FARMAC", "REMEDIO", "SORO", "FARMACO", "AMPOLA",
            "COMPRIMIDO", "INJETAVEL", "VACINA", "INSULINA", "ANTIBIOTICO",
            "AQUISICAO DE MEDICAMENTO", "AQUISICAO DE MEDICAMENTOS"
        ])

    def _normalizar_texto(self, texto):
        if not texto:
            return ""
        # Remove acentos e converte para maiúsculas
        texto = ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn')
        return texto.upper()

    def _normalizar_lista(self, lista):
        return [self._normalizar_texto(item) for item in lista]

    def _gerar_vetos_imediatos(self, lista_base):
        vetos = set()
        for termo in lista_base:
            norm = self._normalizar_texto(termo)
            vetos.add(norm)
            # Adiciona plural simples de forma mais segura
            if not norm.endswith('S'):
                vetos.add(norm + 'S')
        return list(vetos)

    def _contem_termo_exato(self, termo, texto):
        """Usa RegEx para procurar a palavra inteira, evitando falsos positivos."""
        padrao = r'\b' + re.escape(termo) + r'\b'
        return re.search(padrao, texto) is not None

    def _tem_medicamento(self, texto_norm):
        return any(termo in texto_norm for termo in self.termos_salvamento)

    def analisar_pertinencia(self, obj_norm, uf, itens=None):
        # 1. VETOS ABSOLUTOS
        if any(veto in obj_norm for veto in self.vetos_absolutos):
            return False

        # 2. SUPER PASSE (Medicamentos)
        tem_med = self._tem_medicamento(obj_norm)
        if not tem_med and itens:
            for item in itens:
                desc = item.get('d', '') # Assume que a chave da descrição do item é 'd'
                if self._tem_medicamento(self._normalizar_texto(desc)):
                    tem_med = True
                    break

        if tem_med:
            return uf not in self.estados_bloqueados

        # 3. VETOS IMEDIATOS (Com proteção de falsos positivos via RegEx)
        for veto in self.vetos_imediatos:
            if self._contem_termo_exato(veto, obj_norm):
                return False

        # 4. MMH/NUTRIÇÃO - Apenas Nordeste
        if any(t in obj_norm for t in self.termos_ne_mmh_nutri):
            return uf in self.ufs_permitidas_mmh

        return False

    def processar_licitacao(self, licitacao):
        if not licitacao:
            return None

        uf = str(licitacao.get('uf', '')).upper()
        obj_norm = self._normalizar_texto(licitacao.get('obj', ''))
        itens = licitacao.get('itens', [])

        # Validação de Pertinência
        if not self.analisar_pertinencia(obj_norm, uf, itens):
            return None

        # Validação de Data
        dt_enc_str = licitacao.get('dt_enc')
        if not dt_enc_str:
            return None

        try:
            dt_enc = datetime.fromisoformat(dt_enc_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if dt_enc < self.data_corte:
                return None
        except ValueError:
            return None

        # Chave única: Assume que o identificador tem pelo menos 14 caracteres (CNPJ)
        chave_unica = f"{str(licitacao.get('id', ''))[:14]}_{licitacao.get('edit', '')}"
        
        return (chave_unica, licitacao, dt_enc, len(itens))

    def executar(self):
        if not os.path.exists(self.arq_dados):
            print(f"[!] Ficheiro {self.arq_dados} não encontrado. Execute o extrator primeiro.")
            sys.exit(1)

        print("🧹 A iniciar limpeza e padronização de dados...")

        with gzip.open(self.arq_dados, 'rt', encoding='utf-8') as f:
            banco_bruto = json.load(f)

        print(f"📊 Total de registos no banco bruto: {len(banco_bruto)}")

        banco_deduplicado = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            resultados = executor.map(self.processar_licitacao, banco_bruto)

        for res in resultados:
            if res is None:
                continue

            chave, card, dt_novo, qtd_itens_novo = res

            if chave not in banco_deduplicado:
                banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}
            else:
                qtd_antiga = banco_deduplicado[chave]['qtd']
                dt_antiga = banco_deduplicado[chave]['dt']
                
                if qtd_itens_novo > qtd_antiga or (qtd_itens_novo == qtd_antiga and dt_novo > dt_antiga):
                    banco_deduplicado[chave] = {'card': card, 'dt': dt_novo, 'qtd': qtd_itens_novo}

        lista_final = [item['card'] for item in banco_deduplicado.values()]

        print(f"💾 A guardar {len(lista_final)} licitações processadas e limpas...")

        with gzip.open(self.arq_limpo, 'wt', encoding='utf-8') as f:
            json.dump(lista_final, f, ensure_ascii=False)

        rejeitadas = len(banco_bruto) - len(lista_final)
        print(f"✅ Concluído! {len(lista_final)} oportunidades validadas e prontas para o Dashboard.")
        print(f"📉 Rejeitadas pelo algoritmo de negócio: {rejeitadas}")

if __name__ == '__main__':
    limpador = LimpadorPNCP()
    limpador.executar()
