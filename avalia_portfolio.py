import pandas as pd
import re
import json
import gzip
import csv
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
import unicodedata

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

# Thresholds de matching (ajustáveis)
THRESHOLD_ALTO = 0.70      # ≥70% = ALTO (participar)
THRESHOLD_MEDIO = 0.50   # 50-69% = MÉDIA (avaliar)
THRESHOLD_BAIXO = 0.30   # 30-49% = BAIXA (descartar)
# <30% = INCOMPATÍVEL

# Arquivos padrão (compatíveis com fluxo existente)
ARQ_PORTFOLIO = 'Exportar Dados.csv'
ARQ_LICITACOES = 'pregacoes_pharma_limpos.json.gz'
ARQ_SAIDA = 'relatorio_compatibilidade.csv'
ARQ_LOG = 'log_matcher.log'

MAX_WORKERS = 10

# ============================================================================
# DICIONÁRIO DE SINÔNIMOS FARMACÊUTICOS (expansível)
# ============================================================================

SINONIMOS_FARMACOS = {
    "ESCOPOLAMINA": ["HIOSCINA", "BUTILBROMETO DE ESCOPOLAMINA", "BROMETO DE ESCOPOLAMINA", 
                     "BUTILESCOPOLAMINA", "BUSCOPAN", "BUSCOPAN COMPOSTO", "BUSCOPAN COMP"],
    "DIPIRONA": ["METAMIZOL", "DIPIRONA MONOIDRATADA", "DIPIRONA SODICA", 
                 "NORAMIDAZOFENINA", "DIPIRONA SODICA + ESCOPOLAMINA"],
    "EPINEFRINA": ["ADRENALINA"],
    "FENITOÍNA": ["HIDANTOÍNA", "DIFENILHIDANTOÍNA", "FENITOINA"],
    "FENOBARBITAL": ["FENOBARBITONA", "GARDENAL"],
    "DIAZEPAM": ["VALIUM"],
    "MIDAZOLAM": ["DORMONID"],
    "CLONAZEPAM": ["RIVOTRIL"],
    "HALOPERIDOL": ["HALDOL"],
    "PARACETAMOL": ["ACETAMINOFENO", "ACETAMINOFEN"],
    "CLAVULANATO DE POTÁSSIO": ["CLAV POTASSIO", "CLAVULANATO", "CLAV POT", 
                                 "ÁCIDO CLAVULÂNICO", "CLAVULANATO POTASSIO"],
    "AMOXICILINA": ["AMOXICILINA TRIIDRATADA"],
    "SULFAMETOXAZOL": ["SULFA", "SULFAMETOXAZOL"],
    "TRIMETOPRIMA": ["TMP"],
    "VITAMINA C": ["ÁCIDO ASCÓRBICO", "ASCORBINICO", "ACIDO ASCORBICO"],
    "VITAMINA A": ["RETINOL"],
    "VITAMINA D": ["COLECALCIFEROL", "ERGOCALCIFEROL"],
    "VITAMINA K": ["FITOMENADIONA", "FITONADIONA", "FITOADIONA"],
    "GLICOSE": ["DEXTRose"],
    "CLORETO DE SODIO": ["SORO FISIOLOGICO", "SF", "NACL"],
    "AAS": ["ÁCIDO ACETILSALICÍLICO", "ACETYLSALICYLIC ACID", "ACIDO ACETILSALICILICO"],
    "ÁCIDO ACETILSALICÍLICO": ["AAS"],
    "BUTILBROMETO DE ESCOPOLAMINA": ["HIOSCINA", "ESCOPOLAMINA"],
    "NOREPINEFRINA": ["NORADRENALINA"],
    "FENTANILA": ["CITRATO DE FENTANILA"],
    "MORFINA": ["CLORETO DE MORFINA", "SULFATO DE MORFINA"],
    "DEXTROCETAMINA": ["CETAMINA"],
    "CETOPROFENO": ["CETOPROFENO"],
    "DICLOFENACO": ["DICLOFENACO SODICO", "DICLOFENACO POTASSICO", "VOLTAREN"],
    "IBUPROFENO": ["ADVIL", "MOTRIN"],
    "NAPROXENO": ["FLANAX", "NAPROSYN"],
    "CEFALEXINA": ["KEFLEX"],
    "AZITROMICINA": ["ZITROMAC"],
    "CLARITROMICINA": ["KLACID"],
    "METFORMINA": ["GLIFAGE"],
    "GLIBENCLAMIDA": ["DAONIL"],
    "ENALAPRIL": ["RENITE"],
    "CAPTOPRIL": ["CAPOTEN"],
    "LOSARTANA": ["COZAAR"],
    "ATENOLOL": ["ATELOCARD"],
    "PROPRANOLOL": ["INDERAL"],
    "METOPROLOL": ["LOPRESOR"],
    "ANLODIPINO": ["NORVASC"],
    "OMEPRAZOL": ["LOSEC", "OMEPRAM"],
    "PANTOPRAZOL": ["PANTOZOL"],
    "ESOMEPRAZOL": ["NEXIUM"],
    "RANITIDINA": ["AERODIN"],
    "ONDANSETRONA": ["ZOFRAN"],
    "METOCLOPRAMIDA": ["PLASIL"],
    "BROMOPRIDA": ["DIGESPRID"],
    "SIMETICONA": ["GAS-X", "AIRLIX"],
    "BISACODIL": ["DULCOLAX"],
    "SENNA": ["SEAKAL", "LAXOL"],
    "LOPERAMIDA": ["IMODIUM"],
    "CLORPROMAZINA": ["AMINAZIN", "CLOPROMAZ"],
    "LEVOMEPROMAZINA": ["NOZINAN"],
    "HALOPERIDOL": ["HALDOL"],
    "RISPERIDONA": ["RISPERDAL"],
    "OLANZAPINA": ["ZYPREXA"],
    "QUETIAPINA": ["SEROQUEL"],
    "ARIPIPRAZOL": ["ABILIFY"],
    "CLOZAPINA": ["LEPONEX"],
    "CARBAMAZEPINA": ["TEGRETOL"],
    "VALPROATO": ["DEPAKENE", "VALPAK"],
    "FENITOINA": ["HIDANTOINA"],
    "LAMOTRIGINA": ["LAMICTAL"],
    "TOPIRAMATO": ["TOPAMAX"],
    "GABAPENTINA": ["NEURONTIN"],
    "PREGABALINA": ["LYRICA"],
    "SERTRALINA": ["ZOLOFT"],
    "FLUOXETINA": ["PROZAC"],
    "PAROXETINA": ["PAXIL", "AROPAX"],
    "CITALOPRAM": ["CELEXA"],
    "ESCITALOPRAM": ["LEXAPRO"],
    "VENLAFAXINA": ["EFEXOR"],
    "DULOXETINA": ["CYMBALTA"],
    "BUPROPIONA": ["WELLBUTRIN", "ZYBAN"],
    "MITRATAZAPINA": ["REMERON"],
    "TRAZODONA": ["DESYREL"],
    "AMITRIPTILINA": ["TRYPTANOL"],
    "NORTRIPTILINA": ["PAMELOR"],
    "IMIPRAMINA": ["TOFRANIL"],
    "CLOMIPRAMINA": ["ANAFRANIL"],
    "ALPRAZOLAM": ["FRONTAL", "XANAX"],
    "BROMAZEPAM": ["LEXOTAN"],
    "LORAZEPAM": ["ATIVAN"],
    "NITRAZEPAM": ["MOGADON"],
    "FLUNITRAZEPAM": ["ROHYPNOL"],
    "ZOLPIDEM": ["STILNOX"],
    "ZOPICLONA": ["IMOVANE"],
    "TRAMADOL": ["TRAMAL"],
    "CODEINA": ["CODALGIN"],
    "MORFINA": ["DIMORF"],
    "FENTANILA": ["DURAGESIC", "FENTANYL"],
    "METADONA": ["METADOL"],
    "NALBUFINA": ["NUBAIN"],
    "BUPRENORFINA": ["SUBUTEX", "TEMGESIC"],
    "NALOXONA": ["NARCAN"],
    "ACICLOVIR": ["ZOVIRAX"],
    "VALACICLOVIR": ["VALTREX"],
    "FAMCICLOVIR": ["FAMVIR"],
    "AMANTADINA": ["SYMMETREL"],
    "OSSELTAMIVIR": ["TAMIFLU"],
    "RIBAVIRINA": ["VIRAZOLE", "REBETOL"],
    "ZIDOVUDINA": ["RETROVIR"],
    "LAMIVUDINA": ["EPIVIR"],
    "TENOFOVIR": ["VIREAD"],
    "EFAVIRENZ": ["SUSTIVA"],
    "NEVIRAPINA": ["VIRAMUNE"],
    "INDINAVIR": ["CRIXIVAN"],
    "RITONAVIR": ["NORVIR"],
    "LOPINAVIR": ["KALETRA"],
    "ATAZANAVIR": ["REYATAZ"],
    "DARUNAVIR": ["PREZISTA"],
    "RALTEGRAVIR": ["ISENTRESS"],
    "MARAVIROC": ["SELZENTRY"],
    "ENFUROVITIDA": ["FUZEON"],
    "FLUCONAZOL": ["DIFLUCAN"],
    "ITRACONAZOL": ["SPORANOX"],
    "VORICONAZOL": ["VFEND"],
    "POSACONAZOL": ["NOXAFIL"],
    "ANFOTERICINA": ["FUNGIZONE", "AMBISOME"],
    "CASPofungina": ["CANCIDAS"],
    "MICAFUNGINA": ["MYCAMINE"],
    "ANIDULAFUNGINA": ["ERAXIS"],
    "CETOCONAZOL": ["NIZORAL"],
    "MICONAZOL": ["MONISTAT", "Daktarin"],
    "CLOTRIMAZOL": ["CANESTEN", "GYNE-LOTRIMIN"],
    "NISTATINA": ["MYCOSTATIN", "NILSTAT"],
    "TERBINAFINA": ["LAMISIL"],
    "GRISEOFULVINA": ["FULVICIN"],
    "AMFOTERICINA": ["AMBISOME", "ABELCET"],
    "PENTAMIDINA": ["PENTAM", "NEBUPENT"],
    "ATOVaquona": ["MEPRON"],
    "AZITROMICINA": ["ZITROMAX", "AZITHROMYCIN"],
    "CLARITROMICINA": ["BIAXIN", "KLARICID"],
    "ERITROMICINA": ["ILOSONE", "ERYTHROCIN"],
    "ROXITROMICINA": ["RULID", "XITOCIN"],
    "ESPIRAMICINA": ["ROVAMYCINE"],
    "JOSAMICINA": ["JOSACINE", "JOSALID"],
    "MIDEACAMICINA": ["MOCAMYCIN"],
    "CLINDAMICINA": ["DALACIN", "CLEOCIN"],
    "LINCOMICINA": ["LINCOCIN"],
    "PIRMECILINAM": ["SELEXID", "MECILINAM"],
    "FOSFOMICINA": ["MONUROL", "FOSFOMYCIN"],
    "FUSIDICO": ["FUCIDIN", "FUSIDIC ACID"],
    "MUPROCINA": ["BACTROBAN"],
    "RETAPAMULINA": ["ALTABAX"],
    "BACITRACINA": ["BACIGUENT"],
    "NEOMICINA": ["MYCIFRADIN", "FRAMYCETIN"],
    "GENTAMICINA": ["GARAMYCIN", "CIDOMYCIN"],
    "TOBRAMICINA": ["TOBREX", "TOBI"],
    "AMICACINA": ["AMIKIN", "AMIKACIN"],
    "NETILMICINA": ["NETROMICIN"],
    "ESTREPTOMICINA": ["STREPTOMYCIN"],
    "KANAMICINA": ["KANTREX"],
    "CAPREOMICINA": ["CAPASTAT"],
    "VIOMICINA": ["VIOCIN"],
    "PAROMOMICINA": ["HUMATIN", "AMINOSIDIN"],
    "SPECTINOMICINA": ["TROBICIN"],
    "POLIMIXINA": ["POLYMYXIN", "AEROSPORIN"],
    "COLISTINA": ["COLISTIMETHATE", "PROMIX"],
    "VANCOMICINA": ["VANCOCIN", "VANCOLED"],
    "TEICOPLANINA": ["TARGOCID"],
    "TELAVANCINA": ["VIBATIV"],
    "DALBAVANCINA": ["DALVANCE"],
    "ORITAVANCINA": ["ORBACTIV"],
    "DAPTOMICINA": ["CUBICIN"],
    "TIGECICLINA": ["TYGACIL"],
    "ERAVACICLINA": ["XERAVA"],
    "OMADACICLINA": ["NUZYRA"],
    "TETRACICLINA": ["ACHROMYCIN", "SUMYCIN"],
    "DOXICICLINA": ["VIBRAMYCIN", "DORYX"],
    "MINOCICLINA": ["MINOCIN", "DYNACIN"],
    "CLORTETRACICLINA": ["AUREOMYCIN"],
    "OMETRACICLINA": ["TERRAMYCIN"],
    "METRONIDAZOL": ["FLAGYL"],
    "TINIDAZOL": ["TINDAMAX", "FASIGYN"],
    "ORNIDAZOL": ["TIBERAL", "ORNID"],
    "SECNIDAZOL": ["SECNID", "FLADEN"],
    "NIMORAZOL": ["NAXOGIN", "ACTIV"],
    "ALBENDAZOL": ["ZENTEL", "ALBENZA"],
    "MEBENDAZOL": ["VERMOX", "OVEX"],
    "TIABENDAZOL": ["MINTEZOL"],
    "PIRANTEL": ["COMBANTRIN", "ANTIMINTH"],
    "IVERMECTINA": ["STROMECTOL", "MECTIZAN"],
    "DIETILCARBAMAZINA": ["HETRAZAN", "BANOCIDE"],
    "PRAZIQUANTEL": ["BILTRICIDE", "DISTOCIDE"],
    "OXAMNIQUINA": ["MANSIL", "VANSIL"],
    "NICLOSAMIDA": ["YOMESAN", "TREDEMINE"],
    "NITAZOXANIDA": ["ALINIA", "CRYPTEX"],
    "QUININO": ["QUALAQUIN", "QUINAMM"],
    "CLOROQUINA": ["ARALEN", "NIVAQUINE"],
    "HIDROXICLOROQUINA": ["PLAQUENIL"],
    "PRIMAQUINA": ["PRIMAQUINE"],
    "MEFLOQUINA": ["LARIAM"],
    "ATOVAQUONA": ["MEPRON", "WELLVONE"],
    "PROGUANILA": ["PALUDRINE"],
    "PIRIMETAMINA": ["DARAPRIM"],
    "SULFADOXINA": ["FANSIDAR", "MALARIVON"],
    "ARTEMETER": ["PALuther", "COARTEM"],
    "ARTESUNATO": ["FALCIGO", "AMALATE"],
    "DIIDROARTEMISININA": ["COTEXIN", "ALAXIN"],
    "ARTEMOTIL": ["ARCOXIN", "PALMOXINE"],
    "ARTEMISININA": ["QINGHAOSU", "ARTEANNuin"],
    "LUMEFANTRINA": ["BENFLUMETOL", "COARTEM"],
    "PIPERAQUINA": ["ARTEKIN", "DUOCOTEXIN"],
    "AMODIAQUINA": ["CAMOQUIN", "FLAVOQUINE"],
    "TAFENOQUINA": ["KRINTAFEL", "ETAFENONE"],
    "GANAPLACIDE": ["GANAPAR", "HEBRON"],
    "METILENO": ["AZUL DE METILENO", "UROLENE BLUE"],
    "NITROFURANTOINA": ["MACRODANTIN", "FURADANTIN"],
    "FOSFOMICINA": ["MONUROL", "FOSMICIN"],
    "FURAZOLIDONA": ["FUROXONE", "DIAFURON"],
    "NALIDIXICO": ["NEGGRAM", "WINTOMYLON"],
    "ACIDO PIPEMIDICO": ["PIPRAM", "SELEXID"],
    "CIPROFLOXACINO": ["CIPRO", "CILOXAN"],
    "NORFLOXACINO": ["NOROXIN", "CHIBROXIN"],
    "OFLOXACINO": ["FLOXIN", "OCUFLOX"],
    "LEVOFLOXACINO": ["LEVAQUIN", "TAVANIC"],
    "MOXIFLOXACINO": ["AVELOX", "VIGAMOX"],
    "GATIFLOXACINO": ["TEQUIN", "ZYMAR"],
    "GEMIFLOXACINO": ["FACTIVE", "GAMIMYCIN"],
    "ENOXACINO": ["PENETREX", "COMPRECIN"],
    "LOMEFLOXACINO": ["MAXAQUIN", "CHIBRO-LOM"],
    "TROVAFLOXACINO": ["TROVAN", "TROVAN-TE"],
    "ALATROFLOXACINO": ["TROVAN-XR"],
    "TOSUFLOXACINO": ["OZEX", "TOSUXACIN"],
    "SPARFLOXACINO": ["ZAGAM", "TOROSPAR"],
    "TEMafloxacino": ["TEMAC", "SUPRACIN"],
    "FLEROXACINO": ["MEGALOCIN", "ROXAMONE"],
    "RUFLOXACINO": ["UROFLOX", "MONOS", "QARI"],
    "OXOLINICO": ["OXOLINIC ACID", "UTIBID"],
    "CINOXACINO": ["CINOBAC", "CINOXIB"],
    "ROSOXACINO": ["ERADACIL", "WINOXIB"],
    "CLINafloxacino": ["CLINAFLOX", "CLINACIN"],
    "DANAFLOXACINO": ["ADVOCIN", "DANOCIN"],
    "DIFLOXACINO": ["DICURAL", "A-56619"],
    "IBAFLOXACINO": ["IBAFLOX", "R-12511"],
    "MARBOfloxacino": ["ZENEQUIN", "VICTAS"],
    "ORBIFLOXACINO": ["ORBAX", "CEFAZIL"],
    "PRADOfloxacino": ["PRADOFLOX", "VERAFLOX"],
    "SARAFLOXACINO": ["SARAFLOX", "FLOXASUL"],
    "DAPSONA": ["AVLOSULFON", "DISULONE"],
    "SULFAMETOXAZOL": ["GANTANOL", "SULFAMIN"],
    "SULFADIAZINA": ["SILVADENE", "FLAMAZINE"],
    "SULFADIAZINA PRATA": ["SSD", "SILVADENE"],
    "SULFASSALAZINA": ["AZULFIDINE", "SALAZOPYRIN"],
    "SULFADOXINA": ["FANSIDAR", "MALARIVON"],
    "SULFAFURAZOL": ["SULFISUXOLE", "ENTUSS"],
    "SULFAGUANIDINA": ["SUFLGUANIDINE", "SHIGATOX"],
    "SULFAMERAZINA": ["SULFAMERAZIN", "METHYL"],
    "SULFAMETIZOL": ["SULFAMETHIZOLE", "THIOSULFIL"],
    "SULFAMETOXIPIRIDAZINA": ["LIDEPRIN", "DAZOLIN"],
    "SULFAMETOXIDIAZINA": ["SULFAMETHOXIDIAZINE", "ELCOSINE"],
    "SULFAPIRIDINA": ["SULFAPYRIDINE", "DAGENAN"],
    "SULFAQUINOXALINA": ["SULFAQUINOXALINE", "SULQIN"],
    "SULFATIAZOL": ["SULFATHIAZOLE", "CIBAZOL"],
    "SULFATIODIAZOL": ["SULFATHIODIAZOLE", "THIODIAZOL"],
    "SULFISOXAZOL": ["GANTRISIN", "SULFURIN"],
    "SULFACLORPIRIDAZINA": ["SONILYT", "AQUA-SULF"],
    "SULFADIMETOXINA": ["MADRIBON", "SULFADIMETHOXINE"],
    "SULFADIMIDINA": ["SULFADIMETHYLPYRIMIDINE", "SULFISOMIDINE"],
    "SULFAFENAZOL": ["ORISUL", "SULFAPHENAZOLE"],
    "SULFAGUANOL": ["SULFAGUANOL", "SULFAGUANIDINE"],
    "SULFALENO": ["SULFALENE", "KELFIZINA"],
    "SULFAMETOXAZOL": ["SINOMIN", "GANTANOL"],
    "SULFAMONOMETOXINA": ["SULFAMONOMETHOXINE", "SULFAMIN"],
    "SULFAPERINA": ["SULFAPERINE", "METYLAL"],
    "SULFARIT": ["SULFARITH", "SULFARIDINE"],
    "SULFASOMIZOL": ["SULFASOMIZOLE", "SULFASOMIDINE"],
    "SULFATERC": ["SULFATERC", "SULFATHIOUREA"],
    "SULFATIOMIDINA": ["SULFATHIOMIDINE", "SULFATHIAMIDINE"],
    "SULFATO DE POLIMIXINA B": ["AEROSPORIN", "POLYMYXIN B SULFATE"],
    "POLIMIXINA B": ["AEROSPORIN", "POLYMYXIN B"],
    "COLISTIMETATO": ["COLISTIMETHATE", "PROMIX"],
    "COLISTINA": ["COLISTIN", "COLIMYCIN"],
}

# ============================================================================
# FUNÇÕES UTILITÁRIAS
# ============================================================================

def log(msg, nivel="INFO"):
    """Log com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] [{nivel}] {msg}"
    print(linha)
    with open(ARQ_LOG, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')

def normalizar_texto(texto):
    """Normaliza texto: remove acentos, maiúsculas, espaços extras"""
    if not texto:
        return ""

    texto = str(texto).upper().strip()

    # Remove acentos
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) 
                   if unicodedata.category(c) != 'Mn')

    # Normaliza espaços
    texto = re.sub(r'\s+', ' ', texto)

    return texto

def similaridade_texto(s1, s2):
    """Calcula similaridade usando SequenceMatcher"""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1.upper(), s2.upper()).ratio()

# ============================================================================
# CLASSE PORTFOLIO MATCHER
# ============================================================================

class PortfolioMatcher:
    """
    Sistema de matching farmacêutico com:
    - Reconhecimento de sinônimos químicos
    - Matching de combinações (Opção B: parcial aceitável)
    - Threshold de 50%
    """

    def __init__(self, df_portfolio):
        self.df = df_portfolio.copy()
        self._preprocessar_portfolio()

    def _preprocessar_portfolio(self):
        """Pré-processa o portfólio para matching eficiente"""
        # Normalizar fármacos
        self.df['FARMACO_NORM'] = self.df['Fármaco'].apply(self._normalizar_farmaco)

        # Identificar combinações
        self.df['IS_COMBO'] = self.df['Fármaco'].apply(self._identificar_combinacao)
        self.df['COMPONENTES'] = self.df['Fármaco'].apply(self._extrair_componentes)

        # Extrair concentrações
        self.df['DOSAGEM_NORM'] = self.df.apply(
            lambda row: self._extrair_concentracao(row['Descrição'], row['Dosagem']), 
            axis=1
        )

    def _normalizar_farmaco(self, farmaco):
        """Normaliza nome do fármaco"""
        if pd.isna(farmaco):
            return ""

        farmaco = str(farmaco).upper().strip()

        # Abreviações comuns
        subs = {
            'CLAV.': 'CLAVULANATO', 'CLAV ': 'CLAVULANATO ',
            'FOSF.': 'FOSFATO', 'FOSF ': 'FOSFATO ',
            'SOD.': 'SODICO', 'SOD ': 'SODICO ',
            'POT.': 'POTASSIO', 'POT ': 'POTASSIO ',
            'CAP.': 'CAPSULA', 'CAP ': 'CAPSULA ',
            'CPR.': 'COMPRIMIDO', 'CPR ': 'COMPRIMIDO ',
            'COMP.': 'COMPRIMIDO', 'COMP ': 'COMPRIMIDO ',
            'AMP.': 'AMPOLA', 'AMP ': 'AMPOLA ',
            'FR.': 'FRASCO', 'FR ': 'FRASCO ',
            'SOL.': 'SOLUCAO', 'SOL ': 'SOLUCAO ',
            'CREM.': 'CREME', 'CREM ': 'CREME ',
            'POM.': 'POMADA', 'POM ': 'POMADA ',
            'GTS.': 'GOTAS', 'GTS ': 'GOTAS ',
            'XPE.': 'XAROPE', 'XPE ': 'XAROPE ',
            'SUSP.': 'SUSPENSAO', 'SUSP ': 'SUSPENSAO ',
            'INJ.': 'INJETAVEL', 'INJ ': 'INJETAVEL ',
            'COMB.': 'COMBINACAO', 'COMB ': 'COMBINACAO ',
        }

        for antigo, novo in subs.items():
            farmaco = farmaco.replace(antigo, novo)

        return farmaco

    def _identificar_combinacao(self, farmaco):
        """Identifica se é uma combinação (tem +)"""
        if pd.isna(farmaco):
            return False
        return '+' in str(farmaco) or ' E ' in str(farmaco).upper()

    def _extrair_componentes(self, farmaco):
        """Extrai lista de componentes do fármaco"""
        if pd.isna(farmaco):
            return []

        farmaco_str = str(farmaco).upper()
        separadores = ['+', ' E ', '/']

        for sep in separadores:
            if sep in farmaco_str:
                comps = [c.strip() for c in farmaco_str.split(sep)]
                return [c for c in comps if len(c) > 2]

        return [farmaco_str] if len(farmaco_str) > 2 else []

    def _extrair_concentracao(self, descricao, dosagem):
        """Extrai concentração da descrição ou campo dosagem"""
        if pd.notna(dosagem) and str(dosagem).strip():
            return normalizar_texto(dosagem)

        if pd.isna(descricao):
            return ""

        # Padrões de concentração
        padroes = [
            r'(\d+[,.]?\d*)\s*(MG|ML|G|UI|MCG|MG/ML|UNIDADES|%)',
            r'(\d+[,.]?\d*)\s*(?:MG|ML|G)\b'
        ]

        for padrao in padroes:
            match = re.search(padrao, str(descricao).upper())
            if match:
                return normalizar_texto(match.group(0))

        return ""

    def _match_componente(self, comp_edital, comp_portfolio):
        """Match de componente com sinônimos"""
        comp_edit = normalizar_texto(comp_edital)
        comp_port = normalizar_texto(comp_portfolio)

        if not comp_edit or not comp_port:
            return 0.0

        # Match exato
        if comp_edit == comp_port:
            return 1.0

        # Match via sinônimos
        for principal, sinonimos in SINONIMOS_FARMACOS.items():
            todos = [principal] + sinonimos
            if any(comp_edit == s for s in todos) and any(comp_port == s for s in todos):
                return 1.0

        # Similaridade parcial
        sim = similaridade_texto(comp_edit, comp_port)

        # Se um contém o outro
        if comp_edit in comp_port or comp_port in comp_edit:
            sim = max(sim, 0.7)

        return sim

    def _extrair_componentes_edital(self, descricao):
        """Extrai componentes de descrição do edital"""
        desc = normalizar_texto(descricao)
        if not desc:
            return []

        # Limpa termos técnicos comuns
        desc = re.sub(r'(CONCENTRACAO|DOSAGEM|FORMA FARMACEUTICA|VIA|APRESENTACAO|EMBALAGEM|CONTEM|COM)\s*', ' ', desc)

        componentes = []

        # Separar por +
        if '+' in desc:
            partes = desc.split('+')
        else:
            partes = [desc]

        for parte in partes:
            # Remove concentrações e números
            parte = re.sub(r'\d+[,.]?\d*\s*(MG|ML|G|UI|MCG|MG/ML|%|UN).*', '', parte)
            parte = parte.strip()

            if len(parte) > 3:
                componentes.append(parte)

        return componentes

    def match(self, descricao_edital, concentracao_edital=None, forma_edital=None):
        """
        Faz matching de um item do edital contra o portfólio

        Returns:
            Lista de dicts com matches ordenados por score (decrescente)
        """
        resultados = []

        comps_edital = self._extrair_componentes_edital(descricao_edital)
        num_comps_edital = len(comps_edital)

        for idx, row in self.df.iterrows():
            score = 0.0
            detalhes = {}

            comps_portfolio = row['COMPONENTES']
            if not comps_portfolio:
                continue

            num_comps_portfolio = len(comps_portfolio)

            # 1. MATCH DE COMPONENTES (60% do score)
            if comps_edital and comps_portfolio:
                matches = []
                for comp_port in comps_portfolio:
                    melhor_match = max(
                        self._match_componente(ce, comp_port) 
                        for ce in comps_edital
                    ) if comps_edital else 0.0
                    matches.append(melhor_match)

                score_comp = sum(matches) / len(matches)
                cobertura = sum(1 for m in matches if m > 0.7) / len(matches)

                # Bônus para combinações completas
                if num_comps_edital >= 2 and num_comps_portfolio >= 2 and cobertura == 1.0:
                    score_comp = min(1.0, score_comp * 1.15)  # +15%
                    detalhes['tipo'] = 'COMBINACAO_COMPLETA'
                elif num_comps_portfolio == 1:
                    detalhes['tipo'] = 'SIMPLES'
                else:
                    detalhes['tipo'] = 'PARCIAL'

                score += (score_comp * 0.7 + cobertura * 0.3) * 0.60
                detalhes['componentes_score'] = f"{score_comp:.2f}"
                detalhes['cobertura'] = f"{cobertura:.0%}"

            # 2. MATCH DE CONCENTRAÇÃO (25% do score)
            if concentracao_edital and row['DOSAGEM_NORM']:
                conc_edit = normalizar_texto(concentracao_edital)
                conc_port = row['DOSAGEM_NORM']

                # Remove espaços para comparar
                conc_edit_clean = re.sub(r'\s+', '', conc_edit)
                conc_port_clean = re.sub(r'\s+', '', conc_port)

                if conc_edit_clean == conc_port_clean:
                    sim_conc = 1.0
                else:
                    sim_conc = similaridade_texto(conc_edit, conc_port)
                    # Verificar se uma contém a outra
                    if conc_edit_clean in conc_port_clean or conc_port_clean in conc_edit_clean:
                        sim_conc = max(sim_conc, 0.6)

                score += sim_conc * 0.25
                detalhes['concentracao_score'] = f"{sim_conc:.2f}"
            else:
                score += 0.15  # Neutro
                detalhes['concentracao_score'] = "N/A"

            # 3. MATCH DE FORMA FARMACÊUTICA (15% do score)
            if forma_edital and pd.notna(row['Forma Farmacêutica']):
                forma_edit = normalizar_texto(forma_edital)
                forma_port = normalizar_texto(row['Forma Farmacêutica'])

                sim_forma = similaridade_texto(forma_edit, forma_port)

                if forma_edit in forma_port or forma_port in forma_edit:
                    sim_forma = max(sim_forma, 0.8)

                score += sim_forma * 0.15
                detalhes['forma_score'] = f"{sim_forma:.2f}"
            else:
                score += 0.075  # Neutro
                detalhes['forma_score'] = "N/A"

            # Threshold de 50%
            if score >= 0.50:
                resultados.append({
                    'idx': idx,
                    'score': score,
                    'item_portfolio': row,
                    'detalhes': detalhes
                })

        # Ordenar por score decrescente
        resultados.sort(key=lambda x: x['score'], reverse=True)
        return resultados

# ============================================================================
# FUNÇÕES DE CARREGAMENTO
# ============================================================================

def carregar_portfolio(csv_path=ARQ_PORTFOLIO):
    """Carrega portfólio do CSV"""
    log(f"Carregando portfólio de {csv_path}...")

    if not os.path.exists(csv_path):
        log(f"ERRO: Arquivo {csv_path} não encontrado!", "ERRO")
        return None

    try:
        # Detecta encoding
        encodings = ['utf-8-sig', 'latin1', 'iso-8859-1', 'cp1252']
        df = None

        for enc in encodings:
            try:
                df = pd.read_csv(csv_path, encoding=enc)
                break
            except:
                continue

        if df is None:
            log("ERRO: Não foi possível ler o CSV", "ERRO")
            return None

        log(f"✅ Portfólio carregado: {len(df)} itens")

        return df

    except Exception as e:
        log(f"ERRO ao carregar portfólio: {e}", "ERRO")
        return None

def carregar_licitacoes(json_path=ARQ_LICITACOES):
    """Carrega licitações do JSON comprimido (mantém ordem original)"""
    log(f"Carregando licitações de {json_path}...")

    if not os.path.exists(json_path):
        log(f"ERRO: Arquivo {json_path} não encontrado!", "ERRO")
        return None

    try:
        with gzip.open(json_path, 'rt', encoding='utf-8') as f:
            dados = json.load(f)

        # Garante que é lista
        if isinstance(dados, dict):
            licitacoes = []
            for orgao, editais in dados.items():
                for edital, info in editais.items():
                    info['orgao'] = orgao
                    info['edital'] = edital
                    info['_index_original'] = len(licitacoes)
                    licitacoes.append(info)
        else:
            for idx, item in enumerate(dados):
                item['_index_original'] = idx
            licitacoes = dados

        log(f"✅ {len(licitacoes)} licitações carregadas")
        return licitacoes

    except Exception as e:
        log(f"ERRO ao carregar licitações: {e}", "ERRO")
        return None

# ============================================================================
# FUNÇÃO DE AVALIAÇÃO
# ============================================================================

def avaliar_licitacao(licitacao, matcher):
    """Avalia uma licitação contra o portfólio"""
    try:
        # Extrai dados
        objeto = str(licitacao.get('obj', '') or licitacao.get('objeto', ''))
        edital_id = licitacao.get('edital', licitacao.get('id', 'unknown'))
        orgao = licitacao.get('orgao', licitacao.get('org', 'N/A'))

        # Extrai itens
        itens = licitacao.get('itens', [])
        if not isinstance(itens, list):
            itens = []

        # Se não tiver itens, usa objeto como fallback
        if not itens:
            itens = [{'descricao': objeto, 'concentracao': None, 'forma': None}]

        # Avalia cada item
        melhores_matches = []

        for item in itens:
            if isinstance(item, dict):
                desc = item.get('d', '') or item.get('descricao', '')
                conc = item.get('concentracao')
                forma = item.get('forma') or item.get('forma_farmaceutica')
            else:
                desc = str(item)
                conc = None
                forma = None

            if not desc:
                continue

            matches = matcher.match(desc, conc, forma)

            if matches:
                melhor = matches[0]
                melhores_matches.append({
                    'item_descricao': desc,
                    'score': melhor['score'],
                    'portfolio_descricao': melhor['item_portfolio']['Descrição'],
                    'tipo': melhor['detalhes'].get('tipo', 'SIMPLES'),
                    'detalhes': melhor['detalhes']
                })

        if not melhores_matches:
            return {
                'id': edital_id,
                'orgao': orgao,
                'objeto': objeto[:200],
                'percentual': 0.0,
                'confianca': 'INCOMPATIVEL',
                'matches': [],
                'total_itens': len(itens),
                'itens_compativeis': 0,
                '_index_original': licitacao.get('_index_original', 0)
            }

        # Calcula score agregado da licitação
        scores = [m['score'] for m in melhores_matches]
        score_medio = sum(scores) / len(scores)

        # Bônus se mais da metade dos itens são compatíveis
        taxa_compatibilidade = len(melhores_matches) / len(itens) if itens else 0
        if taxa_compatibilidade >= 0.5:
            score_final = min(1.0, score_medio * (1 + 0.1 * taxa_compatibilidade))
        else:
            score_final = score_medio

        # Converte para percentual
        percentual = round(score_final * 100, 2)

        # Classifica
        if score_final >= THRESHOLD_ALTO:
            confianca = 'ALTA'
        elif score_final >= THRESHOLD_MEDIO:
            confianca = 'MEDIA'
        elif score_final >= THRESHOLD_BAIXO:
            confianca = 'BAIXA'
        else:
            confianca = 'INCOMPATIVEL'

        # Prepara lista de matches para relatório
        matches_str = []
        for m in melhores_matches[:5]:  # Top 5
            matches_str.append(f"{m['portfolio_descricao'][:30]}... ({m['score']:.0%})")

        return {
            'id': edital_id,
            'orgao': orgao,
            'objeto': objeto[:200],
            'percentual': percentual,
            'confianca': confianca,
            'matches': matches_str,
            'total_itens': len(itens),
            'itens_compativeis': len(melhores_matches),
            'score_detalhado': score_final,
            '_index_original': licitacao.get('_index_original', 0)
        }

    except Exception as e:
        log(f"Erro ao avaliar licitação {licitacao.get('id', 'unknown')}: {e}", "ERRO")
        return {
            'id': licitacao.get('id', 'unknown'),
            'percentual': 0.0,
            'confianca': 'ERRO',
            'erro': str(e),
            '_index_original': licitacao.get('_index_original', 0)
        }

def avaliar_todas_licitacoes(licitacoes, matcher):
    """Avalia todas as licitações em paralelo"""
    log(f"🔍 Avaliando {len(licitacoes)} licitações...")

    resultados = []
    processadas = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(avaliar_licitacao, lic, matcher): lic 
            for lic in licitacoes
        }

        for future in as_completed(futures):
            resultado = future.result()
            resultados.append(resultado)
            processadas += 1

            if processadas % 100 == 0:
                log(f"Processadas {processadas}/{len(licitacoes)}...")

    # Ordena pelo índice original (mantém ordem das licitações)
    resultados.sort(key=lambda x: x['_index_original'])

    return resultados

# ============================================================================
# GERAÇÃO DE RELATÓRIO
# ============================================================================

def gerar_relatorio(resultados, origem="MANUAL"):
    """Gera relatório CSV com resultados"""
    log("="*60)
    log(f"📊 GERANDO RELATÓRIO [{origem}]")
    log("="*60)

    # Prepara dados para CSV
    with open(ARQ_SAIDA, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        # Header
        writer.writerow([
            'id', 'orgao', 'objeto_licitacao', 'percentual', 'confianca',
            'total_itens', 'itens_compativeis', 'principais_matches'
        ])

        # Dados
        for r in resultados:
            matches_str = '|'.join(r.get('matches', []))[:500]
            writer.writerow([
                r['id'],
                r.get('orgao', ''),
                r.get('objeto', ''),
                r['percentual'],
                r['confianca'],
                r.get('total_itens', 0),
                r.get('itens_compativeis', 0),
                matches_str
            ])

    # Estatísticas
    total = len(resultados)
    alta = len([r for r in resultados if r['confianca'] == 'ALTA'])
    media = len([r for r in resultados if r['confianca'] == 'MEDIA'])
    baixa = len([r for r in resultados if r['confianca'] == 'BAIXA'])
    incomp = len([r for r in resultados if r['confianca'] == 'INCOMPATIVEL'])

    log(f"📁 Arquivo: {ARQ_SAIDA}")
    log(f"📊 Total: {total}")
    log(f"   🟢 ALTA (≥{THRESHOLD_ALTO:.0%}): {alta}")
    log(f"   🟡 MÉDIA ({THRESHOLD_MEDIO:.0%}-{THRESHOLD_ALTO:.0%}): {media}")
    log(f"   🔴 BAIXA ({THRESHOLD_BAIXO:.0%}-{THRESHOLD_MEDIO:.0%}): {baixa}")
    log(f"   ⚪ INCOMPATÍVEL (<{THRESHOLD_BAIXO:.0%}): {incomp}")

    return ARQ_SAIDA

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Função principal"""

    # Detecta origem
    origem = "MANUAL"
    if len(sys.argv) > 1:
        origem = sys.argv[1].upper()

    # Limpa log anterior
    if os.path.exists(ARQ_LOG):
        os.remove(ARQ_LOG)

    log("="*70)
    log(f"🤖 MATCHER FARMACÊUTICO v1.0 [SINÔNIMOS + COMBINAÇÕES] [{origem}]")
    log(f"📊 THRESHOLDS: ≥{THRESHOLD_ALTO:.0%} ALTO | ≥{THRESHOLD_MEDIO:.0%} MÉDIO | ≥{THRESHOLD_BAIXO:.0%} BAIXA")
    log("="*70)

    # Carrega portfólio
    df_portfolio = carregar_portfolio()
    if df_portfolio is None:
        log("❌ Falha ao carregar portfólio. Abortando.", "ERRO")
        return 1

    # Inicializa matcher
    log("🧠 Inicializando motor de matching...")
    matcher = PortfolioMatcher(df_portfolio)
    log(f"✅ Matcher pronto: {len(matcher.df)} itens indexados")

    # Carrega licitações
    licitacoes = carregar_licitacoes()
    if licitacoes is None:
        log("❌ Falha ao carregar licitações. Abortando.", "ERRO")
        return 1

    # Avalia
    resultados = avaliar_todas_licitacoes(licitacoes, matcher)

    # Gera relatório
    gerar_relatorio(resultados, origem)

    log(f"✅ Matcher [{origem}] concluído!")
    return 0

if __name__ == "__main__":
    exit(main())
