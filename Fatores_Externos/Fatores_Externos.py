# -*- coding: utf-8 -*-
# Versão: 0.1.4.1.0

import requests
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging # Importa a biblioteca de logging
import os
from pathlib import Path
import io
import importlib.util # Import necessário
import time # Para cache busting

# Tenta importar pkg_resources, mas continua se falhar (apenas para log de versão)
try:
    import pkg_resources
    PKG_RESOURCES_AVAILABLE = True
except ImportError:
    PKG_RESOURCES_AVAILABLE = False
    print("AVISO: Biblioteca 'setuptools' não encontrada. Não será possível logar versões.")


# --- Bibliotecas Específicas ---
try:
    import bcb
    import ipeadatapy as ipea # Importa o módulo principal
    import quandl
    import openmeteo_requests
    import cdsapi
    import ee
    import lxml # Necessário para pd.read_xml
    import pyarrow # Necessário para df.to_parquet
    import setuptools # Necessário para pkg_resources
except ImportError as import_err:
    print(f"ERRO CRÍTICO: Biblioteca não instalada ({import_err}). Verifique seu requirements.txt "
          "e execute 'pip install -r requirements.txt'")
    sys.exit(1)

# --- Constantes ---
SCRIPT_VERSION = "0.1.4.1.0"
SCRIPT_VERSION_DESC = """
- Foco em corrigir a regressão do BCB e investigar erro 404 do INMET.
- Revertida a lógica de formato de data no `fetch_macro_bcb` (YYYY-MM-DD primeiro).
- Modificada a função `fetch_clima_inmet` para:
    - Testar estação A601 (Rio de Janeiro) em vez de A701.
    - Remover a tentativa de URL alternativa.
- Mantidas as tentativas anteriores para IPEA e CEPEA.
"""

# --- Resolve o problema de caminho ---
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR)) # Permite que 'import logins' funcione

# --- Configuração Global de Logging ---
log_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log_file_path = SCRIPT_DIR / "log.txt"
versionamento_path = SCRIPT_DIR / "versionamento.txt"

# Configura o logger raiz
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Define o nível mínimo de log (INFO, WARNING, ERROR)
# logger.setLevel(logging.DEBUG) # Descomente para logs MUITO detalhados

# Limpa handlers existentes
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
    handler.close() # Fecha o handler anterior

# Handler para o terminal (Console)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# Handler para o arquivo log.txt
try:
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8') # 'w' sobrescreve
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    logging.info(f"Logging configurado. Saída no terminal e salva em: {log_file_path.name}")
except Exception as e:
    logging.error(f"Falha ao configurar o logging para arquivo '{log_file_path.name}': {e}")

logging.info("="*70)
logging.info(f"INICIANDO SCRIPT DE COLETA DE FATORES EXTERNOS - v{SCRIPT_VERSION}")
logging.info("="*70)

# --- Lógica de Versionamento ---
def update_version_log():
    """Adiciona a versão atual ao arquivo versionamento.txt se não existir."""
    try:
        version_header = f"Versão {SCRIPT_VERSION} - {datetime.now().strftime('%Y-%m-%d')}"
        full_entry = (
            f"\n{'='*70}\n"
            f"{version_header}\n"
            f"{SCRIPT_VERSION_DESC.strip()}\n"
            f"{'='*70}\n"
        )
        content = ""
        if versionamento_path.exists():
            with open(versionamento_path, 'r', encoding='utf-8') as f:
                content = f.read()
        if version_header not in content:
            with open(versionamento_path, 'w', encoding='utf-8') as f:
                f.write(full_entry.strip() + "\n" + content) # Adiciona no topo
            logging.info(f"Log de versão adicionado a '{versionamento_path.name}'")
        else:
            logging.info(f"Versão {SCRIPT_VERSION} já registrada em '{versionamento_path.name}'.")
    except Exception as e:
        logging.error(f"Falha ao atualizar '{versionamento_path.name}': {e}")

update_version_log() # Chama a função de versionamento

# Diretório para salvar os dados (dentro da pasta do script)
DATA_DIR = SCRIPT_DIR / "dados_coletados"
DATA_DIR.mkdir(exist_ok=True) # Cria o diretório se não existir

# --- Definição do Período e Área de Interesse ---
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=3*365)
START_DATE_STR = START_DATE.strftime('%Y-%m-%d')
END_DATE_STR = END_DATE.strftime('%Y-%m-%d')
logging.info(f"Período de análise definido: {START_DATE_STR} a {END_DATE_STR}")

# Área de Interesse (AOI) para dados geoespaciais
AOI_MATO_GROSSO = None # Será inicializado após o GEE

# --- Lista de Fontes para Checklist ---
SOURCES_TO_CHECK = [
    'macro_bcb', 'macro_ipea', 'macro_cepea', 'macro_quandl', # Macro
    'clima_inmet', 'clima_chirps_gee', 'clima_era5',          # Clima
    'satelite_modis_ndvi',                                    # Satelite
    'hidro_ana'                                               # Hidrologia
]
collection_status = {source: "⏳ Pendente" for source in SOURCES_TO_CHECK}

# --- Funções de Verificação e Criação de Credenciais ---

def check_and_setup_credentials():
    """Verifica e configura credenciais e APIs."""
    logging.info("\n" + "-"*70)
    logging.info("ETAPA 0: VERIFICANDO CREDENCIAIS e APIs")
    logging.info("-"*70)
    global logins

    # 1. Importar logins.py
    logins_path = SCRIPT_DIR / "logins.py"
    logging.info(f"Procurando arquivo de logins: '{logins_path.name}'")
    if not logins_path.exists():
        logging.error(f"❌ Arquivo '{logins_path.name}' não encontrado. Crie-o com as chaves API.")
        sys.exit(1)
    try:
        spec = importlib.util.spec_from_file_location("logins", logins_path)
        logins = importlib.util.module_from_spec(spec)
        sys.modules['logins'] = logins
        spec.loader.exec_module(logins)
        logging.info(f"✅ Arquivo '{logins_path.name}' importado.")
    except Exception as e:
        logging.error(f"❌ Não foi possível importar '{logins_path.name}': {e}")
        sys.exit(1)

    # 2. Verificar chaves
    chave_quandl_ok = hasattr(logins, 'QUANDL_API_KEY') and "COLE_SUA_CHAVE" not in logins.QUANDL_API_KEY
    chave_cds_ok = hasattr(logins, 'CDS_API_KEY') and "COLE_SUA_CHAVE" not in logins.CDS_API_KEY
    if not chave_quandl_ok or not chave_cds_ok:
        logging.error(f"❌ Chaves placeholder em '{logins_path.name}'. Edite o arquivo.")
        sys.exit(1)
    else:
        logging.info("✅ Chaves API (Quandl, CDS) preenchidas.")
    if not hasattr(logins, 'CDS_API_URL') or not logins.CDS_API_URL:
         logins.CDS_API_URL = "https://cds.climate.copernicus.eu/api"

    # 3. Verificar/Criar .cdsapirc
    cds_api_rc_path = Path.home() / ".cdsapirc"
    logging.info(f"Verificando arquivo .cdsapirc em: {cds_api_rc_path}")
    if not cds_api_rc_path.exists():
        logging.warning("   Arquivo .cdsapirc não encontrado. Criando...")
        try:
            file_content = f"url: {logins.CDS_API_URL}\nkey: {logins.CDS_API_KEY}"
            with open(cds_api_rc_path, 'w') as f: f.write(file_content)
            logging.info(f"   ✅ Arquivo .cdsapirc criado.")
        except Exception as e:
            logging.error(f"   ❌ Falha ao criar .cdsapirc: {e}")
            sys.exit(1)
    else:
        logging.info("✅ Arquivo .cdsapirc encontrado.")

    # 4. Inicializar GEE
    global AOI_MATO_GROSSO
    logging.info("Inicializando Google Earth Engine (GEE)...")
    try:
        # Tenta inicializar sem projeto explícito primeiro (usa o padrão configurado via `earthengine set_project`)
        ee.Initialize()
        # Se funcionar sem erro de projeto, tenta usar o projeto padrão
        cloud_project = ee.data._cloud_api_user_project # Tenta pegar o projeto padrão
        if not cloud_project:
             logging.warning("   Projeto GEE padrão não encontrado. Tentando projeto explícito 'quantum-minds-475514'...")
             ee.Initialize(project='quantum-minds-475514')
        AOI_MATO_GROSSO = ee.Geometry.Rectangle([-58, -16, -54, -12]) # Exemplo MT
        logging.info("✅ GEE inicializado com sucesso.")
    except Exception as e:
        # Se a inicialização padrão falhar, tenta com o projeto explícito
        try:
             logging.warning(f"   Inicialização GEE padrão falhou ({e}). Tentando projeto explícito 'quantum-minds-475514'...")
             ee.Initialize(project='quantum-minds-475514')
             AOI_MATO_GROSSO = ee.Geometry.Rectangle([-58, -16, -54, -12])
             logging.info("✅ GEE inicializado com sucesso (projeto explícito).")
        except Exception as e_explicit:
             logging.error(f"❌ Falha ao inicializar GEE (padrão e explícito): {e_explicit}.")
             logging.error("   Verifique autenticação ('earthengine authenticate'), projeto ('earthengine set_project') e registro do projeto no GEE.")
             sys.exit(1)


    # 5. Logar versões
    if PKG_RESOURCES_AVAILABLE:
        try:
            ipea_version = pkg_resources.get_distribution("ipeadatapy").version
            logging.info(f"Versão ipeadatapy: {ipea_version}")
            bcb_version = pkg_resources.get_distribution("python-bcb").version
            logging.info(f"Versão python-bcb: {bcb_version}")
        except Exception as e:
            logging.warning(f"Erro ao obter versões das bibliotecas: {e}")

    logging.info("-"*70)
    return logins

# --- Função de Salvamento ---
def save_data(df, name):
    """Salva um DataFrame no diretório de dados em formato Parquet."""
    global collection_status
    if df is not None and not df.empty:
        filename = DATA_DIR / f"{name}.parquet"
        try:
            df.to_parquet(filename)
            collection_status[name] = f"✅ Sucesso ({len(df):,} regs)".replace(",",".")
        except Exception as e:
            logging.error(f"❌ Falha ao salvar Parquet para '{name}': {e}")
            collection_status[name] = "❌ Falha ao Salvar"
    else:
        current_status = collection_status.get(name,"⏳ Pendente")
        if current_status == "⏳ Coletando..." or current_status == "⏳ Pendente":
             collection_status[name] = "❌ Falha (Dados Vazios/Erro)"

# --- 💲 Categoria: Macro e Commodities ---

def fetch_macro_bcb():
    """Busca Câmbio e Selic do Banco Central (SGS)."""
    source_name = 'macro_bcb'
    logging.info(f"  Iniciando: {source_name} (Câmbio e Selic)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    df = pd.DataFrame()
    try:
        from bcb import sgs
        # CORREÇÃO v0.1.4.1.0: Tentar YYYY-MM-DD primeiro, conforme erro original
        start_date_fmt = START_DATE_STR
        end_date_fmt = END_DATE_STR
        logging.debug(f"   BCB com datas (YYYY-MM-DD): {start_date_fmt} a {end_date_fmt}")
        df = sgs.get({'USD_BRL': 1, 'Selic_Meta': 432},
                       start=start_date_fmt,
                       end=end_date_fmt)
        logging.info(f"  ✅ {source_name}: Coleta OK.")

    except ValueError as ve:
        # Se YYYY-MM-DD falhar, tenta DD/MM/YYYY (que funcionou antes)
        logging.warning(f"  ⚠️ {source_name}: Formato YYYY-MM-DD falhou ({ve}). Tentando DD/MM/YYYY...")
        try:
            from bcb import sgs
            start_date_fmt_alt = START_DATE.strftime('%d/%m/%Y')
            end_date_fmt_alt = END_DATE.strftime('%d/%m/%Y')
            logging.debug(f"   BCB com datas (DD/MM/YYYY): {start_date_fmt_alt} a {end_date_fmt_alt}")
            df = sgs.get({'USD_BRL': 1, 'Selic_Meta': 432},
                           start=start_date_fmt_alt,
                           end=end_date_fmt_alt)
            logging.info(f"  ✅ {source_name}: Coleta OK.")
        except Exception as e_alt:
             logging.error(f"  ❌ Falha em {source_name} (Alternativa DD/MM/YYYY também falhou): {e_alt}")
             collection_status[source_name] = "❌ Falha (Erro Formato Data)"
    except Exception as e:
        logging.error(f"  ❌ Falha inesperada em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro API)"

    if df.empty and collection_status[source_name] == "⏳ Coletando...":
        logging.warning(f"  ⚠️ {source_name}: Nenhum dado retornado.")
        collection_status[source_name] = "❌ Falha (Dados Vazios)"
    return df

def fetch_macro_ipea():
    """Busca séries econômicas do IPEAdata."""
    source_name = 'macro_ipea'
    logging.info(f"  Iniciando: {source_name} (IPCA)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    df = pd.DataFrame()
    ipea_func = None # Variável para guardar a função correta
    try:
        # TENTAÇÃO FINAL: Importar 'get' DIRETAMENTE
        logging.debug("   Tentando importar 'get' diretamente de ipeadatapy...")
        try:
            from ipeadatapy import get as ipea_get_func_direct
            logging.debug("    'from ipeadatapy import get' FUNCIONOU.")
            ipea_func = ipea_get_func_direct
        except ImportError:
            logging.warning("   'from ipeadatapy import get' FALHOU.")
            # Se falhar, tenta via ipea.get (como antes)
            logging.debug("   Tentando via 'ipeadatapy.get'...")
            if hasattr(ipea, 'get'):
                logging.debug("    'ipeadatapy.get' existe.")
                ipea_func = ipea.get
            elif hasattr(ipea, 'Serie'):
                 logging.warning("   'ipeadatapy.get' também não existe. Tentando 'ipeadatapy.Serie'...")
                 ipea_func = "Serie" # Marca para usar o método Serie
            else:
                logging.error(f"  ❌ Erro {source_name}: Nem 'get' nem 'Serie' disponíveis após tentativas.")
                collection_status[source_name] = "❌ Falha (Biblioteca?)"
                return pd.DataFrame()

        # Chama a função correta
        if ipea_func == "Serie":
             s = ipea.Serie("PRECOS12_IPCAG12")
             df_ipca = s.as_dataframe(start=START_DATE_STR, end=END_DATE_STR)
        elif callable(ipea_func): # Verifica se é uma função chamável
             df_ipca = ipea_func(series_code="PRECOS12_IPCAG12",
                               start_date=START_DATE_STR,
                               end_date=END_DATE_STR)
        else: # Se chegou aqui, algo deu muito errado
             logging.error(f"  ❌ Erro {source_name}: Não foi possível determinar a função de busca.")
             collection_status[source_name] = "❌ Falha (Biblioteca?)"
             return pd.DataFrame()


        if df_ipca is None or df_ipca.empty:
             logging.warning(f"  ⚠️ {source_name}: Nenhum dado retornado.")
             collection_status[source_name] = "❌ Falha (Dados Vazios)"
             return pd.DataFrame()

        # Renomeia coluna
        if 'VALUE (R$)' in df_ipca.columns:
            df = df_ipca[['VALUE (R$)']].rename(columns={'VALUE (R$)': 'IPCA_Mensal'})
        elif 'value' in df_ipca.columns:
             df = df_ipca[['value']].rename(columns={'value': 'IPCA_Mensal'})
        else:
             logging.error(f"  ❌ Erro {source_name}: Coluna de valor não encontrada ({df_ipca.columns}).")
             collection_status[source_name] = "❌ Falha (Formato Inesperado)"
             return pd.DataFrame()

        logging.info(f"  ✅ {source_name}: Coleta OK.")
        return df
    except Exception as e:
        logging.error(f"  ❌ Erro inesperado em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro API)"
        return pd.DataFrame()

def fetch_macro_cepea():
    """Busca indicadores de preço de commodities do CEPEA (Esalq/USP)."""
    source_name = 'macro_cepea'
    logging.info(f"  Iniciando: {source_name} (Milho)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    cache_buster = int(time.time())
    url = (
        f"https://www.cepea.esalq.usp.br/api/series/id/104?"
        f"start_date={START_DATE_STR}&end_date={END_DATE_STR}&currency=BRL&_={cache_buster}"
    )
    headers = {'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}
    logging.debug(f"   URL {source_name}: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=20)
        logging.debug(f"   {source_name} Status Code: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        if not data.get('series'):
             logging.warning(f"  ⚠️ {source_name}: Nenhum dado ('series') retornado.")
             collection_status[source_name] = "❌ Falha (Dados Vazios)"
             return pd.DataFrame()

        df = pd.DataFrame(data['series'])
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df = df.set_index('date')[['price_brl']].rename(columns={'price_brl': 'Milho_CEPEA_BRL'})
        logging.info(f"  ✅ {source_name}: Coleta OK.")
        return df
    except requests.exceptions.HTTPError as he:
        logging.error(f"  ❌ Erro HTTP em {source_name}: {he}")
        logging.error(f"     URL que falhou (requisição final): {he.request.url}")
        collection_status[source_name] = f"❌ Falha ({he.response.status_code})"
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"  ❌ Erro inesperado em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro API)"
        return pd.DataFrame()

def fetch_macro_quandl(api_key):
    """Busca preços futuros internacionais do Quandl (CME/CBOT)."""
    source_name = 'macro_quandl'
    logging.info(f"  Iniciando: {source_name} (Soja Futuro)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    try:
        quandl.ApiConfig.api_key = api_key
        df = quandl.get("CHRIS/CME_S1", start_date=START_DATE_STR, end_date=END_DATE_STR)
        if df.empty:
            logging.warning(f"  ⚠️ {source_name}: Nenhum dado retornado.")
            collection_status[source_name] = "❌ Falha (Dados Vazios)"
            return pd.DataFrame()
        df = df[['Settle']].rename(columns={'Settle': 'Soja_Futuro_CME_USD'})
        logging.info(f"  ✅ {source_name}: Coleta OK.")
        return df
    except Exception as e:
        logging.error(f"  ❌ Erro em {source_name}: {e}")
        if "403" in str(e):
             logging.warning("     Lembrete: Verifique inscrição no dataset 'CHRIS/CME_S1'.")
             collection_status[source_name] = "❌ Falha (Permissão 403)"
        else:
             collection_status[source_name] = "❌ Falha (Erro API)"
        return pd.DataFrame()

# --- 🌦️ Categoria: Clima e Geografia ---

def fetch_clima_inmet():
    """Busca dados de estações meteorológicas do INMET."""
    source_name = 'clima_inmet'
    # TENTATIVA FINAL: Usando A601 (Rio) - se falhar, API pode estar instável/mudou
    station_id_to_try = "A601"
    logging.info(f"  Iniciando: {source_name} (Estação {station_id_to_try})...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    cache_buster = int(time.time())
    url = (
        f"https://apitempo.inmet.gov.br/estacoes/diaria/"
        f"{START_DATE_STR}/{END_DATE_STR}/{station_id_to_try}?_={cache_buster}"
    )
    logging.debug(f"   URL {source_name}: {url}")
    headers = {'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}
    try:
        response = requests.get(url, headers=headers, timeout=20)
        logging.debug(f"   {source_name} Status Code: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        if not data:
             logging.warning(f"  ⚠️ Nenhum dado retornado do {source_name} para {station_id_to_try}.")
             collection_status[source_name] = "❌ Falha (Dados Vazios)"
             return pd.DataFrame()

        df = pd.DataFrame(data)
        if 'DT_MEDICAO' not in df.columns:
             logging.error(f"  ❌ Coluna 'DT_MEDICAO' não encontrada nos dados do {source_name}.")
             collection_status[source_name] = "❌ Falha (Formato Inesperado)"
             return pd.DataFrame()

        df['data'] = pd.to_datetime(df['DT_MEDICAO'])
        df = df.set_index('data')
        cols = {'CHUVA': 'Prec_INMET_mm', 'TEMP_MAX': 'TempMax_INMET_C'}
        df = df.reindex(columns=list(cols.keys())).rename(columns=cols)
        df = df.apply(pd.to_numeric, errors='coerce').dropna(how='all')
        logging.info(f"  ✅ {source_name}: Coleta OK.")
        return df
    except requests.exceptions.HTTPError as he:
        logging.error(f"  ❌ Erro HTTP em {source_name}: {he}")
        logging.error(f"     URL que falhou: {he.request.url}")
        collection_status[source_name] = f"❌ Falha ({he.response.status_code})"
        # Não tenta mais a URL alternativa, pois deu erro de JSON antes
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"  ❌ Erro inesperado em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro API)"
        return pd.DataFrame()

# --- Funções GEE, ANA, Stubs e main() permanecem IGUAIS ---

def fetch_clima_chirps_gee():
    source_name = 'clima_chirps_gee'
    logging.info(f"  Iniciando: {source_name} (Precip. GEE)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    try:
        chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY').filter(ee.Filter.date(START_DATE_STR, END_DATE_STR)).select('precipitation')

        def extract_stats(image):
            stats = image.reduceRegion(reducer=ee.Reducer.mean(), geometry=AOI_MATO_GROSSO, scale=5566)
            prec_val = stats.get('precipitation')
            return ee.Feature(None, {'date': image.date().format('YYYY-MM-dd'), 'prec_chirps_mm': ee.Algorithms.If(prec_val, prec_val, None)})

        feature_collection = chirps.map(extract_stats)
        results = feature_collection.limit(10000).getInfo()

        if not results or not results.get('features'):
             logging.warning(f"  ⚠️ Nenhum resultado ('features') GEE {source_name}.")
             collection_status[source_name] = "❌ Falha (Dados Vazios GEE)"
             return pd.DataFrame()

        data = [f['properties'] for f in results['features'] if f.get('properties') and f['properties'].get('prec_chirps_mm') is not None]
        if not data:
            logging.warning(f"  ⚠️ Resultados GEE {source_name} sem propriedades/valores válidos.")
            collection_status[source_name] = "❌ Falha (Dados Inválidos GEE)"
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        df['prec_chirps_mm'] = pd.to_numeric(df['prec_chirps_mm'], errors='coerce')
        df = df.dropna()
        # logging.info(f"  ✅ {source_name}: Coleta OK.") # Sucesso é logado ao salvar
        return df
    except Exception as e:
        logging.error(f"  ❌ Erro em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro GEE)"
        return pd.DataFrame()

def fetch_clima_era5_openmeteo():
    """Busca reanálise climática do ERA5 (via API Open-Meteo)."""
    source_name = 'clima_era5'
    logging.info(f"  Iniciando: {source_name} (Prec. Open-Meteo)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."

    end_date_buffered = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if START_DATE_STR > end_date_buffered:
        logging.warning(f"  ⚠️ Período inválido para {source_name} ({START_DATE_STR} > {end_date_buffered}).")
        collection_status[source_name] = "❌ Falha (Data Inválida)"
        return pd.DataFrame()

    lat, lon = -12.54, -55.71 # MT
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {"latitude": lat, "longitude": lon, "start_date": START_DATE_STR, "end_date": end_date_buffered, "daily": ["precipitation_sum"], "timezone": "America/Sao_Paulo"}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get('daily', {})
        if not data or not data.get('time'):
            logging.warning(f"  ⚠️ Nenhum dado ('daily') retornado pelo {source_name}.")
            collection_status[source_name] = "❌ Falha (Dados Vazios API)"
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['time'])
        df = df.set_index('date').drop(columns='time')
        df = df.rename(columns={'precipitation_sum': 'Prec_ERA5_mm'})
        # logging.info(f"  ✅ {source_name}: Coleta OK.") # Sucesso é logado ao salvar
        return df
    except requests.exceptions.HTTPError as e:
        logging.error(f"  ❌ Erro HTTP em {source_name}: {e}")
        try: logging.error(f"     Detalhe API: {response.json()}")
        except: pass
        collection_status[source_name] = f"❌ Falha ({e.response.status_code})"
        return pd.DataFrame()
    except requests.exceptions.Timeout:
         logging.error(f"  ❌ Erro em {source_name}: Timeout.")
         collection_status[source_name] = "❌ Falha (Timeout)"
         return pd.DataFrame()
    except Exception as e:
        logging.error(f"  ❌ Erro inesperado em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro API)"
        return pd.DataFrame()

def fetch_clima_noaa_stub():
    logging.warning("  [STUB] NOAA: Previsões (GFS/CFSv2) não são históricas.")
    return pd.DataFrame()

# --- 🛰️ Categoria: Satélite e Vegetação ---

def fetch_satelite_modis_ndvi_gee():
    source_name = 'satelite_modis_ndvi'
    logging.info(f"  Iniciando: {source_name} (GEE)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."
    try:
        modis = ee.ImageCollection('MODIS/061/MOD13A1').filter(ee.Filter.date(START_DATE_STR, END_DATE_STR)).select('NDVI')

        def extract_stats(image):
            stats = image.reduceRegion(reducer=ee.Reducer.mean(), geometry=AOI_MATO_GROSSO, scale=500)
            ndvi_val = stats.get('NDVI')
            return ee.Feature(None, {'date': image.date().format('YYYY-MM-dd'), 'ndvi_modis_mean': ee.Algorithms.If(ndvi_val, ndvi_val, None)})

        feature_collection = modis.map(extract_stats)
        results = feature_collection.limit(10000).getInfo()

        if not results or not results.get('features'):
             logging.warning(f"  ⚠️ Nenhum resultado ('features') GEE {source_name}.")
             collection_status[source_name] = "❌ Falha (Dados Vazios GEE)"
             return pd.DataFrame()

        data = [f['properties'] for f in results['features'] if f.get('properties') and f['properties'].get('ndvi_modis_mean') is not None]
        if not data:
            logging.warning(f"  ⚠️ Resultados GEE {source_name} sem propriedades/valores válidos.")
            collection_status[source_name] = "❌ Falha (Dados Inválidos GEE)"
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df['ndvi_modis_mean'] = pd.to_numeric(df['ndvi_modis_mean'], errors='coerce') / 10000.0
        df = df.set_index('date').sort_index().dropna(subset=['ndvi_modis_mean'])
        # logging.info(f"  ✅ {source_name}: Coleta OK.") # Sucesso é logado ao salvar
        return df
    except Exception as e:
        logging.error(f"  ❌ Erro em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro GEE)"
        return pd.DataFrame()

def fetch_satelite_sentinel_stub():
    logging.warning("  [STUB] Sentinel-2: Dados muito pesados.")
    return pd.DataFrame()

def fetch_satelite_mapbiomas_stub():
    logging.warning("  [STUB] MapBiomas: Dados anuais, não série temporal.")
    return pd.DataFrame()

# --- 🌊 Categoria: Hidrologia e Energia ---

def fetch_hidrologia_ana():
    """Busca nível de rios da ANA (Agência Nacional de Águas) via API direta."""
    source_name = 'hidro_ana'
    logging.info(f"  Iniciando: {source_name} (Nível Rio)...")
    global collection_status
    collection_status[source_name] = "⏳ Coletando..."

    url_base = "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/GetDadosTelemetricos"
    station_code = "18580000" # MT
    params = {'codEstacao': station_code, 'dataInicio': START_DATE_STR, 'dataFim': END_DATE_STR}

    try:
        response = requests.get(url_base, params=params, timeout=30)
        response.raise_for_status()

        try:
             df = pd.read_xml(io.StringIO(response.text), xpath=".//DadosHidrometereologicos", parser="lxml")
        except Exception as xml_e:
             logging.error(f"  ❌ Erro ao parsear XML da {source_name}: {xml_e}")
             logging.debug(f"     Conteúdo {source_name}: {response.text[:500]}...")
             collection_status[source_name] = "❌ Falha (XML Inválido)"
             return pd.DataFrame()

        if df.empty:
            logging.warning(f"  ⚠️ Nenhum dado XML ('DadosHidrometereologicos') {source_name} estação {station_code}")
            collection_status[source_name] = "❌ Falha (Dados Vazios XML)"
            return pd.DataFrame()

        if 'DataHora' not in df.columns or 'TipoDado' not in df.columns or 'Nivel' not in df.columns:
             logging.error(f"  ❌ Colunas essenciais {source_name} ausentes.")
             logging.debug(f"     Colunas {source_name}: {df.columns.tolist()}")
             collection_status[source_name] = "❌ Falha (Formato Inesperado)"
             return pd.DataFrame()

        df['date'] = pd.to_datetime(df['DataHora'])
        df_nivel = df[df['TipoDado'] == 2].copy()

        if df_nivel.empty:
            logging.warning(f"  ⚠️ Estação {source_name} {station_code} sem dados de Nível (TipoDado 2).")
            collection_status[source_name] = "❌ Falha (Sem Dados Nível)"
            return pd.DataFrame()

        df_nivel = df_nivel.set_index('date')[['Nivel']].rename(columns={'Nivel': 'Nivel_Rio_TelesPires_cm'})
        df_nivel['Nivel_Rio_TelesPires_cm'] = pd.to_numeric(df_nivel['Nivel_Rio_TelesPires_cm'], errors='coerce')
        df_nivel = df_nivel.dropna()

        # logging.info(f"  ✅ {source_name}: Coleta OK.") # Sucesso é logado ao salvar
        return df_nivel

    except ImportError:
         logging.error(f"  ❌ Erro {source_name}: Biblioteca 'lxml' faltando.")
         collection_status[source_name] = "❌ Falha (Dependência)"
         return pd.DataFrame()
    except requests.exceptions.Timeout:
         logging.error(f"  ❌ Erro {source_name}: Timeout.")
         collection_status[source_name] = "❌ Falha (Timeout)"
         return pd.DataFrame()
    except requests.exceptions.HTTPError as he:
        logging.error(f"  ❌ Erro HTTP em {source_name}: {he}")
        if response.status_code == 500: logging.warning("     Servidor ANA (500) instável.")
        collection_status[source_name] = f"❌ Falha ({he.response.status_code})"
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"  ❌ Erro inesperado em {source_name}: {e}")
        collection_status[source_name] = "❌ Falha (Erro API)"
        return pd.DataFrame()

def fetch_hidrologia_ons_stub():
    logging.warning("  [STUB] ONS: Requer scraping.")
    return pd.DataFrame()

# --- 🚛 Categoria: Logística e Transporte (STUBS) ---

def fetch_logistica_stubs():
    logging.warning("  [STUB] ANTT/Antaq: Datasets estáticos.")
    logging.warning("  [STUB] AIS Vessel Traffic: APIs pagas.")
    return pd.DataFrame()

# --- 🐄 Categoria: Agricultura, Pragas e Saúde Animal (STUBS) ---

def fetch_alertas_stubs():
    logging.warning("  [STUB] MAPA/FAO: Relatórios qualitativos (requer NLP).")
    return pd.DataFrame()

# --- 🏃‍♂️ Função Principal de Execução ---
def main():
    """
    Orquestra a coleta de todos os fatores externos.
    """

    # --- ETAPA 0: VERIFICAR CREDENCIAIS ---
    try:
        credentials = check_and_setup_credentials()
    except SystemExit: # Captura sys.exit para parar graciosamente
         logging.fatal("\nExecução interrompida durante verificação de credenciais.")
         log_final_checklist() # Loga o checklist mesmo em falha
         return
    except Exception as e:
        logging.fatal(f"\nFalha crítica na configuração de credenciais: {e}")
        log_final_checklist() # Loga o checklist mesmo em falha
        return

    # Inicializa o cliente CDS (agora é seguro)
    try:
        logging.info("\nInicializando cliente CDS (Copernicus)...")
        cds_client = cdsapi.Client()
        logging.info("✅ Cliente CDS inicializado.")
    except Exception as e:
        logging.error(f"❌ Falha ao inicializar o cliente CDS: {e}")
        logging.error("   Verifique seu arquivo .cdsapirc")
        log_final_checklist() # Mostra checklist mesmo se falhar aqui
        return # Não prossegue se o CDS falhar

    logging.info(f"\nIniciando coleta de dados para o período:")
    logging.info(f"  Data Início: {START_DATE_STR}")
    logging.info(f"  Data Fim:    {END_DATE_STR}")

    all_data = {} # Dicionário para guardar os DataFrames coletados

    # --- Coleta por Categoria ---

    logging.info("\n" + "-"*70)
    logging.info("ETAPA 1: COLETA - Macro e Commodities")
    logging.info("-"*70)
    try: all_data['macro_bcb'] = fetch_macro_bcb()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_macro_bcb: {e}")
    try: all_data['macro_ipea'] = fetch_macro_ipea()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_macro_ipea: {e}")
    try: all_data['macro_cepea'] = fetch_macro_cepea()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_macro_cepea: {e}")
    try: all_data['macro_quandl'] = fetch_macro_quandl(credentials.QUANDL_API_KEY)
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_macro_quandl: {e}")

    logging.info("\n" + "-"*70)
    logging.info("ETAPA 2: COLETA - Clima e Geografia")
    logging.info("-"*70)
    try: all_data['clima_inmet'] = fetch_clima_inmet()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_clima_inmet: {e}")
    try: all_data['clima_era5'] = fetch_clima_era5_openmeteo()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_clima_era5_openmeteo: {e}")
    try: all_data['clima_chirps_gee'] = fetch_clima_chirps_gee()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_clima_chirps_gee: {e}")
    fetch_clima_noaa_stub()

    logging.info("\n" + "-"*70)
    logging.info("ETAPA 3: COLETA - Satélite e Vegetação")
    logging.info("-"*70)
    try: all_data['satelite_modis_ndvi'] = fetch_satelite_modis_ndvi_gee()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_satelite_modis_ndvi: {e}")
    fetch_satelite_sentinel_stub()
    fetch_satelite_mapbiomas_stub()

    logging.info("\n" + "-"*70)
    logging.info("ETAPA 4: COLETA - Hidrologia e Energia")
    logging.info("-"*70)
    try: all_data['hidro_ana'] = fetch_hidrologia_ana()
    except Exception as e: logging.error(f"Falha INESPERADA em fetch_hidro_ana: {e}")
    fetch_hidrologia_ons_stub()

    logging.info("\n" + "-"*70)
    logging.info("ETAPA 5: COLETA - Stubs (Não Coletados)")
    logging.info("-"*70)
    fetch_logistica_stubs()
    fetch_alertas_stubs()

    # --- Salvamento dos Dados ---
    logging.info("\n" + "="*70)
    logging.info(f"ETAPA 6: SALVANDO DADOS em '{DATA_DIR.name}'...")
    logging.info("="*70)

    # Garante que mesmo fontes que falharam tenham um status final antes de salvar
    for source in SOURCES_TO_CHECK:
        current_status = collection_status.get(source)
        # Se ainda está Pendente ou Coletando E não há dados OU houve erro na API/HTTP
        # então marca como falha genérica se não houver erro específico já registrado
        if (current_status == "⏳ Coletando..." or current_status == "⏳ Pendente") and \
           (source not in all_data or all_data[source] is None or all_data[source].empty):
            # Mantém o status de erro específico se já existir
            if not (current_status and current_status.startswith("❌")):
                 collection_status[source] = "❌ Falha (Erro Coleta/Vazio)"

    # Tenta salvar os dados coletados (que não estão vazios)
    for name, df in all_data.items():
        if name in SOURCES_TO_CHECK: # Salva apenas os que estão na checklist
             save_data(df, name) # save_data atualiza o status

    # --- Checklist e Resumo Final ---
    log_final_checklist()


def log_final_checklist():
    """Gera o log final com o checklist e o resumo da execução."""
    successful_saves = 0
    # Conta sucessos finais baseados no status
    for status in collection_status.values():
        if status.startswith("✅"):
            successful_saves += 1

    logging.info("\n" + "="*70)
    logging.info("CHECKLIST FINAL DA COLETA")
    logging.info("="*70)
    try:
        # Garante que todos os status pendentes/coletando sejam marcados como falha no checklist final
        for source in SOURCES_TO_CHECK:
            if collection_status.get(source) == "⏳ Coletando..." or collection_status.get(source) == "⏳ Pendente":
                collection_status[source] = "❌ Falha (Não concluído/Erro)"

        max_len = max(len(s) for s in SOURCES_TO_CHECK) + 1 # Para alinhamento
        for source in SOURCES_TO_CHECK:
            status = collection_status.get(source, "❓ Desconhecido")
            logging.info(f"  - {source:<{max_len}}: {status}")
    except Exception as checklist_e:
         logging.error(f"Erro ao gerar checklist: {checklist_e}")

    logging.info("\n" + "="*70)
    logging.info(f"EXECUÇÃO CONCLUÍDA - v{SCRIPT_VERSION}")
    logging.info(f"  - {successful_saves} de {len(SOURCES_TO_CHECK)} fontes de dados salvas com sucesso.")
    logging.info(f"  - Arquivos .parquet salvos em: '{DATA_DIR.resolve()}'")
    logging.info(f"  - Log completo salvo em: '{log_file_path.resolve()}'")
    logging.info("="*70)
    logging.info("Próxima etapa: Unir, limpar e processar estes dados para o cálculo do IER.")
    logging.info("="*70)


if __name__ == "__main__":
    main()