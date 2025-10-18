import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Funções para Fatores Macroeconômicos e Commodities ---

def fetch_bcb_data_cambio(start_date, end_date):
    """
    Busca a série de câmbio (USD/BRL) da API do Banco Central (SGS).
    Este é um dos fatores "Macro e Commodities". 
    
    - Código da série 1: Dólar Americano (venda)
    """
    logging.info("Buscando Fator: Macro - Câmbio (USD/BRL) do Banco Central...")
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?"
        f"formato=json&dataInicial={start_date.strftime('%d/%m/%Y')}&"
        f"dataFinal={end_date.strftime('%d/%m/%Y')}"
    )
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            logging.warning("Nenhum dado de câmbio retornado pelo BCB.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
        df['valor'] = pd.to_numeric(df['valor'])
        df = df.set_index('data').rename(columns={'valor': 'USD_BRL_Venda'})
        logging.info(f"Sucesso: {len(df)} registros de câmbio obtidos.")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados do BCB: {e}")
        return pd.DataFrame()

def fetch_cepea_data_commodities(start_date_str, end_date_str):
    """
    Busca indicadores de preço de commodities do CEPEA. 
    
    - Exemplo usando o ID 104: Milho (ESALQ/BM&FBOVESPA)
    - As datas devem estar no formato YYYY-MM-DD
    """
    logging.info("Buscando Fator: Macro - Preço Milho (CEPEA)...")
    # Este é um endpoint da API interna usada pelo site do CEPEA.
    # Pode ser instável ou mudar sem aviso.
    url = (
        f"https://www.cepea.esalq.usp.br/api/series/id/104?"
        f"start_date={start_date_str}&end_date={end_date_str}&"
        f"currency=BRL"
    )
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('series'):
            logging.warning("Nenhum dado de preço do CEPEA retornado.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data['series'])
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['price_brl'] = pd.to_numeric(df['price_brl'])
        df = df.set_index('date').rename(columns={'price_brl': 'Milho_CEPEA_BRL'})
        logging.info(f"Sucesso: {len(df)} registros de preço do milho obtidos.")
        return df[['Milho_CEPEA_BRL']]
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados do CEPEA: {e}")
        return pd.DataFrame()

def fetch_ibge_data_producao():
    """
    Busca dados de produção agrícola da API SIDRA/IBGE. 
    
    - Exemplo: Tabela 1612 - LSPA (Levantamento Sistemático da Produção Agrícola)
    - Variável 112: Produção (T COB)
    - Produto 24: Soja (em grão)
    - Nível Territorial N1: Brasil
    - Período: Últimos 12 meses (se disponível, LSPA é mensal)
    """
    logging.info("Buscando Fator: Macro - Produção de Soja (IBGE/SIDRA)...")
    # /T/1612 = Tabela LSPA
    # /V/112 = Variável "Produção"
    # /P/last12 = Últimos 12 meses
    # /C58/24 = Produto "Soja"
    # /N1/1 = Nível "Brasil"
    url = "https://apisidra.ibge.gov.br/values/T/1612/V/112/P/last12/C58/24/N1/1"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if len(data) <= 1: # Cabeçalho
            logging.warning("Nenhum dado de produção do IBGE retornado.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data[1:], columns=[h.lower() for h in data[0].values()])
        # Renomeia colunas para clareza
        df = df.rename(columns={
            'd3n': 'produto',
            'v': 'valor',
            'd4n': 'mes_ano'
        })
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        # Formata a data
        df['data'] = pd.to_datetime(df['mes_ano'], format='%B %Y')
        df = df.set_index('data')
        df = df[['produto', 'valor']].rename(columns={'valor': 'Producao_Soja_Ton'})
        logging.info(f"Sucesso: {len(df)} registros de produção de soja obtidos.")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados do IBGE/SIDRA: {e}")
        return pd.DataFrame()

# --- Funções para Fatores Climáticos ---

def fetch_inmet_data_clima(start_date_str, end_date_str):
    """
    Busca dados de estações meteorológicas do INMET. 
    
    - Exemplo: Estação A901 (Sorriso - MT), uma grande região produtora.
    - Busca dados diários.
    """
    logging.info("Buscando Fator: Clima - Estação INMET (Sorriso-MT)...")
    url = (
        f"https://apitempo.inmet.gov.br/estacao/diaria/"
        f"{start_date_str}/{end_date_str}/A901"
    )
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data:
            logging.warning("Nenhum dado de clima do INMET retornado.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['DT_MEDICAO'])
        df = df.set_index('data')
        # Seleciona colunas relevantes
        cols_clima = {
            'CHUVA': 'Precipitacao_mm',
            'TEMP_MAX': 'Temp_Max_C',
            'TEMP_MIN': 'Temp_Min_C',
            'UMID_MAX': 'Umidade_Max_pct',
            'UMID_MIN': 'Umidade_Min_pct'
        }
        df = df[cols_clima.keys()].rename(columns=cols_clima)
        # Converte tudo para numérico
        df = df.apply(pd.to_numeric, errors='coerce')
        logging.info(f"Sucesso: {len(df)} registros de clima (INMET) obtidos.")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados do INMET: {e}")
        return pd.DataFrame()

# --- Funções de STUB para Dados Complexos/Pagos ---

def fetch_satellite_data_stub():
    """
    [STUB] Placeholder para dados de Satélite e Vegetação. 
    
    - Fontes: MODIS, Sentinel-2, MapBiomas.
    - Métricas: NDVI, EVI, Estresse Hídrico.
    - Implementação: Requer bibliotecas geoespaciais (ex: Google Earth Engine API,
      rasterio, xarray) e processamento complexo para agregar dados por
      município ou área de atuação das empresas.
    """
    logging.warning("[STUB] Função 'fetch_satellite_data_stub' não implementada.")
    return pd.DataFrame()

def fetch_hidrologia_energia_stub():
    """
    [STUB] Placeholder para dados de Hidrologia e Energia. 
    
    - Fontes: ANA (nível de rios), ONS (nível de reservatórios).
    - Implementação: Dados do ONS/ANA são complexos de obter via API.
      O ONS, por exemplo, muitas vezes disponibiliza via arquivos diários (ex: DECK).
      Isso exigiria um scraper robusto.
    """
    logging.warning("[STUB] Função 'fetch_hidrologia_energia_stub' não implementada.")
    return pd.DataFrame()

def fetch_logistics_data_stub():
    """
    [STUB] Placeholder para dados de Logística e Transporte. 
    
    - Fontes: ANTT, Antaq (portos), AIS Vessel Traffic.
    - Métricas: Congestionamento portuário, tempo de embarque.
    - Implementação: Esses dados são frequentemente proprietários, pagos
      ou não estão disponíveis em APIs públicas de fácil acesso.
    """
    logging.warning("[STUB] Função 'fetch_logistics_data_stub' não implementada.")
    return pd.DataFrame()


# --- Função Principal de Execução ---

def main():
    """
    Orquestra a coleta de todos os fatores externos.
    Corresponde à Etapa 1 do Plano de Execução. 
    """
    logging.info("Iniciando Etapa 1: Coleta e Estruturação de Dados...")
    
    # Define o período de análise (últimos 3 anos, conforme backtest) 
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3*365)
    
    # Formatos de data para as diferentes APIs
    start_date_str_api = start_date.strftime('%Y-%m-%d')
    end_date_str_api = end_date.strftime('%Y-%m-%d')
    
    # 1. Fatores Macroeconômicos e Commodities
    df_cambio = fetch_bcb_data_cambio(start_date, end_date)
    df_milho = fetch_cepea_data_commodities(start_date_str_api, end_date_str_api)
    df_producao = fetch_ibge_data_producao() # LSPA já pega os últimos 12 meses
    
    # 2. Fatores Climáticos
    df_clima = fetch_inmet_data_clima(start_date_str_api, end_date_str_api)
    
    # 3. Fatores Complexos (Stubs)
    df_satelite = fetch_satellite_data_stub()
    df_hidro = fetch_hidrologia_energia_stub()
    df_logistica = fetch_logistics_data_stub()
    
    # Exibe um resumo dos dados coletados
    logging.info("\n--- Resumo da Coleta de Dados ---")
    print(f"Câmbio (BCB):\n{df_cambio.tail(3)}\n")
    print(f"Preço Milho (CEPEA):\n{df_milho.tail(3)}\n")
    print(f"Produção Soja (IBGE/SIDRA):\n{df_producao.tail(3)}\n")
    print(f"Clima (INMET - Sorriso/MT):\n{df_clima.tail(3)}\n")
    
    # Próximo passo (conforme seu plano):
    # - Limpar e organizar esses dados. 
    # - Juntar todos em um único DataFrame ou banco de dados (Parquet/SQLite). 
    # - Aplicar a metodologia IER (Z-Score, Logística). 
    logging.info("Coleta de dados públicos finalizada.")
    logging.info("Próximos passos: Implementar stubs de dados complexos, "
                 "limpar e consolidar os dados em um banco local.")

if __name__ == "__main__":
    main()