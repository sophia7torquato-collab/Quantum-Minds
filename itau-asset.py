# -*- coding: utf-8 -*-
"""
Script: coleta_final.py (Versão Definitiva: Integração com API do Banco Central)
Objetivo: Coletar os 6 índices, substituindo falhas do IPEA/IBGE pela API estável do BCB e testando o YFinance.
Autor: Gemini
"""

import pandas as pd
import requests
from io import StringIO, BytesIO
import numpy as np
import time
import sys
import datetime

# --- 1. CONFIGURAÇÃO DE AMBIENTE E DEPENDÊNCIAS ---
# NOTA: Instale estas bibliotecas com: pip install pandas requests yfinance python-bcb openpyxl
try:
    import yfinance as yf
except ImportError:
    print("[ERRO] yfinance nao esta instalado. Instale-o e tente novamente.")
    sys.exit()

try:
    from bcb import sgs # Importa sgs diretamente do bcb
except ImportError:
    print("[ERRO] python-bcb nao esta instalado. Instale-o e tente novamente.")
    sys.exit()


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
DATA_INICIO = '2023-01-01'
df_coletados = []

# --- 2. FUNÇÕES DE PADRONIZAÇÃO ---

def padronizar_df_yfinance(df, nome_indice, fonte, coluna_valor='Close', coluna_data='Date'):
    """Padroniza DataFrame do YFinance com checagem robusta de array/tamanho."""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    
    # Lidar com colunas MultiIndex do yfinance
    if isinstance(df.columns, pd.MultiIndex):
        # Se for MultiIndex, pegar a primeira coluna de cada tipo
        df.columns = df.columns.get_level_values(0)
        # Remover duplicatas mantendo a primeira ocorrência
        df = df.loc[:, ~df.columns.duplicated()]
    
    # Verificar se as colunas necessárias existem
    if coluna_valor not in df.columns:
        print(f"DEBUG: Coluna '{coluna_valor}' não encontrada. Colunas disponíveis: {list(df.columns)}")
        return pd.DataFrame()
    
    # Se não há coluna de data, usar o índice
    if coluna_data not in df.columns:
        df = df.reset_index()
        if 'Date' in df.columns:
            coluna_data = 'Date'
        elif 'index' in df.columns:
            coluna_data = 'index'
        else:
            print(f"DEBUG: Nenhuma coluna de data encontrada. Colunas disponíveis: {list(df.columns)}")
            return pd.DataFrame()
    
    df = df.rename(columns={coluna_valor: 'valor', coluna_data: 'data'})
    df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.normalize()
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce') 
    df['indice'] = nome_indice
    df['fonte'] = fonte
    df_final = df[['data', 'indice', 'valor', 'fonte']].dropna(subset=['valor'])
    df_final = df_final.sort_values(by='data', ascending=True)
    df_final['variacao'] = df_final['valor'].pct_change() * 100
    return df_final


def padronizar_df_geral(df, nome_indice, fonte, coluna_valor, coluna_data='Date'):
    """Padroniza DataFrame de fontes com strings (FAO, IPEA, IBGE/BCB)."""
    if df is None or df.empty or coluna_valor not in df.columns or coluna_data not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df = df.rename(columns={coluna_valor: 'valor', coluna_data: 'data'})
    df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.normalize()
    
    try:
        if not pd.api.types.is_numeric_dtype(df['valor']):
            df['valor'] = pd.to_numeric(df['valor'].astype(str).str.replace(r'[^\d,\-\.]', '', regex=True).str.replace(',', '.', regex=False), errors='coerce')
        else:
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    except Exception as e:
        print(f"DEBUG PADRONIZAÇÃO GERAL: Falha de conversão em {nome_indice}. Erro: {e}")
        return pd.DataFrame()

    df['indice'] = nome_indice
    df['fonte'] = fonte
    df_final = df[['data', 'indice', 'valor', 'fonte']].dropna(subset=['valor'])
    df_final = df_final.sort_values(by='data', ascending=True)
    df_final['variacao'] = df_final['valor'].pct_change() * 100
    return df_final


# ====================================================================
# --- COLETA DE DADOS HISTÓRICOS (Versão V21 - Solução Total) ---
# ====================================================================

print(f"\nIniciando coleta de dados (V21 - Produção) em {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")

# --- 2.1. FAO Food Price Index (Funcional) ---
try:
    url_fao_excel = "https://www.fao.org/fileadmin/templates/worldfood/Reports_and_docs/Food_price_indices_data_may629.xls" 
    response_fao = requests.get(url_fao_excel, headers=HEADERS)
    response_fao.raise_for_status() 
    df_fao = pd.read_excel(BytesIO(response_fao.content), skiprows=4, usecols='A:B', header=None)
    df_fao = df_fao.dropna(subset=[1]).rename(columns={0:'Date', 1:'Food Price Index'})
    df_fao_clean = padronizar_df_geral(df_fao, "FAO Food Price Index", "FAO", 'Food Price Index', 'Date')
    df_coletados.append(df_fao_clean)
    print(f"  [OK] FAO FFPI: {len(df_fao_clean)} registros.")
except Exception as e:
    print(f"  [ERRO] na coleta FAO: {e}.")


# --- 2.2. IAGRO B3 (YFinance) ---
try:
    ticker_iagro = 'AGRI11.SA' 
    df_iagro = yf.download(ticker_iagro, start=DATA_INICIO, progress=False, auto_adjust=True).reset_index()
    df_iagro_clean = padronizar_df_yfinance(df_iagro, "IAGRO B3 (ETF Proxy)", f"yfinance ({ticker_iagro})", 'Close', 'Date')
    df_coletados.append(df_iagro_clean)
    time.sleep(1) 
    print(f"  [OK] IAGRO B3: {len(df_iagro_clean)} registros históricos.")
except Exception as e:
    print(f"  [ERRO] na coleta IAGRO B3: {e}. (Aguardando nova rede).")


# --- 2.3. S&P GSCI Agriculture (YFinance) ---
try:
    ticker_gsci = 'DBA' 
    df_gsci = yf.download(ticker_gsci, start=DATA_INICIO, progress=False, auto_adjust=True).reset_index()
    df_gsci_clean = padronizar_df_yfinance(df_gsci, "S&P GSCI Agriculture (ETF Proxy)", f"yfinance ({ticker_gsci})", 'Close', 'Date')
    df_coletados.append(df_gsci_clean)
    time.sleep(1) 
    print(f"  [OK] S&P GSCI: {len(df_gsci_clean)} registros históricos.")
except Exception as e:
    print(f"  [ERRO] na coleta S&P GSCI: {e}. (Aguardando nova rede).")


# --- 2.4. CEPEA/ESALQ SUBSTITUÍDO POR PROXY BCB (IPCA/INFLAÇÃO) ---
try:
    # Proxy para preço/inflação local (Substitui CEPEA/IPEADATA)
    df_bcb = sgs.get({'IPCA': 433}, start=DATA_INICIO)
    df_bcb = df_bcb.reset_index().rename(columns={'IPCA': 'valor', 'Date': 'data'})
    
    df_bcb_clean = padronizar_df_geral(df_bcb, "IPCA (Proxy CEPEA)", "BCB/SGS", 'valor', 'data')
    df_coletados.append(df_bcb_clean)
    print(f"  [OK] IPCA (Proxy CEPEA): {len(df_bcb_clean)} registros (via API BCB).")
except Exception as e:
    print(f"  [ERRO] na coleta IPCA (BCB): {e}.")


# --- 2.5. IBGE SUBSTITUÍDO POR PROXY BCB (Índice de Confiança Agronegócio) ---
try:
    # Proxy para risco de produção/sentimento (Substitui IBGE)
    df_bcb_agro = sgs.get({'ICC_AGRO': 4466}, start=DATA_INICIO)
    df_bcb_agro = df_bcb_agro.reset_index().rename(columns={'ICC_AGRO': 'valor', 'Date': 'data'})
    
    df_bcb_agro_clean = padronizar_df_geral(df_bcb_agro, "ICC Agro (Proxy IBGE)", "BCB/SGS", 'valor', 'data')
    df_coletados.append(df_bcb_agro_clean)
    print(f"  [OK] ICC Agro (Proxy IBGE): {len(df_bcb_agro_clean)} registros (via API BCB).")

except Exception as e:
    print(f"  [ERRO] na coleta ICC Agro (BCB): {e}.")


# --- 2.6. ICB – Brazil Commodities Index (YFinance) ---
try:
    ticker_soja = 'ZS=F' 
    df_icb_proxy = yf.download(ticker_soja, start=DATA_INICIO, progress=False, auto_adjust=True).reset_index()
    df_icb_clean = padronizar_df_yfinance(df_icb_proxy, "ICB Proxy (Soja Futuro CME)", f"yfinance ({ticker_soja})", 'Close', 'Date')
    df_coletados.append(df_icb_clean)
    time.sleep(1) 
    print(f"  [OK] ICB Proxy: {len(df_icb_clean)} registros históricos.")
except Exception as e:
    print(f"  [ERRO] na coleta ICB Proxy: {e}. (Aguardando nova rede).")


# ====================================================================
# --- CONSOLIDAÇÃO E EXPORTAÇÃO (LOAD) ---
# ====================================================================

if any(not df.empty for df in df_coletados):
    print("\n--- Processo Final: Consolidação ---")
    
    df_coletados_validos = [df for df in df_coletados if not df.empty]
    df_all = pd.concat(df_coletados_validos, ignore_index=True)
    
    df_all['data'] = df_all['data'].dt.normalize()
    df_all['valor'] = df_all['valor'].round(4)
    df_all['variacao'] = df_all['variacao'].round(4)
    
    colunas_finais = ['data', 'indice', 'valor', 'variacao', 'fonte']
    df_all = df_all[colunas_finais]
    
    nome_arquivo = "indices_agro.xlsx"
    df_all.to_excel(nome_arquivo, index=False)
    
    print(f"\n=======================================================")
    print(f"[SUCESSO] Arquivo '{nome_arquivo}' gerado com {len(df_all)} registros.")
    print(f"Total de Índices Coletados: {df_all['indice'].nunique()}")
    print("=======================================================")
else:
    print("[FALHA CRITICA] Nenhum dado foi coletado com sucesso. Verifique se as bibliotecas estao instaladas e a conexao de rede.")