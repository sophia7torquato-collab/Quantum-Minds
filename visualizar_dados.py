#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para visualizar os dados dos índices agrícolas coletados
Autor: Assistente IA
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuração do matplotlib para melhor visualização
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['font.size'] = 10

def carregar_dados():
    """Carrega os dados do arquivo Excel"""
    try:
        df = pd.read_excel('indices_agro.xlsx')
        print(f"[INFO] Dados carregados: {len(df)} registros")
        print(f"[INFO] Período: {df['data'].min()} a {df['data'].max()}")
        print(f"[INFO] Índices disponíveis: {df['indice'].nunique()}")
        print(f"[INFO] Índices: {list(df['indice'].unique())}")
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao carregar dados: {e}")
        return None

def analise_estatistica(df):
    """Realiza análise estatística básica dos dados"""
    print("\n" + "="*60)
    print("ANÁLISE ESTATÍSTICA DOS ÍNDICES")
    print("="*60)
    
    # Estatísticas por índice
    stats = df.groupby('indice')['valor'].agg([
        'count', 'mean', 'std', 'min', 'max',
        ('variacao_media', lambda x: x.mean()),
        ('volatilidade', lambda x: x.std())
    ]).round(4)
    
    print("\nEstatísticas por Índice:")
    print(stats)
    
    # Correlações entre índices
    print("\n" + "-"*40)
    print("MATRIZ DE CORRELAÇÃO")
    print("-"*40)
    
    # Pivotar dados para análise de correlação
    df_pivot = df.pivot(index='data', columns='indice', values='valor')
    correlacao = df_pivot.corr()
    
    print(correlacao.round(3))
    
    return stats, correlacao

def grafico_evolucao_temporal(df):
    """Cria gráfico de evolução temporal dos índices"""
    plt.figure(figsize=(16, 10))
    
    # Normalizar valores para base 100 (facilitar comparação)
    df_normalizado = df.copy()
    for indice in df['indice'].unique():
        mask = df_normalizado['indice'] == indice
        valores = df_normalizado.loc[mask, 'valor']
        if len(valores) > 0:
            df_normalizado.loc[mask, 'valor_normalizado'] = (valores / valores.iloc[0]) * 100
    
    # Plotar cada índice
    for i, indice in enumerate(df['indice'].unique()):
        dados_indice = df_normalizado[df_normalizado['indice'] == indice].sort_values('data')
        plt.subplot(2, 3, i+1)
        plt.plot(dados_indice['data'], dados_indice['valor_normalizado'], 
                linewidth=2, label=indice)
        plt.title(f'{indice}\n(Base 100)', fontsize=12, fontweight='bold')
        plt.xlabel('Data')
        plt.ylabel('Valor Normalizado')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('evolucao_indices.png', dpi=300, bbox_inches='tight')
    plt.show()

def grafico_correlacao(correlacao):
    """Cria heatmap de correlação"""
    plt.figure(figsize=(10, 8))
    
    # Criar máscara para mostrar apenas metade da matriz
    mask = np.triu(np.ones_like(correlacao, dtype=bool))
    
    sns.heatmap(correlacao, mask=mask, annot=True, cmap='RdYlBu_r', 
                center=0, square=True, linewidths=0.5, 
                cbar_kws={"shrink": .8}, fmt='.3f')
    
    plt.title('Matriz de Correlação entre Índices Agrícolas', 
              fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig('correlacao_indices.png', dpi=300, bbox_inches='tight')
    plt.show()

def grafico_volatilidade(df):
    """Cria gráfico de volatilidade (variação percentual)"""
    plt.figure(figsize=(16, 8))
    
    for i, indice in enumerate(df['indice'].unique()):
        dados_indice = df[df['indice'] == indice].sort_values('data')
        plt.subplot(2, 3, i+1)
        
        # Calcular volatilidade móvel (30 dias)
        if len(dados_indice) > 30:
            volatilidade_movel = dados_indice['variacao'].rolling(window=30).std()
            plt.plot(dados_indice['data'], volatilidade_movel, 
                    linewidth=2, color='red', alpha=0.7)
            plt.title(f'Volatilidade Móvel (30 dias)\n{indice}', 
                     fontsize=12, fontweight='bold')
        else:
            plt.plot(dados_indice['data'], dados_indice['variacao'], 
                    linewidth=2, color='red', alpha=0.7)
            plt.title(f'Variação Diária\n{indice}', 
                     fontsize=12, fontweight='bold')
        
        plt.xlabel('Data')
        plt.ylabel('Volatilidade (%)')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('volatilidade_indices.png', dpi=300, bbox_inches='tight')
    plt.show()

def grafico_comparacao_anual(df):
    """Cria gráfico de comparação anual dos índices"""
    # Extrair ano dos dados
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month
    
    # Calcular média anual por índice
    media_anual = df.groupby(['ano', 'indice'])['valor'].mean().reset_index()
    
    plt.figure(figsize=(14, 8))
    
    for indice in df['indice'].unique():
        dados_indice = media_anual[media_anual['indice'] == indice]
        plt.plot(dados_indice['ano'], dados_indice['valor'], 
                marker='o', linewidth=2, markersize=6, label=indice)
    
    plt.title('Evolução Anual dos Índices Agrícolas (Média)', 
              fontsize=14, fontweight='bold')
    plt.xlabel('Ano')
    plt.ylabel('Valor Médio')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('evolucao_anual.png', dpi=300, bbox_inches='tight')
    plt.show()

def resumo_executivo(df, stats):
    """Gera resumo executivo dos dados"""
    print("\n" + "="*60)
    print("RESUMO EXECUTIVO")
    print("="*60)
    
    # Período de análise
    periodo = f"{df['data'].min().strftime('%d/%m/%Y')} a {df['data'].max().strftime('%d/%m/%Y')}"
    print(f"Período de Análise: {periodo}")
    print(f"Total de Registros: {len(df):,}")
    print(f"Número de Índices: {df['indice'].nunique()}")
    
    # Índice com maior crescimento
    crescimento = df.groupby('indice')['valor'].agg(['first', 'last'])
    crescimento['crescimento_pct'] = ((crescimento['last'] / crescimento['first']) - 1) * 100
    maior_crescimento = crescimento['crescimento_pct'].idxmax()
    menor_crescimento = crescimento['crescimento_pct'].idxmin()
    
    print(f"\nMaior Crescimento: {maior_crescimento} ({crescimento.loc[maior_crescimento, 'crescimento_pct']:.2f}%)")
    print(f"Menor Crescimento: {menor_crescimento} ({crescimento.loc[menor_crescimento, 'crescimento_pct']:.2f}%)")
    
    # Volatilidade
    volatilidade_media = df.groupby('indice')['variacao'].std().mean()
    print(f"\nVolatilidade Média: {volatilidade_media:.2f}%")
    
    # Correlação mais alta
    df_pivot = df.pivot(index='data', columns='indice', values='valor')
    correlacao = df_pivot.corr()
    
    # Encontrar maior correlação (excluindo diagonal)
    correlacao_sem_diag = correlacao.where(~np.eye(len(correlacao), dtype=bool))
    max_corr = correlacao_sem_diag.max().max()
    indices_max_corr = correlacao_sem_diag[correlacao_sem_diag == max_corr].stack().index[0]
    
    print(f"Maior Correlação: {indices_max_corr[0]} x {indices_max_corr[1]} ({max_corr:.3f})")

def main():
    """Função principal"""
    print("="*60)
    print("VISUALIZAÇÃO DOS ÍNDICES AGRÍCOLAS")
    print("="*60)
    
    # Carregar dados
    df = carregar_dados()
    if df is None:
        return
    
    # Converter coluna de data
    df['data'] = pd.to_datetime(df['data'])
    
    # Análise estatística
    stats, correlacao = analise_estatistica(df)
    
    # Gerar gráficos
    print("\n[INFO] Gerando gráficos...")
    
    print("  - Gráfico de evolução temporal...")
    grafico_evolucao_temporal(df)
    
    print("  - Gráfico de correlação...")
    grafico_correlacao(correlacao)
    
    print("  - Gráfico de volatilidade...")
    grafico_volatilidade(df)
    
    print("  - Gráfico de comparação anual...")
    grafico_comparacao_anual(df)
    
    # Resumo executivo
    resumo_executivo(df, stats)
    
    print("\n" + "="*60)
    print("ANÁLISE CONCLUÍDA!")
    print("Gráficos salvos como:")
    print("  - evolucao_indices.png")
    print("  - correlacao_indices.png") 
    print("  - volatilidade_indices.png")
    print("  - evolucao_anual.png")
    print("="*60)

if __name__ == "__main__":
    main()
