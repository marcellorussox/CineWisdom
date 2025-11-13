import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


def plot_mab_performance(
        history_df: pd.DataFrame,
        model_name_to_track: str = "KBRS_Hybrid",
        title_selection: str = 'Percentuale di Selezione del Modello Ibrido (Thompson Sampling)',
        title_reward: str = 'Andamento del Tasso di Successo Cumulativo (MAB Performance)'
) -> None:
    """
    Genera due grafici: il tasso di selezione cumulativo per un modello
    e il tasso di successo cumulativo (average reward) del MAB.

    Parametri
    ----------
    history_df : pd.DataFrame
        DataFrame contenente la storia della simulazione MAB. Deve contenere
        le colonne 'iteration', 'model_name', e 'reward'.
    model_name_to_track : str
        Il nome del modello di cui tracciare la percentuale di selezione cumulativa.
    title_selection : str
        Titolo per il grafico del tasso di selezione.
    title_reward : str
        Titolo per il grafico del tasso di successo cumulativo.
    """

    # Assicurati che le colonne necessarie esistano
    if 'model_name' not in history_df.columns or 'reward' not in history_df.columns:
        raise ValueError("history_df deve contenere le colonne 'model_name' e 'reward'.")

    # 1. Calcolo del Tasso di Selezione Cumulativo

    # Crea una colonna binaria per il modello tracciato
    history_df['is_tracked'] = history_df['model_name'].apply(
        lambda x: 1 if x == model_name_to_track else 0
    )

    # Calcola il tasso cumulativo
    history_df['cumulative_tracked_rate'] = history_df['is_tracked'].cumsum() / (history_df.index + 1)

    # 2. Calcolo del Tasso di Successo Cumulativo (Average Reward)
    history_df['cumulative_reward'] = history_df['reward'].cumsum()
    history_df['cumulative_average_reward'] = history_df['cumulative_reward'] / (history_df.index + 1)

    # --- Plotting ---

    # Plot 1: Tasso di Selezione Cumulativo
    plt.figure(figsize=(14, 6))
    plt.plot(
        history_df['iteration'],
        history_df['cumulative_tracked_rate'],
        label=f'{model_name_to_track} Selection Rate',
        color='blue'
    )
    plt.title(title_selection)
    plt.xlabel('Iterazione')
    plt.ylabel('Tasso di Selezione Cumulativo')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.savefig("plots/selection_rate.png")
    plt.show()

    # Plot 2: Tasso di Successo Cumulativo (Average Reward)
    plt.figure(figsize=(14, 6))
    plt.plot(
        history_df['iteration'],
        history_df['cumulative_average_reward'],
        label='Cumulative Average Reward',
        color='green'
    )
    plt.title(title_reward)
    plt.xlabel('Iterazione')
    plt.ylabel('Tasso di Successo Medio Cumulativo')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.savefig("plots/cumulative_reward.png")
    plt.show()


def plot_comparison_results(comparison_df: pd.DataFrame) -> None:
    """
    📊 Plot confronto risultati di multiple simulazioni.

    Args:
        comparison_df: DataFrame con colonne:
            - Simulazione: nome configurazione
            - KBRS Selection (%): percentuale scelta KBRS
            - Baseline Selection (%): percentuale scelta Baseline
            - Average Reward: reward medio
            - KBRS R_G: accuracy KBRS
    """
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

    # Plot 1: Selection Rate Comparison
    x = range(len(comparison_df))
    width = 0.35
    ax1.bar([i - width/2 for i in x], comparison_df['KBRS Selection (%)'],
            width, label='KBRS', alpha=0.8, color='blue')
    ax1.bar([i + width/2 for i in x], comparison_df['Baseline Selection (%)'],
            width, label='Baseline', alpha=0.8, color='orange')
    ax1.set_xlabel('Configurazione')
    ax1.set_ylabel('Selection Rate (%)')
    ax1.set_title('KBRS vs Baseline - Selection Rate')
    ax1.set_xticks(x)
    ax1.set_xticklabels(comparison_df['Simulazione'], rotation=15)
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot 2: Average Reward
    ax2.plot(comparison_df['Simulazione'], comparison_df['Average Reward'],
             'o-', linewidth=2, markersize=8, color='green')
    ax2.set_xlabel('Configurazione')
    ax2.set_ylabel('Average Reward')
    ax2.set_title('Average Reward per Configurazione')
    ax2.grid(True, alpha=0.3)

    # Plot 3: KBRS R_G
    ax3.plot(comparison_df['Simulazione'], comparison_df['KBRS R_G'],
             'o-', color='purple', linewidth=2, markersize=8)
    ax3.set_xlabel('Configurazione')
    ax3.set_ylabel('KBRS General Accuracy (R_G)')
    ax3.set_title('KBRS Accuracy per Configurazione')
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(0, 1.1)

    # Plot 4: Performance Comparison (Normalize for visualization)
    categories = ['KBRS\nSelection', 'Baseline\nSelection', 'Avg Reward', 'KBRS R_G']
    first_values = [
        comparison_df.iloc[0]['KBRS Selection (%)'],
        comparison_df.iloc[0]['Baseline Selection (%)'],
        comparison_df.iloc[0]['Average Reward'] * 100,
        comparison_df.iloc[0]['KBRS R_G'] * 100
    ]
    last_values = [
        comparison_df.iloc[-1]['KBRS Selection (%)'],
        comparison_df.iloc[-1]['Baseline Selection (%)'],
        comparison_df.iloc[-1]['Average Reward'] * 100,
        comparison_df.iloc[-1]['KBRS R_G'] * 100
    ]

    x = range(len(categories))
    ax4.bar([i - width/2 for i in x], first_values, width,
            label=f"{comparison_df.iloc[0]['Simulazione']}", alpha=0.8)
    ax4.bar([i + width/2 for i in x], last_values, width,
            label=f"{comparison_df.iloc[-1]['Simulazione']}", alpha=0.8)
    ax4.set_xlabel('Metriche')
    ax4.set_ylabel('Valore')
    ax4.set_title('Confronto Prima vs Ultima Configurazione')
    ax4.set_xticks(x)
    ax4.set_xticklabels(categories)
    ax4.legend()
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    print("✅ Grafici comparativi generati!")


def plot_pondered_rewards(history_df: pd.DataFrame, title: str = "Pondered Reward Analysis") -> None:
    """
    📊 Plot analysis Pondered Reward (R_A vs R_P).

    Args:
        history_df: DataFrame con colonne 'iteration', 'model_name', 'reward'
        title: Titolo del grafico
    """
    # Simulate R_G proxy (in real usage, this comes from reward system)
    kbrs_rg = 0.8  # Placeholder - should be dynamic

    # Compute R_P = 0.5 * R_A + 0.5 * R_G
    history_df = history_df.copy()
    history_df['R_G_proxy'] = history_df['model_name'].map({
        'KBRS_Hybrid': kbrs_rg,
        'Popularity_Baseline': 0.0,
    }).fillna(0.0)
    history_df['R_A'] = history_df['reward']
    history_df['R_P'] = 0.5 * history_df['R_A'] + 0.5 * history_df['R_G_proxy']

    # Cumulative averages
    history_df['cumsum_R_A'] = history_df['R_A'].cumsum()
    history_df['cumsum_R_P'] = history_df['R_P'].cumsum()
    iterations = history_df['iteration'] + 1
    history_df['cum_avg_R_A'] = history_df['cumsum_R_A'] / iterations
    history_df['cum_avg_R_P'] = history_df['cumsum_R_P'] / iterations

    # Plot
    plt.figure(figsize=(14, 6))

    plt.plot(
        history_df['iteration'],
        history_df['cum_avg_R_A'],
        label='R_A (Exploration: Unseen ≥4.0)',
        color='green',
        linewidth=2,
        alpha=0.8,
    )

    plt.plot(
        history_df['iteration'],
        history_df['cum_avg_R_P'],
        label='R_P (Pondered: 0.5·R_A + 0.5·R_G)',
        color='blue',
        linewidth=2,
        alpha=0.8,
    )

    final_ra = history_df['cum_avg_R_A'].iloc[-1]
    final_rp = history_df['cum_avg_R_P'].iloc[-1]

    plt.title(title, fontsize=14, fontweight='bold')
    plt.xlabel('Iteration', fontsize=12)
    plt.ylabel('Cumulative Average Reward', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend(fontsize=11, loc='best')

    # Add final values as annotation
    textstr = f'\n'.join([
        f"Final R_A: {final_ra:.4f}",
        f"Final R_P: {final_rp:.4f}",
    ])
    plt.text(0.02, 0.98, textstr,
             transform=plt.gca().transAxes,
             fontsize=10,
             verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()
    plt.show()


def create_summary_table(comparison_df: pd.DataFrame) -> None:
    """
    📋 Stampa tabella riassuntiva formattata.

    Args:
        comparison_df: DataFrame con risultati comparativi
    """
    print("\n" + "=" * 80)
    print("📊 TABELLA COMPARATIVA - IMPATTO DEI PARAMETRI")
    print("=" * 80)
    print(comparison_df.round(4).to_string(index=False))
    print("=" * 80)