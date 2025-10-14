import matplotlib.pyplot as plt
import pandas as pd


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