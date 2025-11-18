# src/experiments/mab_experiment.py
import pandas as pd
import numpy as np
from pathlib import Path
import json
import matplotlib.pyplot as plt

# Imports
from src.data.manager import DataManager  # NO SplitManager qui!
from src.recommender.kbrs import KBRSEngine
from src.bandit.algorithms import LinUCB
from src.simulation.simulator import MABSimulator
from src.simulation.reward_system import RewardSystem

def run_online_simulation():
    """
    Pipeline 2: Valutazione Online (Efficacia MAB)

    Obiettivo: Misurare QUANTO È EFFICACE la strategia MAB nell'imparare nel tempo
    Metodologia: Replay sequenziale ordinato per timestamp
    Metriche: Cumulative Reward, CTR, learning curves

    NOTE: Non usa split! Usa TUTTI i dati ordinati per tempo.
    """

    print("=" * 80)
    print("PIPELINE 2: ONLINE SIMULATION (EFFICACIA MAB)")
    print("=" * 80)

    # Step 1: Carica TUTTI i dati (NO split!)
    print("\n[1/4] Caricamento TUTTI i dati...")
    data_manager = DataManager()
    movies_df, ratings_df = data_manager.load_movielens()
    movies_enriched = data_manager.enrich_with_dbpedia(movies_df)
    print(f"  ✓ Caricati {len(movies_enriched)} film, {len(ratings_df)} rating")

    # Step 2: Ordina per timestamp
    print("\n[2/4] Ordinamento cronologico...")
    # Assumiamo che ratings_df abbia una colonna 'timestamp'
    ratings_sorted = ratings_df.sort_values('timestamp').reset_index(drop=True)
    print(f"  ✓ Ordinati {len(ratings_sorted)} eventi dal {ratings_sorted['timestamp'].min()} al {ratings_sorted['timestamp'].max()}")

    # Step 3: Inizializza componenti
    print("\n[3/4] Inizializzazione MAB e simulator...")
    kbrs_engine = KBRSEngine(movies_enriched)

    # Inizializza algoritmo MAB (LinUCB)
    # Il numero di feature = dimensione del vettore profilo utente
    n_features = len(kbrs_engine.feature_columns)
    mab_algorithm = LinUCB(n_features=n_features, alpha=1.0)

    # Sistema di ricompensa
    reward_system = RewardSystem(threshold=4.0)  # rating >= 4.0 → reward=1

    # Simulator
    simulator = MABSimulator(
        mab=mab_algorithm,
        kbrs_engine=kbrs_engine,
        reward_system=reward_system
    )

    # Step 4: Esegui simulazione Replay
    print("\n[4/4] Esecuzione simulazione Replay...")
    print("  Ad ogni evento (user, movie, rating):")
    print("    1. MAB decide quale film raccomandare")
    print("    2. Se coincide con il film reale → reward e update")
    print("    3. Altrimenti → ignora")

    results = simulator.run_replay(ratings_sorted)

    # Salva risultati
    output_dir = Path('results/mab')
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / 'simulation_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    # Genera plot cumulative reward
    plt.figure(figsize=(12, 6))
    plt.plot(results['cumulative_reward'])
    plt.title('Cumulative Reward over Time (MAB Learning)')
    plt.xlabel('Eventi')
    plt.ylabel('Cumulative Reward')
    plt.grid(True)
    plt.savefig(output_dir / 'cumulative_reward.png', dpi=150)
    plt.close()

    print(f"\n{'='*80}")
    print(f"RISULTATI ONLINE SIMULATION:")
    print(f"  Cumulative Reward finale: {results['final_cumulative_reward']:.2f}")
    print(f"  CTR finale: {results['final_ctr']:.4f}")
    print(f"  Algoritmo: LinUCB")
    print(f"{'='*80}")
    print(f"Risultati salvati in: {output_dir / 'simulation_results.json'}")

    return results

if __name__ == "__main__":
    run_online_simulation()
