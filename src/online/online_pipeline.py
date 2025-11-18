"""
Online Pipeline - Orchestratore pipeline online

Pipeline completa per valutazione online MAB con replay evaluation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path


class OnlinePipeline:
    """
    Pipeline online completa.

    Gestisce tutto il flusso:
    1. Load dati TUTTI (NO split!)
    2. Ordinamento per timestamp
    3. Inizializzazione KBRS + MAB
    4. Replay simulation
    5. Tracking learning curve
    6. Analisi risultati
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Inizializza la pipeline online.

        Args:
            config: Dict con configurazione
        """
        self.config = config or {
            'mab_type': 'LinUCB',
            'mab_alpha': 1.0,
            'n_features': 128,
            'max_iterations': None,  # None = tutti gli eventi
            'reward_threshold': 4.0,
            'weight_exploration': 0.5,
            'weight_accuracy': 0.5,
            'output_dir': 'results/online'
        }

        # Componenti
        from src.common.data_loader import (
            load_movielens_data,
            enrich_with_dbpedia,
            normalize_catalog,
            compress_features
        )
        from src.common.kbrs_wrapper import KBRSWrapper
        from src.common.utils import (
            filter_users_by_rating_count,
            get_timestamp_sorted_ratings,
            validate_data,
            compute_global_statistics
        )
        from src.online.online_simulator import OnlineSimulator

        self.load_data_func = load_movielens_data
        self.enrich_data_func = enrich_with_dbpedia
        self.normalize_data_func = normalize_catalog
        self.compress_data_func = compress_features

        self.filter_users_func = filter_users_by_rating_count
        self.sort_by_timestamp_func = get_timestamp_sorted_ratings
        self.validate_data_func = validate_data
        self.compute_stats_func = compute_global_statistics

        self.kbrs_wrapper_class = KBRSWrapper
        self.simulator_class = OnlineSimulator

        # Data
        self.ratings_df = None
        self.movies_df = None
        self.links_df = None
        self.movies_enriched = None
        self.movies_normalized = None
        self.movies_compressed = None
        self.ratings_sorted = None

        # Components
        self.kbrs_wrapper = None
        self.simulator = None
        self.mab = None

        # Results
        self.results = {}

        print("✅ Online Pipeline inizializzata")

    def load_all_data(self) -> Dict:
        """
        Carica TUTTI i dati (NO split!).

        Returns:
            Dict con statistiche dati
        """
        print("\n" + "="*80)
        print("STEP 1: CARICAMENTO DATI (TUTTI)")
        print("="*80)

        # Carica dati grezzi
        print("\n1.1 Caricamento dati MovieLens...")
        self.ratings_df, self.movies_df, self.links_df = self.load_data_func()

        # Arricchimento DBpedia
        print("\n1.2 Arricchimento con DBpedia...")
        self.movies_enriched = self.enrich_data_func(self.movies_df)

        # Normalizzazione
        print("\n1.3 Normalizzazione catalogo...")
        self.movies_normalized = self.normalize_data_func(self.movies_enriched)

        # Compressione SVD
        print("\n1.4 Compressione TruncatedSVD...")
        self.movies_compressed = self.compress_data_func(
            self.movies_normalized,
            n_components=self.config['n_features']
        )

        # Filtra utenti con pochi rating
        print("\n1.5 Filtraggio utenti...")
        self.ratings_df = self.filter_users_func(
            self.ratings_df,
            min_ratings=self.config.get('min_ratings_per_user', 3)
        )

        # Valida dati
        print("\n1.6 Validazione dati...")
        is_valid = self.validate_data_func(self.ratings_df, self.movies_compressed)
        if not is_valid:
            raise ValueError("Validazione dati fallita")

        # Statistiche globali
        print("\n1.7 Calcolo statistiche globali...")
        stats = self.compute_stats_func(self.ratings_df)

        self.results['data_stats'] = stats

        print("✅ Caricamento dati completato")

        return stats

    def sort_by_timestamp(self) -> Dict:
        """
        Ordina tutti i rating per timestamp.

        Returns:
            Dict con info ordinamento
        """
        print("\n" + "="*80)
        print("STEP 2: ORDINAMENTO CRONOLOGICO")
        print("="*80)

        print("\n2.1 Ordinamento per timestamp...")
        self.ratings_sorted = self.sort_by_timestamp_func(self.ratings_df)

        # Info ordinamento
        timestamp_info = {
            'min_timestamp': self.ratings_sorted['timestamp'].min(),
            'max_timestamp': self.ratings_sorted['timestamp'].max(),
            'total_events': len(self.ratings_sorted),
            'date_range': pd.to_datetime(self.ratings_sorted['timestamp'], unit='s').dt.date.tolist()
        }

        print(f"\n2.2 Info ordinamento:")
        print(f"   - Prima data: {timestamp_info['date_range'][0]}")
        print(f"   - Ultima data: {timestamp_info['date_range'][-1]}")
        print(f"   - Eventi totali: {timestamp_info['total_events']:,}")

        self.results['timestamp_info'] = timestamp_info

        print("✅ Ordinamento completato")

        return timestamp_info

    def initialize_mab(self) -> Dict:
        """
        Inizializza KBRS e MAB.

        Returns:
            Dict con info inizializzazione
        """
        print("\n" + "="*80)
        print("STEP 3: INIZIALIZZAZIONE KBRS + MAB")
        print("="*80)

        print("\n3.1 Inizializzazione KBRS...")
        self.kbrs_wrapper = self.kbrs_wrapper_class(self.movies_compressed)

        print(f"\n3.2 Inizializzazione Simulator...")
        self.simulator = self.simulator_class()

        print(f"\n3.3 Inizializzazione MAB ({self.config['mab_type']})...")
        self.mab = self.simulator.initialize_mab(
            mab_type=self.config['mab_type'],
            n_features=self.kbrs_wrapper.n_features * 2,  # user + movie features
            alpha=self.config['mab_alpha']
        )

        print(f"\n3.4 Inizializzazione Reward System...")
        reward_system = self.simulator.initialize_reward_system(
            weight_exploration=self.config['weight_exploration'],
            weight_accuracy=self.config['weight_accuracy'],
            threshold=self.config['reward_threshold']
        )

        init_info = {
            'kbrs_features': self.kbrs_wrapper.n_features,
            'mab_type': self.config['mab_type'],
            'mab_alpha': self.config['mab_alpha'],
            'n_features_total': self.kbrs_wrapper.n_features * 2,
            'reward_threshold': self.config['reward_threshold']
        }

        self.results['initialization'] = init_info

        print("✅ Inizializzazione completata")

        return init_info

    def run_simulation(self) -> Dict:
        """
        Esegue simulazione replay.

        Returns:
            Dict con risultati simulazione
        """
        print("\n" + "="*80)
        print("STEP 4: SIMULAZIONE REPLAY")
        print("="*80)

        print("\n4.1 Setup simulazione...")
        max_iter = self.config['max_iterations']
        if max_iter:
            print(f"   Limitato a {max_iter:,} iterazioni")
        else:
            print(f"   Utilizzerà tutti i {len(self.ratings_sorted):,} eventi")

        print("\n4.2 Esecuzione simulazione...")
        print("   Ad ogni evento:")
        print("     1. MAB decide quale film raccomandare")
        print("     2. Se coincide con evento reale → reward + update")
        print("     3. Altrimenti → ignora")

        # Reward system
        reward_system = self.simulator.initialize_reward_system(
            weight_exploration=self.config['weight_exploration'],
            weight_accuracy=self.config['weight_accuracy'],
            threshold=self.config['reward_threshold']
        )

        # Esegui simulazione
        simulation_results = self.simulator.run_temporal_simulation(
            ratings_sorted_df=self.ratings_sorted,
            kbrs_wrapper=self.kbrs_wrapper,
            mab=self.mab,
            reward_system=reward_system,
            max_iterations=max_iter
        )

        self.results['simulation'] = simulation_results

        print("✅ Simulazione completata")

        return simulation_results

    def analyze_results(self) -> Dict:
        """
        Analizza i risultati della simulazione.

        Returns:
            Dict con analisi
        """
        print("\n" + "="*80)
        print("STEP 5: ANALISI RISULTATI")
        print("="*80)

        sim_results = self.results['simulation']

        print("\n5.1 Metriche finali...")
        analysis = {
            'final_cumulative_reward': sim_results['final_cumulative_reward'],
            'final_ctr': sim_results['final_ctr'],
            'total_events': sim_results['total_events'],
            'n_hits': sim_results['n_hits'],
            'hit_rate': sim_results['n_hits'] / sim_results['total_events'] if sim_results['total_events'] > 0 else 0,
            'mab_type': sim_results['mab_type']
        }

        print(f"\n5.2 Risultati finali:")
        print(f"   Cumulative Reward: {analysis['final_cumulative_reward']:.0f}")
        print(f"   CTR: {analysis['final_ctr']:.4f}")
        print(f"   Hit rate: {analysis['hit_rate']:.1%}")
        print(f"   Eventi: {analysis['total_events']:,}")
        print(f"   MAB: {analysis['mab_type']}")

        # Learning speed (slope ultima parte)
        print("\n5.3 Analisi learning speed...")
        cumulative_rewards = sim_results['cumulative_rewards']
        if len(cumulative_rewards) > 1000:
            last_1000 = cumulative_rewards[-1000:]
            learning_speed = (last_1000[-1] - last_1000[0]) / 1000
            analysis['learning_speed'] = learning_speed
            print(f"   Learning speed (ultimi 1000): {learning_speed:.6f}")

        self.results['analysis'] = analysis

        print("✅ Analisi completata")

        return analysis

    def compare_algorithms(self, other_results: Dict[str, Dict] = None) -> Dict:
        """
        Confronta con altri algoritmi MAB.

        Args:
            other_results: Dict con risultati altri algoritmi

        Returns:
            Dict con confronto
        """
        print("\n" + "="*80)
        print("STEP 6: CONFRONTO ALGORITMI")
        print("="*80)

        print("\n6.1 Preparazione confronto...")

        # Prepara risultati per confronto
        results_dict = {self.config['mab_type']: self.results['simulation']}

        # Aggiungi altri risultati se forniti
        if other_results:
            results_dict.update(other_results)

        print(f"   Algoritmi da confrontare: {list(results_dict.keys())}")

        print("\n6.2 Esecuzione confronto...")
        comparison_df = self.simulator.compare_algorithms(results_dict)

        print("\n6.3 Risultati confronto:")
        for _, row in comparison_df.iterrows():
            print(f"   {row['Algorithm']}: CTR={row['Final_CTR']:.4f}, "
                  f"Reward={row['Final_Cumulative_Reward']:.0f}")

        comparison = {
            'comparison_df': comparison_df.to_dict('records'),
            'winner': comparison_df.iloc[0]['Algorithm'] if len(comparison_df) > 0 else self.config['mab_type']
        }

        self.results['comparison'] = comparison

        print("✅ Confronto completato")

        return comparison

    def generate_report(self):
        """
        Genera report completo dei risultati.
        """
        print("\n" + "="*80)
        print("STEP 7: GENERAZIONE REPORT")
        print("="*80)

        output_dir = Path(self.config['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)

        print("\n7.1 Salvataggio risultati...")
        from src.common.utils import save_results
        save_results(self.results, f"{self.config['output_dir']}/complete_results.json")

        print("\n7.2 Creazione grafici...")
        sim_results = self.results['simulation']
        self.simulator.plot_learning_curve(
            sim_results,
            f"{self.config['output_dir']}/learning_curve.png"
        )

        print("\n7.3 Salvataggio report testuale...")
        report_path = output_dir / "simulation_report.txt"
        with open(report_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write("ONLINE SIMULATION REPORT\n")
            f.write("="*80 + "\n\n")

            f.write(f"MAB Type: {self.results['analysis']['mab_type']}\n")
            f.write(f"Total Events: {self.results['analysis']['total_events']:,}\n")
            f.write(f"Final CTR: {self.results['analysis']['final_ctr']:.4f}\n")
            f.write(f"Cumulative Reward: {self.results['analysis']['final_cumulative_reward']:.0f}\n\n")

            f.write("TIMELINE:\n")
            f.write("-"*80 + "\n")
            ts_info = self.results['timestamp_info']
            f.write(f"Start: {pd.to_datetime(ts_info['min_timestamp'], unit='s')}\n")
            f.write(f"End: {pd.to_datetime(ts_info['max_timestamp'], unit='s')}\n")

        print("✅ Report generato")

    def run_full_pipeline(self) -> Dict:
        """
        Esegue l'intera pipeline online.

        Returns:
            Dict con tutti i risultati
        """
        print("\n" + "#"*80)
        print("# ONLINE PIPELINE - DUAL EVALUATION (EFFICACY)")
        print("#"*80)

        try:
            # Step 1: Load all data
            self.load_all_data()

            # Step 2: Sort by timestamp
            self.sort_by_timestamp()

            # Step 3: Initialize MAB
            self.initialize_mab()

            # Step 4: Run simulation
            self.run_simulation()

            # Step 5: Analyze results
            self.analyze_results()

            # Step 6: Generate report
            self.generate_report()

            print("\n" + "#"*80)
            print("# PIPELINE ONLINE COMPLETATA CON SUCCESSO")
            print("#"*80)

            return self.results

        except Exception as e:
            print(f"\n❌ Errore nella pipeline: {e}")
            raise
