"""
Offline Pipeline - Orchestratore pipeline offline

Pipeline completa per valutazione offline KBRS con metriche tradizionali.
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from pathlib import Path


class OfflinePipeline:
    """
    Pipeline offline completa.

    Gestisce tutto il flusso:
    1. Load e preprocessing dati
    2. Creazione split
    3. Training KBRS (lazy learning)
    4. Validazione su validation set
    5. Test finale su test set
    6. Generazione report
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Inizializza la pipeline offline.

        Args:
            config: Dict con configurazione (test_size, val_size, etc.)
        """
        self.config = config or {
            'test_size': 0.1,
            'val_size': 0.1,
            'min_ratings_per_user': 5,
            'random_state': 42,
            'n_components_svd': 4096,
            'output_dir': 'results/offline'
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
            create_data_splits,
            validate_data,
            compute_global_statistics
        )
        from src.offline.offline_evaluator import OfflineEvaluator

        self.load_data_func = load_movielens_data
        self.enrich_data_func = enrich_with_dbpedia
        self.normalize_data_func = normalize_catalog
        self.compress_data_func = compress_features

        self.filter_users_func = filter_users_by_rating_count
        self.create_splits_func = create_data_splits
        self.validate_data_func = validate_data
        self.compute_stats_func = compute_global_statistics

        self.kbrs_wrapper_class = KBRSWrapper
        self.evaluator_class = OfflineEvaluator

        # Data
        self.ratings_df = None
        self.movies_df = None
        self.links_df = None
        self.movies_enriched = None
        self.movies_normalized = None
        self.movies_compressed = None

        # Splits
        self.train_df = None
        self.val_df = None
        self.test_df = None

        # KBRS
        self.kbrs_wrapper = None

        # Results
        self.results = {}

        print("✅ Offline Pipeline inizializzata")

    def load_and_prepare_data(self) -> Dict:
        """
        Carica e prepara i dati MovieLens.

        Returns:
            Dict con statistiche dei dati
        """
        print("\n" + "="*80)
        print("STEP 1: CARICAMENTO E PREPARAZIONE DATI")
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
            n_components=self.config['n_components_svd']
        )

        # Filtra utenti con pochi rating
        print("\n1.5 Filtraggio utenti...")
        self.ratings_df = self.filter_users_func(
            self.ratings_df,
            min_ratings=self.config['min_ratings_per_user']
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

        print("✅ Preparazione dati completata")

        return stats

    def create_splits(self) -> Dict:
        """
        Crea split train/validation/test.

        Returns:
            Dict con statistiche degli split
        """
        print("\n" + "="*80)
        print("STEP 2: CREAZIONE SPLIT")
        print("="*80)

        print("\n2.1 Creazione split stratificati...")
        self.train_df, self.val_df, self.test_df = self.create_splits_func(
            self.ratings_df,
            test_size=self.config['test_size'],
            val_size=self.config['val_size'],
            random_state=self.config['random_state'],
            min_ratings_per_user=self.config['min_ratings_per_user']
        )

        print("\n2.2 Salvataggio split...")
        from src.offline.split_manager_wrapper import save_splits
        save_splits(self.train_df, self.val_df, self.test_df, "datasets/splits")

        # Statistiche split
        from src.offline.split_manager_wrapper import get_split_statistics
        split_stats = get_split_statistics(self.train_df, self.val_df, self.test_df)

        self.results['split_stats'] = split_stats

        print("✅ Creazione split completata")

        return split_stats

    def train_kbrs(self):
        """
        Addestra KBRS (lazy learning, no-op).

        KBRS è lazy, non richiede training esplicito.
        """
        print("\n" + "="*80)
        print("STEP 3: TRAINING KBRS")
        print("="*80)

        print("\n3.1 Inizializzazione KBRS Engine...")
        self.kbrs_wrapper = self.kbrs_wrapper_class(self.movies_compressed)

        print("\n3.2 Note su lazy learning:")
        print("   - KBRS è un algoritmo lazy, non richiede training")
        print("   - Impara da user patterns durante le predizioni")
        print("   - Usa similarità coseno tra feature film")

        self.results['training_time'] = 0.0  # Non c'è training

        print("✅ Training KBRS completato")

    def validate_model(self) -> Dict:
        """
        Valida il modello su validation set.

        Returns:
            Dict con risultati validazione
        """
        print("\n" + "="*80)
        print("STEP 4: VALIDAZIONE MODELLO")
        print("="*80)

        print("\n4.1 Inizializzazione Evaluator...")
        evaluator = self.evaluator_class()

        print("\n4.2 Predizione su validation set...")
        predictions, ground_truth = evaluator.predict_all_ratings(
            self.kbrs_wrapper,
            self.val_df,
            self.train_df
        )

        print("\n4.3 Valutazione...")
        val_results = evaluator.evaluate_model(
            predictions,
            ground_truth,
            model_name="KBRS_Validation"
        )

        self.results['validation'] = val_results

        print(f"\n4.4 Risultati validazione:")
        print(f"   RMSE: {val_results['rmse']:.4f}")
        print(f"   MAE: {val_results['mae']:.4f}")

        print("✅ Validazione completata")

        return val_results

    def test_model(self) -> Dict:
        """
        Test finale del modello su test set.

        Returns:
            Dict con risultati test
        """
        print("\n" + "="*80)
        print("STEP 5: TEST FINALE")
        print("="*80)

        print("\n5.1 Inizializzazione Evaluator...")
        evaluator = self.evaluator_class()

        print("\n5.2 Predizione su test set...")
        predictions, ground_truth = evaluator.predict_all_ratings(
            self.kbrs_wrapper,
            self.test_df,
            self.train_df
        )

        print("\n5.3 Valutazione...")
        test_results = evaluator.evaluate_model(
            predictions,
            ground_truth,
            model_name="KBRS_Test"
        )

        self.results['test'] = test_results

        print(f"\n5.4 Risultati test:")
        print(f"   RMSE: {test_results['rmse']:.4f}")
        print(f"   MAE: {test_results['mae']:.4f}")
        print(f"   Precision@10: {test_results.get('precision@10', 'N/A')}")
        print(f"   NDCG@10: {test_results.get('ndcg@10', 'N/A')}")

        print("✅ Test completato")

        return test_results

    def compare_with_baseline(self) -> Dict:
        """
        Confronta KBRS con baseline popularity.

        Returns:
            Dict con confronto
        """
        print("\n" + "="*80)
        print("STEP 6: CONFRONTO CON BASELINE")
        print("="*80)

        print("\n6.1 Calcolo predizioni baseline...")
        # Baseline: rating medio globale
        baseline_predictions = [self.train_df['rating'].mean()] * len(self.test_df)
        ground_truth = self.test_df['rating'].tolist()

        print("\n6.2 Confronto...")
        evaluator = self.evaluator_class()
        comparison = evaluator.compare_with_baseline(
            predictions=self.results['test'].get('predictions', []),
            baseline_predictions=baseline_predictions,
            ground_truth=ground_truth,
            model_name="KBRS",
            baseline_name="Global_Average"
        )

        self.results['comparison'] = comparison

        print(f"\n6.3 Vincitore: {comparison['winner']}")

        print("✅ Confronto completato")

        return comparison

    def generate_report(self):
        """
        Genera report completo dei risultati.
        """
        print("\n" + "="*80)
        print("STEP 7: GENERAZIONE REPORT")
        print("="*80)

        print("\n7.1 Salvataggio risultati...")
        from src.common.utils import save_results
        save_results(self.results, f"{self.config['output_dir']}/complete_results.json")

        print("\n7.2 Generazione report testuale...")
        evaluator = self.evaluator_class()
        evaluator.generate_report(self.results['test'], self.config['output_dir'])

        print("\n7.3 Creazione visualizzazioni...")
        if 'comparison' in self.results:
            evaluator.plot_results(
                self.results['comparison'],
                f"{self.config['output_dir']}/metrics_comparison.png"
            )

        print("✅ Report generato")

    def run_full_pipeline(self) -> Dict:
        """
        Esegue l'intera pipeline offline.

        Returns:
            Dict con tutti i risultati
        """
        print("\n" + "#"*80)
        print("# OFFLINE PIPELINE - DUAL EVALUATION (ACCURACY)")
        print("#"*80)

        try:
            # Step 1: Load data
            self.load_and_prepare_data()

            # Step 2: Create splits
            self.create_splits()

            # Step 3: Train KBRS
            self.train_kbrs()

            # Step 4: Validate
            self.validate_model()

            # Step 5: Test
            self.test_model()

            # Step 6: Compare
            self.compare_with_baseline()

            # Step 7: Report
            self.generate_report()

            print("\n" + "#"*80)
            print("# PIPELINE OFFLINE COMPLETATA CON SUCCESSO")
            print("#"*80)

            return self.results

        except Exception as e:
            print(f"\n❌ Errore nella pipeline: {e}")
            raise
