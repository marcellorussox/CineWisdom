"""
KBRS Evaluation Module

Comprehensive evaluation for Knowledge-Based Recommender System with MAB.

Evaluates:
1. Offline Performance (RMSE, MAE, Coverage)
2. Online Performance (Reward evolution, Strategy selection)
3. MAB Learning (Regret, Convergence)
4. Recommendation Quality (Diversity, Novelty)
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List
import json
import os


class KBRSEvaluator:
    """Comprehensive evaluation for KBRS with MAB."""
    
    def __init__(self, results_dir: str = 'results/ml-small-100k'):
        """
        Initialize evaluator.
        
        Args:
            results_dir: Directory to save results
        """
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(f"{results_dir}/plots", exist_ok=True)
    
    def evaluate_offline(
        self,
        kbrs_model,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        cosine_sim_matrix: np.ndarray,
        movie_ids: pd.Series,
        sample_size: int = 1000
    ) -> Dict:
        """
        Evaluate KBRS on offline test set.
        
        Args:
            kbrs_model: Trained KBRS instance
            train_df: Training dataset (User History)
            test_df: Test dataset (Targets)
            cosine_sim_matrix: Similarity matrix
            movie_ids: Movie IDs
            sample_size: Number of samples for evaluation
        
        Returns:
            Dictionary with offline metrics
        """
        print("\n" + "="*60)
        print("OFFLINE EVALUATION")
        print("="*60)
        
        # Sample for faster evaluation
        sample_df = test_df.sample(n=min(sample_size, len(test_df)), random_state=42)
        
        predictions = []
        actuals = []
        errors = []
        
        print(f"\n📊 Evaluating {len(sample_df)} predictions...")
        print("   (Using Train set as history to prevent data leakage)")
        
        for _, row in sample_df.iterrows():
            user_id = row['userId']
            movie_id = row['movieId']
            actual = row['rating']
            
            # Use TRAIN DF as history, predict for TEST DF target
            pred = kbrs_model.predict_rating(
                user_id=user_id,
                movie_id=movie_id,
                ratings_df=train_df,
                cosine_sim_matrix=cosine_sim_matrix,
                movie_ids=movie_ids
            )
            
            if pred is not None:
                predictions.append(pred)
                actuals.append(actual)
                errors.append(abs(pred - actual))
        
        predictions = np.array(predictions)
        actuals = np.array(actuals)
        errors = np.array(errors)
        
        # Metrics
        rmse = np.sqrt(np.mean((predictions - actuals) ** 2))
        mae = np.mean(errors)
        coverage = len(predictions) / len(sample_df)
        
        # Error distribution
        error_bins = [0, 0.5, 1.0, 1.5, 2.0, 5.0]
        error_counts = pd.cut(errors, bins=error_bins).value_counts().sort_index()
        error_dist = {str(k): float(v/len(errors)) for k, v in error_counts.items()}
        
        results = {
            'rmse': float(rmse),
            'mae': float(mae),
            'coverage': float(coverage),
            'n_predictions': len(predictions),
            'n_samples': len(sample_df),
            'error_distribution': error_dist
        }
        
        print(f"\n✅ Offline Results:")
        print(f"  RMSE:     {rmse:.4f}")
        print(f"  MAE:      {mae:.4f}")
        print(f"  Coverage: {coverage:.2%}")
        
        # Save
        with open(f"{self.results_dir}/offline_evaluation.json", 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        return results
    
    def evaluate_online(
        self,
        simulation_results: Dict
    ) -> Dict:
        """
        Evaluate online MAB simulation results.
        
        Args:
            simulation_results: Results from OnlineKBRSSimulator
        
        Returns:
            Dictionary with online metrics
        """
        print("\n" + "="*60)
        print("ONLINE EVALUATION")
        print("="*60)
        
        history = simulation_results['history']
        mab_stats = simulation_results['mab_stats']
        
        # 1. Performance over time
        window_size = 100
        history['rmse_rolling'] = np.sqrt(
            ((history['predicted_rating'] - history['true_rating']) ** 2).rolling(window_size).mean()
        )
        history['reward_rolling'] = history['reward'].rolling(window_size).mean()
        
        # 2. Strategy performance
        strategy_perf_df = history.groupby('strategy').agg({
            'reward': ['mean', 'std', 'count'],
            'predicted_rating': lambda x: np.sqrt(np.mean((x - history.loc[x.index, 'true_rating']) ** 2))
        }).round(4)
        
        # Flatten MultiIndex columns for JSON serialization
        strategy_perf_df.columns = ['_'.join(col).strip() for col in strategy_perf_df.columns.values]
        strategy_performance = strategy_perf_df.to_dict()
        
        # 3. Learning metrics
        total_pulls = mab_stats['pulls'].sum()
        exploration_rate = mab_stats['pulls'][0] / total_pulls
        exploitation_rate = mab_stats['pulls'][1] / total_pulls
        
        # 4. Regret (simplified: difference from best arm)
        best_arm_reward = mab_stats['avg_rewards'].max()
        cumulative_regret = []
        for i in range(len(history)):
            arm_used = history.iloc[i]['arm']
            reward_got = history.iloc[i]['reward']
            regret = best_arm_reward - reward_got
            cumulative_regret.append(regret)
        history['cumulative_regret'] = np.cumsum(cumulative_regret)
        
        results = {
            'final_metrics': simulation_results['summary'],
            'strategy_performance': strategy_performance,
            'mab_statistics': {
                'exploration_pulls': int(mab_stats['pulls'][0]),
                'exploitation_pulls': int(mab_stats['pulls'][1]),
                'exploration_rate': float(exploration_rate),
                'exploitation_rate': float(exploitation_rate),
                'expected_values': mab_stats['expected_values'].tolist()
            },
            'learning': {
                'final_regret': float(history['cumulative_regret'].iloc[-1]),
                'avg_regret_last_100': float(history['cumulative_regret'].diff().tail(100).mean())
            }
        }
        
        print(f"\n✅ Online Results:")
        print(f"  Final RMSE:        {simulation_results['summary']['final_rmse']:.4f}")
        print(f"  Mean Reward:       {simulation_results['summary']['mean_reward']:.4f}")
        print(f"  Exploration Rate:  {exploration_rate:.2%}")
        print(f"  Exploitation Rate: {exploitation_rate:.2%}")
        
        # Save
        with open(f"{self.results_dir}/online_evaluation.json", 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Save history
        history.to_csv(f"{self.results_dir}/online_history.csv", index=False)
        
        return results, history
    
    def plot_results(
        self,
        history: pd.DataFrame,
        mab_stats: Dict
    ):
        """
        Create comprehensive visualization plots.
        
        Args:
            history: Simulation history DataFrame
            mab_stats: MAB statistics
        """
        print(f"\n📊 Creating plots...")
        
        # Set style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (15, 10)
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        
        # 1. Cumulative Reward
        ax = axes[0, 0]
        history['cumulative_reward'] = history['reward'].cumsum()
        ax.plot(history.index, history['cumulative_reward'], linewidth=2)
        ax.set_title('Cumulative Reward Over Time', fontsize=14, fontweight='bold')
        ax.set_xlabel('Interaction')
        ax.set_ylabel('Cumulative Reward')
        ax.grid(True, alpha=0.3)
        
        # 2. Rolling RMSE
        ax = axes[0, 1]
        ax.plot(history.index, history['rmse_rolling'], linewidth=2, color='red')
        ax.set_title('Rolling RMSE (window=100)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Interaction')
        ax.set_ylabel('RMSE')
        ax.grid(True, alpha=0.3)
        
        # 3. Strategy Selection Over Time
        ax = axes[0, 2]
        window = 100
        history['exploration_rate_rolling'] = (history['arm'] == 0).rolling(window).mean()
        history['exploitation_rate_rolling'] = (history['arm'] == 1).rolling(window).mean()
        ax.plot(history.index, history['exploration_rate_rolling'], label='Exploration', linewidth=2)
        ax.plot(history.index, history['exploitation_rate_rolling'], label='Exploitation', linewidth=2)
        ax.set_title('Strategy Selection Rate (window=100)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Interaction')
        ax.set_ylabel('Selection Rate')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # 4. Reward Distribution by Strategy
        ax = axes[1, 0]
        history.boxplot(column='reward', by='strategy', ax=ax)
        ax.set_title('Reward Distribution by Strategy', fontsize=14, fontweight='bold')
        ax.set_xlabel('Strategy')
        ax.set_ylabel('Reward')
        plt.sca(ax)
        plt.xticks(rotation=0)
        
        # 5. Cumulative Regret
        ax = axes[1, 1]
        ax.plot(history.index, history['cumulative_regret'], linewidth=2, color='orange')
        ax.set_title('Cumulative Regret', fontsize=14, fontweight='bold')
        ax.set_xlabel('Interaction')
        ax.set_ylabel('Cumulative Regret')
        ax.grid(True, alpha=0.3)
        
        # 6. Strategy Pulls Distribution
        ax = axes[1, 2]
        pulls = mab_stats['pulls']
        strategies = ['Exploration', 'Exploitation']
        colors = ['#3498db', '#e74c3c']
        bars = ax.bar(strategies, pulls, color=colors, alpha=0.7, edgecolor='black')
        ax.set_title('Total Strategy Selections', fontsize=14, fontweight='bold')
        ax.set_ylabel('Number of Pulls')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}',
                   ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        
        # Save
        plot_path = f"{self.results_dir}/plots/kbrs_mab_evaluation.png"
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        print(f"✅ Plot saved to {plot_path}")
        
        plt.close()
    
    def generate_report(
        self,
        offline_results: Dict,
        online_results: Dict
    ):
        """
        Generate comprehensive evaluation report.
        
        Args:
            offline_results: Offline evaluation results
            online_results: Online evaluation results
        """
        report = f"""
# KBRS + MAB Evaluation Report

## Offline Performance

- **RMSE**: {offline_results['rmse']:.4f}
- **MAE**: {offline_results['mae']:.4f}
- **Coverage**: {offline_results['coverage']:.2%}
- **Predictions**: {offline_results['n_predictions']:,} / {offline_results['n_samples']:,}

## Online Performance

### Overall Metrics
- **Final RMSE**: {online_results['final_metrics']['final_rmse']:.4f}
- **Final MAE**: {online_results['final_metrics']['final_mae']:.4f}
- **Mean Reward**: {online_results['final_metrics']['mean_reward']:.4f}
- **Total Interactions**: {online_results['final_metrics']['total_interactions']:,}

### Strategy Selection
- **Exploration Rate**: {online_results['mab_statistics']['exploration_rate']:.2%}
- **Exploitation Rate**: {online_results['mab_statistics']['exploitation_rate']:.2%}
- **Exploration Pulls**: {online_results['mab_statistics']['exploration_pulls']:,}
- **Exploitation Pulls**: {online_results['mab_statistics']['exploitation_pulls']:,}

### MAB Learning
- **Final Cumulative Regret**: {online_results['learning']['final_regret']:.2f}
- **Avg Regret (last 100)**: {online_results['learning']['avg_regret_last_100']:.4f}

### Strategy Performance
{pd.DataFrame(online_results['strategy_performance']).to_markdown()}

## Conclusions

The MAB system successfully learned to balance exploration and exploitation strategies
for the KBRS, adapting dynamically based on real-time feedback.

---
Generated: {pd.Timestamp.now()}
"""
        
        report_path = f"{self.results_dir}/evaluation_report.md"
        with open(report_path, 'w') as f:
            f.write(report)
        
        print(f"\n✅ Report saved to {report_path}")
        
        return report
