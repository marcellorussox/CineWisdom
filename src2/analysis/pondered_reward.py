"""
Module: pondered_reward

Analytical computation of Pondered Reward metric from MAB simulation history.

The Pondered Reward (R_P) balances two components:
- R_A (Exploration Reward): Binary reward for predicting ≥4.0 on UNSEEN items
- R_G (General Accuracy Proxy): Model's general accuracy rate (computed dynamically from simulation)

Formula: R_P = 0.5 * R_A + 0.5 * R_G^proxy

This module provides functions to:
1. Load MAB history from CSV
2. Compute pondered rewards per iteration
3. Calculate cumulative average rewards
4. Visualize comparative performance

NOTE: kbrs_general_accuracy should be obtained from the simulator's get_kbrs_general_accuracy() method,
not hardcoded to avoid stale values.
"""
from __future__ import annotations

from typing import Optional, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class PonderedRewardAnalyzer:
    """
    Analyzer for computing and visualizing Pondered Reward metrics from MAB history.

    Parameters
    ----------
    history_df : pd.DataFrame
        MAB simulation history with columns: iteration, model_name, reward
    kbrs_general_accuracy : float
        General accuracy proxy for KBRS_Hybrid (should be obtained from simulator.get_kbrs_general_accuracy())
    baseline_general_accuracy : float
        General accuracy proxy for Popularity_Baseline (default: 0.0)
    """

    def __init__(
        self,
        history_df: pd.DataFrame,
        kbrs_general_accuracy: float,
        baseline_general_accuracy: float = 0.0,
        plot: bool = False,
        save_plot_path: Optional[str] = None,
    ):
        required_cols = {"iteration", "model_name", "reward"}
        missing = required_cols - set(history_df.columns)
        if missing:
            raise ValueError(f"history_df missing required columns: {sorted(missing)}")

        self.history_df = history_df.copy()
        self.kbrs_accuracy = kbrs_general_accuracy
        self.baseline_accuracy = baseline_general_accuracy
        self.plot = plot
        self.save_plot_path = save_plot_path

        # Compute pondered rewards
        self._compute_pondered_rewards()

        # Generate plot if requested
        if self.plot:
            self._generate_plot()
    
    def _compute_pondered_rewards(self) -> None:
        """
        Compute pondered reward (R_P) for each iteration.

        R_P = 0.5 * R_A + 0.5 * R_G^proxy

        Where:
        - R_A is the exploration reward (from history_df['reward'])
        - R_G^proxy is obtained from kbrs_general_accuracy parameter
        """
        # Map model names to their general accuracy proxy
        self.history_df['R_G_proxy'] = self.history_df['model_name'].map({
            'KBRS_Hybrid': self.kbrs_accuracy,
            'Popularity_Baseline': self.baseline_accuracy,
        })
        
        # Handle any unmapped model names (default to 0.0)
        # Fixed: Avoid pandas FutureWarning by not using inplace on chained assignment
        self.history_df['R_G_proxy'] = self.history_df['R_G_proxy'].fillna(0.0)
        
        # R_A is the original exploration reward
        self.history_df['R_A'] = self.history_df['reward']
        
        # Compute pondered reward: R_P = 0.5 * R_A + 0.5 * R_G
        self.history_df['R_P'] = (
            0.5 * self.history_df['R_A'] + 
            0.5 * self.history_df['R_G_proxy']
        )
    
    def compute_cumulative_metrics(self) -> pd.DataFrame:
        """
        Compute cumulative average rewards for both R_A and R_P.
        
        Returns
        -------
        pd.DataFrame
            DataFrame with columns: iteration, cum_avg_R_A, cum_avg_R_P
        """
        df = self.history_df.copy()
        
        # Cumulative sums
        df['cumsum_R_A'] = df['R_A'].cumsum()
        df['cumsum_R_P'] = df['R_P'].cumsum()
        
        # Cumulative averages
        iterations = df['iteration'] + 1  # iteration is 0-indexed
        df['cum_avg_R_A'] = df['cumsum_R_A'] / iterations
        df['cum_avg_R_P'] = df['cumsum_R_P'] / iterations
        
        return df[['iteration', 'cum_avg_R_A', 'cum_avg_R_P']]
    
    def get_final_metrics(self) -> dict:
        """
        Get final cumulative average rewards.
        
        Returns
        -------
        dict
            Dictionary with keys: final_R_A, final_R_P, n_iterations
        """
        cum_metrics = self.compute_cumulative_metrics()
        final_row = cum_metrics.iloc[-1]
        
        return {
            'final_R_A': float(final_row['cum_avg_R_A']),
            'final_R_P': float(final_row['cum_avg_R_P']),
            'n_iterations': int(final_row['iteration']) + 1,
        }
    
    def plot_comparative_rewards(
        self,
        figsize: Tuple[int, int] = (14, 6),
        save_path: Optional[str] = None,
        show: bool = True,
    ) -> None:
        """
        Plot cumulative average rewards for both R_A and R_P metrics.
        
        Parameters
        ----------
        figsize : Tuple[int, int]
            Figure size (width, height)
        save_path : Optional[str]
            Path to save the plot (if None, plot is not saved)
        show : bool
            Whether to display the plot
        """
        cum_metrics = self.compute_cumulative_metrics()
        
        plt.figure(figsize=figsize)
        
        # Plot R_A (Exploration Reward)
        plt.plot(
            cum_metrics['iteration'],
            cum_metrics['cum_avg_R_A'],
            label='R_A (Exploration: Unseen ≥4.0)',
            color='green',
            linewidth=2,
            alpha=0.8,
        )
        
        # Plot R_P (Pondered Reward)
        plt.plot(
            cum_metrics['iteration'],
            cum_metrics['cum_avg_R_P'],
            label='R_P (Pondered: 0.5·R_A + 0.5·R_G)',
            color='blue',
            linewidth=2,
            alpha=0.8,
        )
        
        # Get final values for annotation
        final_metrics = self.get_final_metrics()
        
        plt.title(
            'Comparative MAB Performance: Exploration vs Pondered Reward',
            fontsize=14,
            fontweight='bold',
        )
        plt.xlabel('Iteration', fontsize=12)
        plt.ylabel('Cumulative Average Reward', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.4)
        plt.legend(fontsize=11, loc='best')
        
        # Add final values as text annotation
        textstr = '\n'.join([
            f"Final R_A: {final_metrics['final_R_A']:.4f}",
            f"Final R_P: {final_metrics['final_R_P']:.4f}",
        ])
        plt.text(
            0.02, 0.98, textstr,
            transform=plt.gca().transAxes,
            fontsize=10,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
        )
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Plot saved to: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()

    def _generate_plot(self) -> None:
        """
        Internal method to generate plot when plot=True is passed to __init__.
        Uses self.save_plot_path if provided.
        """
        self.plot_comparative_rewards(
            save_path=self.save_plot_path,
            show=True
        )

    def generate_summary_report(self) -> str:
        """
        Generate a text summary report of the pondered reward analysis.
        
        Returns
        -------
        str
            Formatted summary report
        """
        final_metrics = self.get_final_metrics()
        
        # Model selection statistics
        model_counts = self.history_df['model_name'].value_counts()
        total_iters = final_metrics['n_iterations']
        
        report = []
        report.append("=" * 70)
        report.append("PONDERED REWARD ANALYSIS SUMMARY")
        report.append("=" * 70)
        report.append("")
        report.append("METRIC DEFINITIONS:")
        report.append(f"  • R_A (Exploration): Binary reward for UNSEEN items with pred ≥ 4.0")
        report.append(f"  • R_G (General Accuracy Proxy): {self.kbrs_accuracy} for KBRS_Hybrid")
        report.append(f"  • R_P (Pondered): 0.5 × R_A + 0.5 × R_G")
        report.append("")
        report.append("FINAL CUMULATIVE AVERAGE REWARDS:")
        report.append(f"  • R_A (Exploration):     {final_metrics['final_R_A']:.4f}")
        report.append(f"  • R_P (Pondered):        {final_metrics['final_R_P']:.4f}")
        report.append(f"  • Improvement:           {final_metrics['final_R_P'] - final_metrics['final_R_A']:.4f}")
        report.append(f"  • Relative Gain:         {((final_metrics['final_R_P'] / final_metrics['final_R_A']) - 1) * 100:.2f}%")
        report.append("")
        report.append("MODEL SELECTION STATISTICS:")
        for model_name, count in model_counts.items():
            pct = (count / total_iters) * 100
            report.append(f"  • {model_name:25s}: {count:6d} ({pct:5.2f}%)")
        report.append("")
        report.append(f"Total Iterations: {total_iters}")
        report.append("=" * 70)
        
        return "\n".join(report)


def load_and_analyze(
    history_path: str,
    kbrs_general_accuracy: float,
    baseline_general_accuracy: float = 0.0,
    plot: bool = True,
    save_plot_path: Optional[str] = None,
) -> PonderedRewardAnalyzer:
    """
    Convenience function to load MAB history and perform pondered reward analysis.

    Parameters
    ----------
    history_path : str
        Path to MAB history CSV file
    kbrs_general_accuracy : float
        General accuracy proxy for KBRS_Hybrid (should be obtained from simulator.get_kbrs_general_accuracy())
    baseline_general_accuracy : float
        General accuracy proxy for Popularity_Baseline (default: 0.0)
    plot : bool
        Whether to generate comparative plot
    save_plot_path : Optional[str]
        Path to save the plot (if None, plot is not saved)
    
    Returns
    -------
    PonderedRewardAnalyzer
        Analyzer instance with computed metrics
    
    Example
    -------
    >>> analyzer = load_and_analyze(
    ...     'datasets/processed/mab_history.csv',
    ...     plot=True,
    ...     save_plot_path='plots/pondered_reward_comparison.png'
    ... )
    >>> print(analyzer.generate_summary_report())
    """
    # Load history
    history_df = pd.read_csv(history_path)
    print(f"Loaded MAB history: {len(history_df)} iterations")
    
    # Create analyzer
    analyzer = PonderedRewardAnalyzer(
        history_df=history_df,
        kbrs_general_accuracy=kbrs_general_accuracy,
        baseline_general_accuracy=baseline_general_accuracy,
    )
    
    # Generate plot if requested
    if plot:
        analyzer.plot_comparative_rewards(save_path=save_plot_path)
    
    # Print summary
    print("\n" + analyzer.generate_summary_report())
    
    return analyzer


# Example usage for notebook/script
if __name__ == "__main__":
    # Example: Load and analyze MAB history
    # Note: Get R_G dynamically from simulator, not hardcoded!
    # simulator = MABSimulator(...)
    # history = simulator.run_simulation(...)
    # kbrs_rg = simulator.get_kbrs_general_accuracy()
    #
    # analyzer = load_and_analyze(
    #     history_path="datasets/processed/mab_history.csv",
    #     kbrs_general_accuracy=kbrs_rg,
    #     baseline_general_accuracy=0.0,
    #     plot=True,
    #     save_plot_path="plots/pondered_reward_comparison.png",
    # )
    
    # Access final metrics programmatically
    final_metrics = analyzer.get_final_metrics()
    print(f"\nFinal R_P: {final_metrics['final_R_P']:.4f}")
