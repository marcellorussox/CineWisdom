"""
Results Analyzer - API for analyzing MAB experiment results.

Questo modulo fornisce un'API per analizzare i risultati delle simulazioni MAB,
generare grafici e report.

Esempio d'uso:
```python
from src.analysis.results_analyzer import ResultsAnalyzer

# Inizializza analyzer con risultati
analyzer = ResultsAnalyzer(results_list)

# Genera grafici
analyzer.plot_comparison()
analyzer.plot_pondered_rewards()

# Genera report
report = analyzer.generate_report()
print(report)
```
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from src.viz.plot_manager import (
    plot_comparison_results,
    plot_pondered_rewards,
    create_summary_table,
)


@dataclass
class AnalysisResult:
    """Container for analysis results."""
    name: str
    history_df: pd.DataFrame
    selection_counts: pd.Series
    avg_reward: float
    kbrs_rg: float
    config: Dict[str, Any]


class ResultsAnalyzer:
    """
    Analyzer for MAB experiment results.

    Funzionalità:
    - Confronto risultati multipli
    - Analisi Pondered Reward
    - Generazione grafici
    - Report automatici
    """

    def __init__(self, results: List[AnalysisResult]):
        """
        Initialize analyzer.

        Args:
            results: List of AnalysisResult objects
        """
        self.results = results

    def create_comparison_dataframe(self) -> pd.DataFrame:
        """Create DataFrame for comparison."""
        comparison_data = []

        for result in self.results:
            comparison_data.append({
                'Simulazione': result.name,
                'KBRS Selection (%)': result.selection_counts.get('KBRS_Hybrid', 0),
                'Baseline Selection (%)': result.selection_counts.get('Popularity_Baseline', 0),
                'Average Reward': result.avg_reward,
                'KBRS R_G': result.kbrs_rg,
            })

        return pd.DataFrame(comparison_data)

    def plot_comparison(self, comparison_df: Optional[pd.DataFrame] = None) -> None:
        """
        Plot comparison of multiple experiments.

        Args:
            comparison_df: DataFrame with comparison results (se None, crea automaticamente)
        """
        if comparison_df is None:
            comparison_df = self.create_comparison_dataframe()

        plot_comparison_results(comparison_df)

    def plot_pondered_rewards(self, result_index: int = 0) -> None:
        """
        Plot Pondered Reward analysis for a specific experiment.

        Args:
            result_index: Index of experiment to analyze (default: 0)
        """
        if result_index >= len(self.results):
            raise ValueError(f"Result index {result_index} out of range")

        result = self.results[result_index]
        title = f"Pondered Reward Analysis - {result.name}"

        plot_pondered_rewards(result.history_df, title)

    def print_summary(self, comparison_df: Optional[pd.DataFrame] = None) -> None:
        """
        Print formatted summary table.

        Args:
            comparison_df: DataFrame with comparison results (se None, crea automaticamente)
        """
        if comparison_df is None:
            comparison_df = self.create_comparison_dataframe()

        create_summary_table(comparison_df)

    def analyze_kbrs_dominance(self, comparison_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Analyze KBRS dominance vs Baseline.

        Args:
            comparison_df: DataFrame with comparison results

        Returns:
            Dictionary with analysis metrics
        """
        if comparison_df is None:
            comparison_df = self.create_comparison_dataframe()

        kbrs_selections = comparison_df['KBRS Selection (%)']
        baseline_selections = comparison_df['Baseline Selection (%)']

        analysis = {
            'kbrs_dominance': kbrs_selections.mean(),
            'baseline_performance': baseline_selections.mean(),
            'variance': kbrs_selections.var(),
            'min_kbrs': kbrs_selections.min(),
            'max_kbrs': kbrs_selections.max(),
            'consistent_winner': 'KBRS' if kbrs_selections.min() > baseline_selections.max() else 'Mixed',
        }

        return analysis

    def analyze_reward_quality(self, comparison_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Analyze reward quality across experiments.

        Args:
            comparison_df: DataFrame with comparison results

        Returns:
            Dictionary with reward metrics
        """
        if comparison_df is None:
            comparison_df = self.create_comparison_dataframe()

        avg_rewards = comparison_df['Average Reward']
        kbrs_rgs = comparison_df['KBRS R_G']

        analysis = {
            'mean_reward': avg_rewards.mean(),
            'reward_variance': avg_rewards.var(),
            'mean_kbrs_rg': kbrs_rgs.mean(),
            'rg_variance': kbrs_rgs.var(),
            'rg_range': (kbrs_rgs.min(), kbrs_rgs.max()),
            'best_performing_config': comparison_df.loc[avg_rewards.idxmax(), 'Simulazione'],
            'worst_performing_config': comparison_df.loc[avg_rewards.idxmin(), 'Simulazione'],
        }

        return analysis

    def analyze_parameter_impact(self, param_name: str, comparison_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Analyze impact of a specific parameter on results.

        Args:
            param_name: Name of parameter to analyze
            comparison_df: DataFrame with comparison results

        Returns:
            Dictionary with parameter impact analysis
        """
        if comparison_df is None:
            comparison_df = self.create_comparison_dataframe()

        # Extract parameter values from configs
        param_values = []
        for result in self.results:
            param_values.append(result.config.get(param_name, None))

        comparison_df = comparison_df.copy()
        comparison_df[f'{param_name}_value'] = param_values

        # Analyze correlation
        correlations = {}
        for metric in ['KBRS Selection (%)', 'Average Reward', 'KBRS R_G']:
            corr = comparison_df[f'{param_name}_value'].corr(comparison_df[metric])
            correlations[metric] = corr

        analysis = {
            'parameter': param_name,
            'parameter_range': (min(param_values), max(param_values)),
            'correlations': correlations,
            'impact_assessment': self._assess_impact(correlations),
        }

        return analysis

    def _assess_impact(self, correlations: Dict[str, float]) -> str:
        """Assess overall impact based on correlations."""
        avg_corr = np.mean([abs(v) for v in correlations.values()])

        if avg_corr > 0.7:
            return "Strong impact"
        elif avg_corr > 0.4:
            return "Moderate impact"
        elif avg_corr > 0.2:
            return "Weak impact"
        else:
            return "No significant impact"

    def generate_report(self, comparison_df: Optional[pd.DataFrame] = None) -> str:
        """
        Generate comprehensive analysis report.

        Args:
            comparison_df: DataFrame with comparison results

        Returns:
            Formatted report string
        """
        if comparison_df is None:
            comparison_df = self.create_comparison_dataframe()

        # Perform analyses
        dominance_analysis = self.analyze_kbrs_dominance(comparison_df)
        reward_analysis = self.analyze_reward_quality(comparison_df)

        # Generate report
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("📊 MAB EXPERIMENT ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")

        # Summary table
        report_lines.append("COMPARISON TABLE:")
        report_lines.append(comparison_df.round(4).to_string(index=False))
        report_lines.append("")

        # KBRS vs Baseline analysis
        report_lines.append("KBRS vs BASELINE ANALYSIS:")
        report_lines.append(f"  • KBRS Average Dominance: {dominance_analysis['kbrs_dominance']:.2f}%")
        report_lines.append(f"  • Baseline Average Performance: {dominance_analysis['baseline_performance']:.2f}%")
        report_lines.append(f"  • Variance in KBRS Selection: {dominance_analysis['variance']:.2f}")
        report_lines.append(f"  • Consistent Winner: {dominance_analysis['consistent_winner']}")
        report_lines.append("")

        # Reward quality analysis
        report_lines.append("REWARD QUALITY ANALYSIS:")
        report_lines.append(f"  • Mean Average Reward: {reward_analysis['mean_reward']:.4f}")
        report_lines.append(f"  • Reward Variance: {reward_analysis['reward_variance']:.4f}")
        report_lines.append(f"  • Mean KBRS R_G: {reward_analysis['mean_kbrs_rg']:.4f}")
        report_lines.append(f"  • R_G Variance: {reward_analysis['rg_variance']:.4f}")
        report_lines.append(f"  • R_G Range: {reward_analysis['rg_range'][0]:.4f} - {reward_analysis['rg_range'][1]:.4f}")
        report_lines.append(f"  • Best Config: {reward_analysis['best_performing_config']}")
        report_lines.append(f"  • Worst Config: {reward_analysis['worst_performing_config']}")
        report_lines.append("")

        # Parameter impact analysis (if available)
        if len(self.results) > 1:
            param_analysis = self.analyze_parameter_impact('top_k_similar', comparison_df)
            report_lines.append("PARAMETER IMPACT ANALYSIS:")
            report_lines.append(f"  • Parameter: {param_analysis['parameter']}")
            report_lines.append(f"  • Range: {param_analysis['parameter_range'][0]} - {param_analysis['parameter_range'][1]}")
            report_lines.append(f"  • Correlations:")
            for metric, corr in param_analysis['correlations'].items():
                report_lines.append(f"    - {metric}: {corr:.4f}")
            report_lines.append(f"  • Impact Assessment: {param_analysis['impact_assessment']}")
            report_lines.append("")

        report_lines.append("=" * 80)

        return "\n".join(report_lines)
