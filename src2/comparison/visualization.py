"""
Visualization Module - Generate comparative visualizations

This module provides functions to create publication-quality visualizations
comparing Traditional and MAB pipelines.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# Try to import plotly for interactive plots
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# NOTE: metrics_converter è stato eliminato
# Le metriche Traditional e MAB sono indipendenti


class ComparativeVisualizer:
    """Generate comparative visualizations for Traditional vs MAB pipelines."""

    def __init__(
        self,
        output_dir: str = "results/comparison/plots",
        style: str = "seaborn-v0_8",
        palette: Optional[List[str]] = None
    ):
        """
        Initialize the visualizer.

        Args:
            output_dir: Directory to save plots
            style: Matplotlib style to use
            palette: Color palette for plots
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Set style
        plt.style.use(style)
        sns.set_palette(palette or ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])

        # Colors for pipelines
        self.colors = {
            'traditional': '#2E86AB',
            'mab': '#A23B72',
            'traditional_light': '#A8DADC',
            'mab_light': '#F1A7B8'
        }

    def plot_metrics_comparison(
        self,
        traditional_metrics: UnifiedMetrics,
        mab_metrics: UnifiedMetrics,
        save_plot: bool = True,
        format: str = 'png',
        dpi: int = 150
    ) -> plt.Figure:
        """
        Create a radar chart comparing all metrics.

        Args:
            traditional_metrics: Unified metrics from Traditional pipeline
            mab_metrics: Unified metrics from MAB pipeline
            save_plot: Whether to save the plot
            format: Image format (png, pdf, svg)
            dpi: Image DPI

        Returns:
            Matplotlib figure object
        """
        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))

        # Prepare data
        categories = [
            'Rating Accuracy',
            'Ranking Precision',
            'Ranking NDCG',
            'Ranking MAP',
            'Exploration',
            'Adaptation',
            'Stability',
            'Overall'
        ]

        trad_values = [
            traditional_metrics.rating_accuracy,
            traditional_metrics.ranking_precision,
            traditional_metrics.ranking_ndcg,
            traditional_metrics.ranking_map,
            traditional_metrics.exploration_score,
            traditional_metrics.adaptation_score,
            traditional_metrics.stability_score,
            traditional_metrics.overall_score
        ]

        mab_values = [
            mab_metrics.rating_accuracy,
            mab_metrics.ranking_precision,
            mab_metrics.ranking_ndcg,
            mab_metrics.ranking_map,
            mab_metrics.exploration_score,
            mab_metrics.adaptation_score,
            mab_metrics.stability_score,
            mab_metrics.overall_score
        ]

        # Number of variables
        N = len(categories)

        # Compute angle for each axis
        angles = [n / float(N) * 2 * np.pi for n in range(N)]
        angles += angles[:1]  # Complete the circle

        # Add values to complete the circle
        trad_values += trad_values[:1]
        mab_values += mab_values[:1]

        # Plot
        ax.plot(angles, trad_values, 'o-', linewidth=2, label='Traditional', color=self.colors['traditional'])
        ax.fill(angles, trad_values, alpha=0.25, color=self.colors['traditional'])

        ax.plot(angles, mab_values, 'o-', linewidth=2, label='MAB', color=self.colors['mab'])
        ax.fill(angles, mab_values, alpha=0.25, color=self.colors['mab'])

        # Add category labels
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)

        # Set y-axis limits
        ax.set_ylim(0, 1)
        ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
        ax.set_yticklabels(['0.2', '0.4', '0.6', '0.8', '1.0'])

        # Add legend and title
        plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        plt.title('Traditional vs MAB - Metrics Comparison', size=16, pad=20)

        if save_plot:
            plt.savefig(self.output_dir / f'metrics_radar.{format}', dpi=dpi, bbox_inches='tight')

        plt.tight_layout()
        return fig

    def plot_bar_comparison(
        self,
        traditional_metrics: UnifiedMetrics,
        mab_metrics: UnifiedMetrics,
        save_plot: bool = True,
        format: str = 'png',
        dpi: int = 150
    ) -> plt.Figure:
        """
        Create a bar chart comparing metrics side-by-side.

        Args:
            traditional_metrics: Unified metrics from Traditional pipeline
            mab_metrics: Unified metrics from MAB pipeline
            save_plot: Whether to save the plot
            format: Image format
            dpi: Image DPI

        Returns:
            Matplotlib figure object
        """
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()

        # Define metric groups
        metric_groups = [
            {
                'title': 'Rating Prediction',
                'metrics': [('Rating Accuracy', 'rating_accuracy')],
                'ax_idx': 0
            },
            {
                'title': 'Ranking Quality',
                'metrics': [
                    ('Precision@10', 'ranking_precision'),
                    ('NDCG@10', 'ranking_ndcg'),
                    ('MAP', 'ranking_map')
                ],
                'ax_idx': 1
            },
            {
                'title': 'MAB Characteristics',
                'metrics': [
                    ('Exploration', 'exploration_score'),
                    ('Adaptation', 'adaptation_score'),
                    ('Stability', 'stability_score')
                ],
                'ax_idx': 2
            },
            {
                'title': 'Overall Performance',
                'metrics': [('Overall Score', 'overall_score')],
                'ax_idx': 3
            }
        ]

        for group in metric_groups:
            ax = axes[group['ax_idx']]
            metric_data = []
            metric_labels = []

            for label, attr in group['metrics']:
                trad_val = getattr(traditional_metrics, attr)
                mab_val = getattr(mab_metrics, attr)
                metric_data.append([trad_val, mab_val])
                metric_labels.append(label)

            x = np.arange(len(metric_labels))
            width = 0.35

            trad_values = [d[0] for d in metric_data]
            mab_values = [d[1] for d in metric_data]

            bars1 = ax.bar(x - width/2, trad_values, width, label='Traditional', color=self.colors['traditional'])
            bars2 = ax.bar(x + width/2, mab_values, width, label='MAB', color=self.colors['mab'])

            # Add value labels on bars
            for bar in bars1:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                       f'{height:.3f}', ha='center', va='bottom', fontsize=9)

            for bar in bars2:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                       f'{height:.3f}', ha='center', va='bottom', fontsize=9)

            ax.set_xlabel('Metrics')
            ax.set_ylabel('Score')
            ax.set_title(group['title'])
            ax.set_xticks(x)
            ax.set_xticklabels(metric_labels, rotation=15, ha='right')
            ax.legend()
            ax.set_ylim(0, max(max(trad_values), max(mab_values)) * 1.2)

        plt.tight_layout()

        if save_plot:
            plt.savefig(self.output_dir / f'metrics_bar.{format}', dpi=dpi, bbox_inches='tight')

        return fig

    def plot_heatmap_comparison(
        self,
        traditional_metrics: UnifiedMetrics,
        mab_metrics: UnifiedMetrics,
        save_plot: bool = True,
        format: str = 'png',
        dpi: int = 150
    ) -> plt.Figure:
        """
        Create a heatmap showing the difference between pipelines.

        Args:
            traditional_metrics: Unified metrics from Traditional pipeline
            mab_metrics: Unified metrics from MAB pipeline
            save_plot: Whether to save the plot
            format: Image format
            dpi: Image DPI

        Returns:
            Matplotlib figure object
        """
        # Prepare data
        metrics = {
            'Rating Accuracy': (traditional_metrics.rating_accuracy, mab_metrics.rating_accuracy),
            'Ranking Precision': (traditional_metrics.ranking_precision, mab_metrics.ranking_precision),
            'Ranking NDCG': (traditional_metrics.ranking_ndcg, mab_metrics.ranking_ndcg),
            'Ranking MAP': (traditional_metrics.ranking_map, mab_metrics.ranking_map),
            'Exploration': (traditional_metrics.exploration_score, mab_metrics.exploration_score),
            'Adaptation': (traditional_metrics.adaptation_score, mab_metrics.adaptation_score),
            'Stability': (traditional_metrics.stability_score, mab_metrics.stability_score),
            'Overall': (traditional_metrics.overall_score, mab_metrics.overall_score)
        }

        # Create matrix
        data = np.array([[t, m] for t, m in metrics.values()])

        fig, ax = plt.subplots(figsize=(8, 10))

        # Create heatmap
        im = ax.imshow(data, cmap='RdYlBu', aspect='auto', vmin=0, vmax=1)

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Metric Score')

        # Set ticks and labels
        ax.set_xticks([0, 1])
        ax.set_xticklabels(['Traditional', 'MAB'])
        ax.set_yticks(range(len(metrics)))
        ax.set_yticklabels(list(metrics.keys()))

        # Add text annotations
        for i, (metric, (trad_val, mab_val)) in enumerate(metrics.items()):
            ax.text(0, i, f'{trad_val:.3f}', ha='center', va='center',
                   color='white' if trad_val < 0.5 else 'black', fontweight='bold')
            ax.text(1, i, f'{mab_val:.3f}', ha='center', va='center',
                   color='white' if mab_val < 0.5 else 'black', fontweight='bold')

        ax.set_title('Traditional vs MAB - Metrics Heatmap')

        plt.tight_layout()

        if save_plot:
            plt.savefig(self.output_dir / f'metrics_heatmap.{format}', dpi=dpi, bbox_inches='tight')

        return fig

    def plot_learning_curves(
        self,
        traditional_history: Optional[Dict] = None,
        mab_history: Optional[Dict] = None,
        save_plot: bool = True,
        format: str = 'png',
        dpi: int = 150
    ) -> plt.Figure:
        """
        Plot learning curves for both pipelines (if history is available).

        Args:
            traditional_history: History from Traditional pipeline (if available)
            mab_history: History from MAB pipeline
            save_plot: Whether to save the plot
            format: Image format
            dpi: Image DPI

        Returns:
            Matplotlib figure object
        """
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()

        # Plot MAB curves (always available)
        if mab_history is not None:
            iterations = range(len(mab_history))

            # Reward over time
            axes[0].plot(iterations, mab_history.get('reward', []), color=self.colors['mab'], alpha=0.7)
            axes[0].set_title('MAB - Reward Over Time')
            axes[0].set_xlabel('Iteration')
            axes[0].set_ylabel('Reward')
            axes[0].grid(True, alpha=0.3)

            # R_A and R_G over time
            axes[1].plot(iterations, mab_history.get('reward_ra', []), label='R_A (Exploration)', color=self.colors['mab'])
            axes[1].plot(iterations, mab_history.get('reward_rg', []), label='R_G (Accuracy)', color=self.colors['mab_light'])
            axes[1].set_title('MAB - Reward Components')
            axes[1].set_xlabel('Iteration')
            axes[1].set_ylabel('Reward')
            axes[1].legend()
            axes[1].grid(True, alpha=0.3)

            # Temperature over time
            axes[2].plot(iterations, mab_history.get('temperature', []), color='#F18F01')
            axes[2].set_title('MAB - Temperature Schedule')
            axes[2].set_xlabel('Iteration')
            axes[2].set_ylabel('Temperature')
            axes[2].grid(True, alpha=0.3)

            # KBRS Selection Rate
            if 'kbrs_selection_rate' in mab_history:
                axes[3].plot(iterations, mab_history['kbrs_selection_rate'], color='#C73E1D')
                axes[3].set_title('MAB - KBRS Selection Rate')
                axes[3].set_xlabel('Iteration')
                axes[3].set_ylabel('KBRS Selection %')
                axes[3].grid(True, alpha=0.3)

        # Traditional curves (if available)
        if traditional_history is not None:
            # For Traditional, we might have validation curves over epochs
            pass

        plt.tight_layout()

        if save_plot:
            plt.savefig(self.output_dir / f'learning_curves.{format}', dpi=dpi, bbox_inches='tight')

        return fig

    def create_comprehensive_report(
        self,
        traditional_metrics: UnifiedMetrics,
        mab_metrics: UnifiedMetrics,
        comparison_summary: Dict,
        traditional_history: Optional[Dict] = None,
        mab_history: Optional[Dict] = None,
        format: str = 'png',
        dpi: int = 150
    ) -> List[plt.Figure]:
        """
        Create a comprehensive set of comparison plots.

        Args:
            traditional_metrics: Unified metrics from Traditional pipeline
            mab_metrics: Unified metrics from MAB pipeline
            comparison_summary: Summary from MetricsConverter
            traditional_history: Optional history from Traditional pipeline
            mab_history: Optional history from MAB pipeline
            format: Image format
            dpi: Image DPI

        Returns:
            List of matplotlib figure objects
        """
        figures = []

        # 1. Radar chart
        fig1 = self.plot_metrics_comparison(
            traditional_metrics, mab_metrics, save_plot=False, format=format, dpi=dpi
        )
        figures.append(fig1)

        # 2. Bar comparison
        fig2 = self.plot_bar_comparison(
            traditional_metrics, mab_metrics, save_plot=False, format=format, dpi=dpi
        )
        figures.append(fig2)

        # 3. Heatmap
        fig3 = self.plot_heatmap_comparison(
            traditional_metrics, mab_metrics, save_plot=False, format=format, dpi=dpi
        )
        figures.append(fig3)

        # 4. Learning curves
        fig4 = self.plot_learning_curves(
            traditional_history, mab_history, save_plot=False, format=format, dpi=dpi
        )
        figures.append(fig4)

        # Save all plots
        for i, fig in enumerate(figures, 1):
            plt.figure(fig.number)
            plt.savefig(
                self.output_dir / f'plot_{i}.{format}',
                dpi=dpi,
                bbox_inches='tight'
            )

        print(f"\n✓ All plots saved to {self.output_dir}")

        return figures

    def save_summary_table(
        self,
        traditional_metrics: UnifiedMetrics,
        mab_metrics: UnifiedMetrics,
        comparison_summary: Dict,
        output_file: Optional[str] = None
    ) -> None:
        """
        Save a summary table as CSV.

        Args:
            traditional_metrics: Unified metrics from Traditional pipeline
            mab_metrics: Unified metrics from MAB pipeline
            comparison_summary: Summary from MetricsConverter
            output_file: Output file path (optional)
        """
        if output_file is None:
            output_file = self.output_dir / 'comparison_summary.csv'

        # Create summary dataframe
        data = []

        for metric_name, data_dict in comparison_summary['winner'].items():
            data.append({
                'Metric': metric_name,
                'Traditional': data_dict['traditional'],
                'MAB': data_dict['mab'],
                'Winner': data_dict['winner'],
                'Difference': data_dict['difference'],
                'Percent_Difference': data_dict['percent_diff']
            })

        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)

        print(f"✓ Summary table saved to {output_file}")


# Convenience functions
def create_quick_comparison_plot(
    traditional_metrics: UnifiedMetrics,
    mab_metrics: UnifiedMetrics,
    output_dir: str = "results/comparison/plots",
    format: str = 'png'
) -> plt.Figure:
    """
    Create a quick comparison plot.

    Args:
        traditional_metrics: Unified metrics from Traditional pipeline
        mab_metrics: Unified metrics from MAB pipeline
        output_dir: Output directory
        format: Image format

    Returns:
        Matplotlib figure object
    """
    visualizer = ComparativeVisualizer(output_dir=output_dir)
    return visualizer.plot_metrics_comparison(
        traditional_metrics, mab_metrics, save_plot=True, format=format
    )


__all__ = [
    'ComparativeVisualizer',
    'create_quick_comparison_plot'
]
