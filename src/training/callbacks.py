"""
Training callbacks for CineWisdom models.
"""

from __future__ import annotations

import os
import torch
from typing import Callable, Any


class EarlyStopping:
    """Early stopping callback."""

    def __init__(
        self,
        patience: int = 7,
        min_delta: float = 0,
        restore_best_weights: bool = True,
        monitor: str = 'val_loss',
        mode: str = 'min'
    ):
        """
        Initialize early stopping.

        Args:
            patience: Number of epochs to wait before stopping
            min_delta: Minimum change to qualify as improvement
            restore_best_weights: Whether to restore best weights
            monitor: Metric to monitor
            mode: 'min' or 'max'
        """
        self.patience = patience
        self.min_delta = min_delta
        self.restore_best_weights = restore_best_weights
        self.monitor = monitor
        self.mode = mode

        self.best_loss = float('inf') if mode == 'min' else float('-inf')
        self.counter = 0
        self.best_weights = None

    def __call__(self, current_metric: float, model: torch.nn.Module) -> bool:
        """
        Check if training should stop.

        Args:
            current_metric: Current metric value
            model: Model to potentially save weights

        Returns:
            True if training should stop
        """
        improved = (
            (self.mode == 'min' and current_metric < self.best_loss - self.min_delta) or
            (self.mode == 'max' and current_metric > self.best_loss + self.min_delta)
        )

        if improved:
            self.best_loss = current_metric
            self.counter = 0
            if self.restore_best_weights:
                self.best_weights = model.state_dict().copy()
        else:
            self.counter += 1

        if self.counter >= self.patience:
            if self.restore_best_weights and self.best_weights is not None:
                model.load_state_dict(self.best_weights)
            return True

        return False

    def reset(self):
        """Reset early stopping state."""
        self.best_loss = float('inf') if self.mode == 'min' else float('-inf')
        self.counter = 0
        self.best_weights = None


class ModelCheckpoint:
    """Model checkpoint callback."""

    def __init__(
        self,
        filepath: str,
        monitor: str = 'val_loss',
        mode: str = 'min',
        save_best_only: bool = True,
        save_weights_only: bool = False
    ):
        """
        Initialize model checkpoint.

        Args:
            filepath: Path to save checkpoint
            monitor: Metric to monitor
            mode: 'min' or 'max'
            save_best_only: Whether to save only best model
            save_weights_only: Whether to save only weights
        """
        self.filepath = filepath
        self.monitor = monitor
        self.mode = mode
        self.save_best_only = save_best_only
        self.save_weights_only = save_weights_only

        self.best_loss = float('inf') if mode == 'min' else float('-inf')

    def __call__(self, current_metric: float, model: torch.nn.Module, epoch: int) -> bool:
        """
        Save checkpoint if improved.

        Args:
            current_metric: Current metric value
            model: Model to save
            epoch: Current epoch

        Returns:
            True if checkpoint was saved
        """
        improved = (
            (self.mode == 'min' and current_metric < self.best_loss) or
            (self.mode == 'max' and current_metric > self.best_loss)
        )

        if improved:
            self.best_loss = current_metric
            return True

        return False

    def save_checkpoint(self, model: torch.nn.Module, epoch: int, **kwargs):
        """Save checkpoint."""
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

        checkpoint = {
            'epoch': epoch,
            'model_state_dict': model.state_dict(),
            **kwargs
        }

        torch.save(checkpoint, self.filepath)
