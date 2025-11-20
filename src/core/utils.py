"""
Utility functions per CineWisdom NCF+MAB System.
"""

from __future__ import annotations

import random
import numpy as np
import torch
from typing import Tuple, Optional
import os
import json
from datetime import datetime


def set_seed(seed: int) -> None:
    """Set random seed for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def get_device(device: Optional[str] = None) -> torch.device:
    """
    Get torch device.

    Args:
        device: Specified device string, or None for auto-detection

    Returns:
        Torch device
    """
    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"
    return torch.device(device)


def ensure_dir(path: str) -> None:
    """Ensure directory exists."""
    os.makedirs(path, exist_ok=True)


def save_json(data: dict, path: str) -> None:
    """Save data to JSON file."""
    ensure_dir(os.path.dirname(path))
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, default=str)


def load_json(path: str) -> dict:
    """Load data from JSON file."""
    with open(path, 'r') as f:
        return json.load(f)


def get_timestamp() -> str:
    """Get current timestamp as string."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_model_size(model: torch.nn.Module) -> dict:
    """
    Get model size information.

    Args:
        model: PyTorch model

    Returns:
        Dictionary with model size info
    """
    param_size = sum(p.numel() for p in model.parameters())
    buffer_size = sum(b.numel() for b in model.buffers())
    size_all_mb = (param_size + buffer_size) / 1024 / 1024

    return {
        'parameters': param_size,
        'buffers': buffer_size,
        'total': param_size + buffer_size,
        'size_mb': size_all_mb
    }


def count_parameters(model: torch.nn.Module) -> int:
    """Count trainable parameters in model."""
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


class EarlyStopping:
    """Early stopping utility."""

    def __init__(
        self,
        patience: int = 7,
        min_delta: float = 0,
        restore_best_weights: bool = True
    ):
        self.patience = patience
        self.min_delta = min_delta
        self.restore_best_weights = restore_best_weights
        self.best_loss = None
        self.counter = 0
        self.best_weights = None

    def __call__(
        self,
        val_loss: float,
        model: torch.nn.Module
    ) -> bool:
        """
        Check if training should stop.

        Args:
            val_loss: Validation loss
            model: Model to possibly save weights

        Returns:
            True if training should stop, False otherwise
        """
        if self.best_loss is None:
            self.best_loss = val_loss
            self.save_checkpoint(model)
            return False

        if val_loss < self.best_loss - self.min_delta:
            self.best_loss = val_loss
            self.counter = 0
            self.save_checkpoint(model)
        else:
            self.counter += 1

        if self.counter >= self.patience:
            if self.restore_best_weights and self.best_weights is not None:
                model.load_state_dict(self.best_weights)
            return True

        return False

    def save_checkpoint(self, model: torch.nn.Module) -> None:
        """Save model checkpoint."""
        if self.restore_best_weights:
            self.best_weights = model.state_dict().copy()


class AverageMeter:
    """Computes and stores the average and current value."""

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val: float, n: int = 1) -> None:
        """Update meter with new value."""
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count if self.count > 0 else 0


def get_lr(optimizer: torch.optim.Optimizer) -> float:
    """Get current learning rate from optimizer."""
    for param_group in optimizer.param_groups:
        return param_group['lr']


def seed_worker(worker_id: int) -> None:
    """Set seed for DataLoader workers."""
    worker_seed = torch.initial_seed() % 2**32
    np.random.seed(worker_seed)
    random.seed(worker_seed)


def format_time(seconds: float) -> str:
    """Format seconds into human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
