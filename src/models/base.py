"""
Base model class for all CineWisdom models.
"""

from __future__ import annotations

import torch
import torch.nn as nn
from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseModel(nn.Module, ABC):
    """Abstract base class for all models."""

    def __init__(self):
        super().__init__()
        self.model_config = {}

    @abstractmethod
    def forward(self, *args, **kwargs):
        """Forward pass definition."""
        pass

    @abstractmethod
    def get_model_info(self) -> Dict[str, Any]:
        """Get model information."""
        pass

    def save_model(self, filepath: str) -> None:
        """Save model to file."""
        torch.save({
            'model_state_dict': self.state_dict(),
            'model_config': self.model_config
        }, filepath)

    def load_model(self, filepath: str, map_location: str = 'cpu') -> None:
        """Load model from file."""
        checkpoint = torch.load(filepath, map_location=map_location)
        self.load_state_dict(checkpoint['model_state_dict'])
        self.model_config = checkpoint.get('model_config', {})
