"""
NCF Trainer - Training and evaluation for Neural Collaborative Filtering model.
"""

from __future__ import annotations

import os
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Dict, Any, Optional, Tuple
import logging
from tqdm import tqdm

from ..models.ncf import NCF
from ..core.utils import AverageMeter, format_time
from .callbacks import EarlyStopping, ModelCheckpoint

logger = logging.getLogger(__name__)


class NCFTrainer:
    """
    Trainer for NCF model with support for:
    - Training on train set
    - Validation for hyperparameter tuning
    - Testing on test set
    - Early stopping and checkpointing
    """

    def __init__(
        self,
        model: NCF,
        device: Optional[str] = None,
        learning_rate: float = 0.001,
        weight_decay: float = 1e-5
    ):
        """
        Initialize trainer.

        Args:
            model: NCF model
            device: Device to use ('cuda' or 'cpu')
            learning_rate: Learning rate
            weight_decay: Weight decay (L2 regularization)
        """
        self.model = model
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)

        # Optimizer
        self.optimizer = torch.optim.Adam(
            model.parameters(),
            lr=learning_rate,
            weight_decay=weight_decay
        )

        # Loss function
        self.criterion = nn.MSELoss()

        # Training history
        self.history = {
            'train_loss': [],
            'val_loss': [],
            'train_rmse': [],
            'val_rmse': []
        }

    def train_epoch(self, train_loader: DataLoader) -> Tuple[float, float]:
        """
        Train for one epoch.

        Args:
            train_loader: Training data loader

        Returns:
            Tuple of (loss, rmse)
        """
        self.model.train()
        total_loss = 0.0
        total_rmse = 0.0
        num_samples = 0

        for batch in tqdm(train_loader, desc="Training", leave=False):
            # Move batch to device
            user_ids = batch['user_ids'].to(self.device)
            movie_ids = batch['movie_ids'].to(self.device)
            ratings = batch['ratings'].to(self.device).float()

            # Extract movie features if available
            movie_features = None
            if 'movie_features' in batch:
                movie_features = batch['movie_features'].to(self.device)

            # Forward pass
            self.optimizer.zero_grad()
            predictions = self.model(user_ids, movie_ids, movie_features).squeeze()
            loss = self.criterion(predictions, ratings)

            # Backward pass
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
            self.optimizer.step()

            # Update metrics
            total_loss += loss.item() * user_ids.size(0)
            rmse = torch.sqrt(loss).item()
            total_rmse += rmse * user_ids.size(0)
            num_samples += user_ids.size(0)

        avg_loss = total_loss / num_samples
        avg_rmse = total_rmse / num_samples

        return avg_loss, avg_rmse

    def validate(self, val_loader: DataLoader) -> Tuple[float, float]:
        """
        Validate model.

        Args:
            val_loader: Validation data loader

        Returns:
            Tuple of (loss, rmse)
        """
        self.model.eval()
        total_loss = 0.0
        total_rmse = 0.0
        num_samples = 0

        with torch.no_grad():
            for batch in val_loader:
                user_ids = batch['user_ids'].to(self.device)
                movie_ids = batch['movie_ids'].to(self.device)
                ratings = batch['ratings'].to(self.device).float()

                # Extract movie features if available
                movie_features = None
                if 'movie_features' in batch:
                    movie_features = batch['movie_features'].to(self.device)

                predictions = self.model(user_ids, movie_ids, movie_features).squeeze()
                loss = self.criterion(predictions, ratings)

                total_loss += loss.item() * user_ids.size(0)
                total_rmse += torch.sqrt(loss).item() * user_ids.size(0)
                num_samples += user_ids.size(0)

        avg_loss = total_loss / num_samples
        avg_rmse = total_rmse / num_samples

        return avg_loss, avg_rmse

    def test(self, test_loader: DataLoader) -> Dict[str, float]:
        """
        Test model.

        Args:
            test_loader: Test data loader

        Returns:
            Dictionary with test metrics
        """
        self.model.eval()
        predictions_list = []
        actual_list = []

        with torch.no_grad():
            for batch in test_loader:
                user_ids = batch['user_ids'].to(self.device)
                movie_ids = batch['movie_ids'].to(self.device)
                ratings = batch['ratings'].to(self.device).float()

                # Extract movie features if available
                movie_features = None
                if 'movie_features' in batch:
                    movie_features = batch['movie_features'].to(self.device)

                predictions = self.model(user_ids, movie_ids, movie_features).squeeze()
                predictions_list.extend(predictions.cpu().numpy())
                actual_list.extend(ratings.cpu().numpy())

        # Calculate metrics
        predictions_array = np.array(predictions_list)
        actual_array = np.array(actual_list)

        rmse = np.sqrt(mean_squared_error(actual_array, predictions_array))
        mae = mean_absolute_error(actual_array, predictions_array)

        return {
            'rmse': rmse,
            'mae': mae,
            'num_predictions': len(predictions_list)
        }

    def train(
        self,
        train_loader: DataLoader,
        val_loader: Optional[DataLoader] = None,
        num_epochs: int = 50,
        early_stopping_patience: int = 5,
        save_best_model: bool = True,
        output_dir: str = 'models'
    ) -> Dict[str, Any]:
        """
        Train the model.

        Args:
            train_loader: Training data loader
            val_loader: Validation data loader
            num_epochs: Number of epochs
            early_stopping_patience: Early stopping patience
            save_best_model: Whether to save best model
            output_dir: Directory to save models

        Returns:
            Training history
        """
        logger.info(f"Starting training on {self.device}")
        logger.info(f"Model: {self.model.get_model_info()}")

        # Callbacks
        early_stopping = EarlyStopping(
            patience=early_stopping_patience,
            monitor='val_loss',
            mode='min'
        )

        model_checkpoint = ModelCheckpoint(
            filepath=os.path.join(output_dir, 'best_model.pt'),
            monitor='val_loss',
            mode='min'
        )

        best_val_rmse = float('inf')
        best_epoch = 0

        for epoch in range(num_epochs):
            # Train
            train_loss, train_rmse = self.train_epoch(train_loader)

            # Validate
            val_loss = None
            val_rmse = None
            if val_loader:
                val_loss, val_rmse = self.validate(val_loader)
                self.history['val_loss'].append(val_loss)
                self.history['val_rmse'].append(val_rmse)

            # Update history
            self.history['train_loss'].append(train_loss)
            self.history['train_rmse'].append(train_rmse)

            # Log progress
            if val_loader:
                print(
                    f"Epoch {epoch+1}/{num_epochs}: "
                    f"Train Loss: {train_loss:.4f}, Train RMSE: {train_rmse:.4f}, "
                    f"Val Loss: {val_loss:.4f}, Val RMSE: {val_rmse:.4f}"
                )

                # Early stopping check
                if early_stopping(val_loss, self.model):
                    print(f"Early stopping at epoch {epoch+1}")
                    break

                # Model checkpoint
                if save_best_model and val_rmse < best_val_rmse:
                    best_val_rmse = val_rmse
                    best_epoch = epoch
                    model_checkpoint.save_checkpoint(
                        self.model,
                        epoch,
                        val_rmse=val_rmse
                    )
            else:
                print(
                    f"Epoch {epoch+1}/{num_epochs}: "
                    f"Train Loss: {train_loss:.4f}, Train RMSE: {train_rmse:.4f}"
                )

        if save_best_model and val_loader:
            logger.info(f"Best model saved at epoch {best_epoch+1} (val_rmse: {best_val_rmse:.4f})")

        return self.history

    def predict(self, user_ids: list[int], movie_ids: list[int]) -> np.ndarray:
        """
        Predict ratings for user-movie pairs.

        Args:
            user_ids: List of user IDs
            movie_ids: List of movie IDs

        Returns:
            Predicted ratings
        """
        self.model.eval()
        with torch.no_grad():
            user_tensor = torch.tensor(user_ids, dtype=torch.long, device=self.device)
            movie_tensor = torch.tensor(movie_ids, dtype=torch.long, device=self.device)
            predictions = self.model(user_tensor, movie_tensor)
            return predictions.cpu().numpy()

    def save_model(self, filepath: str) -> None:
        """Save trained model."""
        self.model.save_model(filepath)
        logger.info(f"Model saved to {filepath}")

    def load_model(self, filepath: str) -> None:
        """Load trained model."""
        self.model.load_model(filepath, map_location=self.device)
        logger.info(f"Model loaded from {filepath}")
