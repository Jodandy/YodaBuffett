#!/usr/bin/env python3
"""
Dimension ML Trainer

Train gradient boosting models on 14 dimension scores to predict forward returns.
Extract feature importance to understand which dimensions actually predict returns.

Usage:
    python dimension_ml_trainer.py                    # Full training + analysis
    python dimension_ml_trainer.py --horizon 6M       # Specific forward period
    python dimension_ml_trainer.py --dry-run          # Show data stats only
    python dimension_ml_trainer.py --feature-importance  # Show which dimensions matter
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Tuple, Optional
import argparse
import logging
import json
import warnings
warnings.filterwarnings('ignore')

# ML imports
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix, roc_auc_score
)

try:
    import lightgbm as lgb
    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False

try:
    import xgboost as xgb
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False

from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# The 14 dimensions
DIMENSIONS = [
    'profitability', 'returns', 'growth', 'financial_health',
    'earnings_quality', 'capital_allocation', 'working_capital', 'beneish_mscore',
    'value', 'risk', 'momentum', 'quality', 'valuation_percentile', 'sentiment'
]

# Forward return horizons (trading days)
HORIZONS = {
    '1M': 21,
    '3M': 63,
    '6M': 126,
    '12M': 252
}


class DimensionMLTrainer:
    """Train ML models on dimension scores to predict forward returns."""

    def __init__(self, horizon: str = '6M'):
        self.horizon = horizon
        self.horizon_days = HORIZONS.get(horizon, 126)
        self.conn = None
        self.model = None
        self.scaler = StandardScaler()

    async def connect(self):
        """Connect to database."""
        self.conn = await asyncpg.connect(DATABASE_URL)
        logger.info("Connected to database")

    async def close(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()

    async def get_dimension_scores(self) -> pd.DataFrame:
        """
        Get pivoted dimension scores with forward returns.
        Returns DataFrame with columns: company_id, score_date, dim1, dim2, ..., forward_return
        """
        logger.info(f"Fetching dimension scores with {self.horizon} forward returns...")

        # Build pivot query for all dimensions
        pivot_cases = ",\n            ".join([
            f"MAX(CASE WHEN dimension_code = '{dim}' THEN score END) as {dim}"
            for dim in DIMENSIONS
        ])

        query = f"""
        WITH pivoted_scores AS (
            SELECT
                dds.company_id,
                dds.score_date,
                {pivot_cases}
            FROM daily_dimension_scores dds
            WHERE dds.score_date <= '2024-12-31'  -- Need forward data
            GROUP BY dds.company_id, dds.score_date
            HAVING COUNT(DISTINCT dimension_code) >= 8  -- At least 8 dimensions
        ),
        price_at_score AS (
            SELECT DISTINCT ON (company_id, score_date)
                ps.company_id,
                ps.score_date,
                dpd.close_price as entry_price,
                dpd.date as entry_date
            FROM pivoted_scores ps
            JOIN daily_price_data dpd ON ps.company_id = dpd.company_id
            WHERE dpd.date >= ps.score_date
            AND dpd.date <= ps.score_date + INTERVAL '7 days'
            AND dpd.company_id IS NOT NULL
            ORDER BY ps.company_id, ps.score_date, dpd.date
        ),
        future_prices AS (
            SELECT DISTINCT ON (pas.company_id, pas.score_date)
                pas.company_id,
                pas.score_date,
                pas.entry_price,
                dpd.close_price as exit_price,
                dpd.date as exit_date
            FROM price_at_score pas
            JOIN daily_price_data dpd ON pas.company_id = dpd.company_id
            WHERE dpd.date >= pas.entry_date + INTERVAL '{self.horizon_days} days'
            AND dpd.date <= pas.entry_date + INTERVAL '{self.horizon_days + 14} days'
            AND dpd.company_id IS NOT NULL
            ORDER BY pas.company_id, pas.score_date, dpd.date
        )
        SELECT
            ps.*,
            cm.company_name,
            fp.entry_price,
            fp.exit_price,
            CASE
                WHEN fp.entry_price > 0 THEN
                    ((fp.exit_price - fp.entry_price) / fp.entry_price) * 100
                ELSE NULL
            END as forward_return_pct
        FROM pivoted_scores ps
        JOIN company_master cm ON ps.company_id = cm.id
        JOIN future_prices fp ON ps.company_id = fp.company_id AND ps.score_date = fp.score_date
        WHERE fp.entry_price > 0 AND fp.exit_price > 0
        ORDER BY ps.score_date, ps.company_id
        """

        rows = await self.conn.fetch(query)

        if not rows:
            logger.warning("No data returned from query")
            return pd.DataFrame()

        df = pd.DataFrame([dict(r) for r in rows])

        # Convert Decimal to float for all numeric columns
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = df[col].astype(float)
                except (ValueError, TypeError):
                    pass

        # Explicitly convert dimension and return columns
        for dim in DIMENSIONS:
            if dim in df.columns:
                df[dim] = pd.to_numeric(df[dim], errors='coerce')

        df['forward_return_pct'] = pd.to_numeric(df['forward_return_pct'], errors='coerce')

        logger.info(f"Loaded {len(df)} records with forward returns")

        return df

    def create_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create classification labels from forward returns.

        Labels:
        - 2 = "Strong Buy": > 20% return
        - 1 = "Buy": > 10% return
        - 0 = "Hold": -10% to +10%
        - -1 = "Sell": < -10%

        For binary classification:
        - 1 = "Good Pick": > 15% return
        - 0 = "Other"
        """
        df = df.copy()

        # Multi-class labels
        df['label_multiclass'] = pd.cut(
            df['forward_return_pct'],
            bins=[-np.inf, -10, 10, 20, np.inf],
            labels=[-1, 0, 1, 2]
        ).astype(int)

        # Binary labels (simpler, more robust)
        df['label_binary'] = (df['forward_return_pct'] > 15).astype(int)

        # Also create quintile labels
        df['label_quintile'] = pd.qcut(
            df['forward_return_pct'],
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        )

        logger.info(f"Label distribution (binary): {df['label_binary'].value_counts().to_dict()}")
        logger.info(f"Label distribution (multiclass): {df['label_multiclass'].value_counts().to_dict()}")

        return df

    def prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """
        Prepare feature matrix X and target y.
        Handle missing values.
        """
        feature_cols = [d for d in DIMENSIONS if d in df.columns]

        X = df[feature_cols].copy()

        # Fill missing with median (conservative)
        for col in feature_cols:
            if X[col].isna().any():
                X[col] = X[col].fillna(X[col].median())

        X = X.values
        y = df['label_binary'].values

        logger.info(f"Features: {len(feature_cols)}, Samples: {len(X)}")
        logger.info(f"Feature columns: {feature_cols}")

        return X, y, feature_cols

    def train_model(self, X: np.ndarray, y: np.ndarray, feature_names: List[str]) -> Dict:
        """
        Train gradient boosting model with time-series cross-validation.
        Returns model metrics and feature importance.
        """
        logger.info("Training gradient boosting model...")

        # Time series split (no look-ahead)
        tscv = TimeSeriesSplit(n_splits=5)

        # Choose best available model
        if HAS_LIGHTGBM:
            logger.info("Using LightGBM")
            model = lgb.LGBMClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.05,
                num_leaves=31,
                min_child_samples=20,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                verbose=-1
            )
        elif HAS_XGBOOST:
            logger.info("Using XGBoost")
            model = xgb.XGBClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                use_label_encoder=False,
                eval_metric='logloss'
            )
        else:
            logger.info("Using sklearn GradientBoosting")
            model = GradientBoostingClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.8,
                random_state=42
            )

        # Cross-validation scores
        cv_scores = []
        cv_precision = []
        cv_recall = []

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            X_train, X_val = X[train_idx], X[val_idx]
            y_train, y_val = y[train_idx], y[val_idx]

            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_val_scaled = self.scaler.transform(X_val)

            model.fit(X_train_scaled, y_train)
            y_pred = model.predict(X_val_scaled)

            acc = accuracy_score(y_val, y_pred)
            prec = precision_score(y_val, y_pred, zero_division=0)
            rec = recall_score(y_val, y_pred, zero_division=0)

            cv_scores.append(acc)
            cv_precision.append(prec)
            cv_recall.append(rec)

            logger.info(f"  Fold {fold+1}: Accuracy={acc:.3f}, Precision={prec:.3f}, Recall={rec:.3f}")

        # Train final model on all data
        X_scaled = self.scaler.fit_transform(X)
        model.fit(X_scaled, y)
        self.model = model

        # Get feature importance
        if hasattr(model, 'feature_importances_'):
            importance = model.feature_importances_
        else:
            importance = np.zeros(len(feature_names))

        feature_importance = dict(zip(feature_names, importance))
        feature_importance = dict(sorted(feature_importance.items(), key=lambda x: x[1], reverse=True))

        results = {
            'cv_accuracy_mean': np.mean(cv_scores),
            'cv_accuracy_std': np.std(cv_scores),
            'cv_precision_mean': np.mean(cv_precision),
            'cv_recall_mean': np.mean(cv_recall),
            'feature_importance': feature_importance,
            'n_samples': len(X),
            'n_positive': int(y.sum()),
            'positive_rate': float(y.mean())
        }

        return results

    def analyze_by_quintile(self, df: pd.DataFrame, feature_names: List[str]) -> Dict:
        """
        Analyze which dimensions differ between top and bottom quintile stocks.
        This is a non-ML approach to validate feature importance.
        """
        logger.info("Analyzing dimension differences by return quintile...")

        # Compare top 20% vs bottom 20%
        top_quintile = df[df['forward_return_pct'] >= df['forward_return_pct'].quantile(0.8)]
        bottom_quintile = df[df['forward_return_pct'] <= df['forward_return_pct'].quantile(0.2)]

        differences = {}
        for dim in feature_names:
            if dim in df.columns:
                top_mean = top_quintile[dim].mean()
                bottom_mean = bottom_quintile[dim].mean()
                diff = top_mean - bottom_mean
                differences[dim] = {
                    'top_20%_mean': round(top_mean, 1),
                    'bottom_20%_mean': round(bottom_mean, 1),
                    'difference': round(diff, 1),
                    'predictive': 'Higher is better' if diff > 0 else 'Lower is better'
                }

        # Sort by absolute difference
        differences = dict(sorted(differences.items(), key=lambda x: abs(x[1]['difference']), reverse=True))

        return differences

    async def run_analysis(self, dry_run: bool = False) -> Dict:
        """Run full ML analysis pipeline."""
        await self.connect()

        try:
            # Get data
            df = await self.get_dimension_scores()

            if df.empty:
                logger.error("No data available for analysis")
                return {}

            # Stats
            stats = {
                'total_records': len(df),
                'unique_companies': df['company_id'].nunique(),
                'date_range': f"{df['score_date'].min()} to {df['score_date'].max()}",
                'unique_dates': df['score_date'].nunique(),
                'forward_return_mean': round(df['forward_return_pct'].mean(), 2),
                'forward_return_std': round(df['forward_return_pct'].std(), 2),
                'forward_return_median': round(df['forward_return_pct'].median(), 2)
            }

            logger.info(f"\n{'='*60}")
            logger.info(f"DATA SUMMARY ({self.horizon} forward returns)")
            logger.info(f"{'='*60}")
            for k, v in stats.items():
                logger.info(f"  {k}: {v}")

            if dry_run:
                return {'stats': stats}

            # Create labels
            df = self.create_labels(df)

            # Prepare features
            X, y, feature_names = self.prepare_features(df)

            # Train model
            logger.info(f"\n{'='*60}")
            logger.info("MODEL TRAINING (Time-Series Cross-Validation)")
            logger.info(f"{'='*60}")
            results = self.train_model(X, y, feature_names)

            # Quintile analysis
            quintile_analysis = self.analyze_by_quintile(df, feature_names)

            # Print results
            logger.info(f"\n{'='*60}")
            logger.info("RESULTS")
            logger.info(f"{'='*60}")
            logger.info(f"CV Accuracy: {results['cv_accuracy_mean']:.3f} (+/- {results['cv_accuracy_std']:.3f})")
            logger.info(f"CV Precision: {results['cv_precision_mean']:.3f}")
            logger.info(f"CV Recall: {results['cv_recall_mean']:.3f}")
            logger.info(f"Positive rate (>15% return): {results['positive_rate']:.1%}")

            logger.info(f"\n{'='*60}")
            logger.info("FEATURE IMPORTANCE (ML Model)")
            logger.info(f"{'='*60}")
            for dim, imp in results['feature_importance'].items():
                bar = '█' * int(imp * 50)
                logger.info(f"  {dim:25s} {imp:.4f} {bar}")

            logger.info(f"\n{'='*60}")
            logger.info("QUINTILE ANALYSIS (Top 20% vs Bottom 20% returns)")
            logger.info(f"{'='*60}")
            for dim, analysis in quintile_analysis.items():
                logger.info(f"  {dim:25s} Top: {analysis['top_20%_mean']:5.1f}  Bottom: {analysis['bottom_20%_mean']:5.1f}  Diff: {analysis['difference']:+5.1f}  ({analysis['predictive']})")

            return {
                'stats': stats,
                'model_results': results,
                'quintile_analysis': quintile_analysis
            }

        finally:
            await self.close()


async def main():
    parser = argparse.ArgumentParser(description='Train ML model on dimension scores')
    parser.add_argument('--horizon', type=str, default='6M',
                        choices=['1M', '3M', '6M', '12M'],
                        help='Forward return horizon')
    parser.add_argument('--dry-run', action='store_true',
                        help='Only show data statistics')
    parser.add_argument('--all-horizons', action='store_true',
                        help='Run analysis for all horizons')

    args = parser.parse_args()

    if args.all_horizons:
        all_results = {}
        for horizon in ['1M', '3M', '6M', '12M']:
            logger.info(f"\n\n{'#'*60}")
            logger.info(f"ANALYZING {horizon} FORWARD RETURNS")
            logger.info(f"{'#'*60}\n")
            trainer = DimensionMLTrainer(horizon=horizon)
            all_results[horizon] = await trainer.run_analysis(dry_run=args.dry_run)

        # Summary comparison
        if not args.dry_run:
            logger.info(f"\n\n{'='*60}")
            logger.info("CROSS-HORIZON COMPARISON")
            logger.info(f"{'='*60}")
            logger.info(f"{'Horizon':<10} {'Accuracy':<12} {'Precision':<12} {'Best Feature':<25}")
            logger.info("-" * 60)
            for horizon, res in all_results.items():
                if 'model_results' in res:
                    mr = res['model_results']
                    best_feat = list(mr['feature_importance'].keys())[0]
                    logger.info(f"{horizon:<10} {mr['cv_accuracy_mean']:.3f}        {mr['cv_precision_mean']:.3f}        {best_feat}")
    else:
        trainer = DimensionMLTrainer(horizon=args.horizon)
        await trainer.run_analysis(dry_run=args.dry_run)


if __name__ == '__main__':
    asyncio.run(main())
