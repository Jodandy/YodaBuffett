#!/usr/bin/env python3
"""
Temporal Anomaly Strategy for YodaBuffett Backtesting.

Implements our proven temporal anomaly detection as a tradeable strategy.
Uses existing embeddings and anomaly detection to generate trading signals.
"""

import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
import asyncio
import asyncpg
import sys
from pathlib import Path

# Add backend to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from domains.document_intelligence.factory import get_database_url
from ..models.backtesting import (
    Strategy, TradingSignal, Position, MarketData, BacktestConfig,
    SignalType, SignalSource
)

logger = logging.getLogger(__name__)


class TemporalAnomalyStrategy(Strategy):
    """
    Trading strategy based on temporal anomaly detection in financial documents.
    
    This strategy:
    1. Detects anomalies in company communication patterns using embeddings
    2. Generates trading signals when anomalies exceed confidence thresholds
    3. Manages position sizing based on anomaly strength and confidence
    4. Implements risk management through stop-losses and position limits
    
    Based on validated results:
    - AAK 2020-2021: Balance sheet anomaly → Major asset/debt spike
    - AcadeMedia 2017-2018: Risk factor changes → Swedish law changes  
    - AddLife 2018-2019: Income statement anomaly → 40% revenue growth
    """
    
    def __init__(self, **params):
        super().__init__("TemporalAnomalyStrategy", "1.0", **params)
        
        # Strategy parameters with sensible defaults
        self.min_confidence = params.get('min_confidence', 0.65)  # Minimum anomaly confidence
        self.anomaly_threshold = params.get('anomaly_threshold', 0.3)  # Max similarity for anomaly
        self.lookback_periods = params.get('lookback_periods', 8)  # Quarters to look back for baseline
        self.holding_period_days = params.get('holding_period_days', 60)  # Target holding period
        self.position_sizing_method = params.get('position_sizing_method', 'confidence_weighted')
        
        # Risk management
        self.max_position_size = params.get('max_position_size', 0.08)  # 8% max per position
        self.stop_loss_threshold = params.get('stop_loss_threshold', -0.12)  # 12% stop loss
        self.take_profit_threshold = params.get('take_profit_threshold', 0.20)  # 20% take profit
        
        # Database connection
        self.db_conn = None
        
        # Strategy state
        self.last_anomaly_scan: Optional[datetime] = None
        self.processed_documents: set = set()
        
        logger.info(f"🧠 Initialized {self.get_description()}")
        logger.info(f"   Min confidence: {self.min_confidence:.1%}")
        logger.info(f"   Anomaly threshold: {self.anomaly_threshold:.2f}")
        logger.info(f"   Lookback periods: {self.lookback_periods}")
    
    async def setup(self, config: BacktestConfig) -> None:
        """Initialize strategy with database connection and validation"""
        logger.info("🚀 Setting up Temporal Anomaly Strategy...")
        
        # Connect to database
        self.db_conn = await asyncpg.connect(get_database_url())
        
        # Validate we have necessary data
        await self._validate_data_availability(config)
        
        self._setup_complete = True
        logger.info("✅ Strategy setup complete")
    
    async def _validate_data_availability(self, config: BacktestConfig) -> None:
        """Check if we have sufficient data for the backtest period"""
        
        # Check embeddings availability
        embeddings_count = await self.db_conn.fetchval("""
            SELECT COUNT(*)
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE se.embedding_model LIKE 'local/%'
            AND ed.year BETWEEN $1 AND $2
        """, config.start_date.year - 1, config.end_date.year)
        
        if embeddings_count < 100:
            raise ValueError(f"Insufficient embedding data: {embeddings_count} embeddings found")
        
        logger.info(f"📊 Found {embeddings_count:,} embeddings for analysis")
        
        # Check document coverage
        companies_with_data = await self.db_conn.fetchval("""
            SELECT COUNT(DISTINCT ed.company_name)
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE se.embedding_model LIKE 'local/%'
        """)
        
        logger.info(f"🏢 Found {companies_with_data} companies with embedding data")
    
    async def generate_signals(
        self,
        current_date: datetime,
        market_data: Dict[str, MarketData],
        portfolio_state: Dict[str, Any]
    ) -> List[TradingSignal]:
        """
        Generate trading signals based on temporal anomalies detected in recent documents.
        """
        if not self._setup_complete or not self.db_conn:
            return []
        
        signals = []
        
        try:
            # Get recent documents that might have anomalies
            recent_documents = await self._get_recent_documents(current_date)
            
            logger.debug(f"🔍 Analyzing {len(recent_documents)} recent documents for anomalies")
            
            for doc in recent_documents:
                # Skip if we've already processed this document
                if doc['document_id'] in self.processed_documents:
                    continue
                
                # Detect anomalies for this document
                anomalies = await self._detect_document_anomalies(doc)
                
                if anomalies:
                    # Convert anomalies to trading signals
                    doc_signals = await self._create_signals_from_anomalies(
                        doc, anomalies, current_date, market_data
                    )
                    signals.extend(doc_signals)
                    
                    # Mark as processed
                    self.processed_documents.add(doc['document_id'])
            
            if signals:
                logger.info(f"🚨 Generated {len(signals)} anomaly-based signals")
                for signal in signals:
                    anomaly_info = signal.metadata.get('anomaly_type', 'unknown')
                    logger.info(f"   {signal.signal_type.value} {signal.symbol} ({anomaly_info}, confidence: {signal.confidence:.1%})")
            
        except Exception as e:
            logger.error(f"❌ Error generating signals: {e}")
        
        return signals
    
    async def _get_recent_documents(self, current_date: datetime) -> List[Dict[str, Any]]:
        """Get documents published in the last 30 days that we can analyze"""
        
        cutoff_date = current_date - timedelta(days=30)
        
        documents = await self.db_conn.fetch("""
            SELECT DISTINCT 
                ed.id as document_id,
                ed.company_name,
                ed.form_type,
                ed.year,
                ed.filing_date,
                ed.created_at
            FROM extracted_documents ed
            JOIN document_sections ds ON ed.id = ds.extracted_document_id  
            JOIN section_embeddings se ON ds.id = se.document_section_id
            WHERE se.embedding_model LIKE 'local/%'
            AND ed.filing_date >= $1
            AND ed.filing_date <= $2
            ORDER BY ed.filing_date DESC
            LIMIT 50
        """, cutoff_date.date(), current_date.date())
        
        return [dict(doc) for doc in documents]
    
    async def _detect_document_anomalies(self, document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect temporal anomalies for a specific document by comparing its sections
        to historical baselines for the same company.
        """
        company_name = document['company_name']
        document_id = document['document_id']
        
        # Get sections and embeddings for this document
        current_sections = await self.db_conn.fetch("""
            SELECT 
                ds.section_type,
                ds.section_title,
                se.embedding,
                ds.section_confidence
            FROM document_sections ds
            JOIN section_embeddings se ON ds.id = se.document_section_id
            WHERE ds.extracted_document_id = $1
            AND se.embedding_model LIKE 'local/%'
            AND ds.section_type IN ('balance_sheet', 'income_statement', 'risk_factors', 
                                   'management_discussion', 'cash_flow_statement')
        """, document_id)
        
        if not current_sections:
            return []
        
        anomalies = []
        
        # Check each section type for anomalies
        for section in current_sections:
            section_type = section['section_type']
            current_embedding = eval(section['embedding'])  # Convert string back to list
            
            # Get historical baseline for this company and section type
            baseline_similarity = await self._get_baseline_similarity(
                company_name, section_type, current_embedding, document['year']
            )
            
            # Check if this represents an anomaly
            if baseline_similarity is not None and baseline_similarity < self.anomaly_threshold:
                
                # Calculate anomaly strength (how different from baseline)
                anomaly_strength = max(0, self.anomaly_threshold - baseline_similarity)
                confidence = min(1.0, anomaly_strength * 2.0)  # Scale to confidence
                
                # Only consider high-confidence anomalies
                if confidence >= self.min_confidence:
                    anomalies.append({
                        'section_type': section_type,
                        'section_title': section['section_title'],
                        'baseline_similarity': baseline_similarity,
                        'anomaly_strength': anomaly_strength,
                        'confidence': confidence,
                        'section_confidence': section['section_confidence']
                    })
        
        return anomalies
    
    async def _get_baseline_similarity(
        self,
        company_name: str,
        section_type: str,
        current_embedding: List[float],
        current_year: int
    ) -> Optional[float]:
        """
        Calculate similarity to historical baseline for this company's section type.
        Uses embeddings from previous years to establish normal communication patterns.
        """
        
        # Get historical embeddings for this company and section type
        historical_embeddings = await self.db_conn.fetch("""
            SELECT se.embedding
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE ed.company_name = $1
            AND ds.section_type = $2
            AND ed.year < $3
            AND ed.year >= $4
            AND se.embedding_model LIKE 'local/%'
        """, company_name, section_type, current_year, current_year - 3)  # 3 years of history
        
        if len(historical_embeddings) < 2:  # Need minimum baseline data
            return None
        
        # Calculate similarities to all historical embeddings
        similarities = []
        for hist_emb_row in historical_embeddings:
            hist_embedding = eval(hist_emb_row['embedding'])
            similarity = self._cosine_similarity(current_embedding, hist_embedding)
            similarities.append(similarity)
        
        # Return average similarity to historical baseline
        return sum(similarities) / len(similarities) if similarities else None
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        import math
        
        if len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(a * a for a in vec2))
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    async def _create_signals_from_anomalies(
        self,
        document: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        current_date: datetime,
        market_data: Dict[str, MarketData]
    ) -> List[TradingSignal]:
        """Convert detected anomalies into trading signals"""
        
        company_name = document['company_name']
        
        # Map company name to stock symbol
        symbol = await self._get_stock_symbol(company_name)
        if not symbol or symbol not in market_data:
            logger.debug(f"⚠️ No market data for {company_name} (symbol: {symbol})")
            return []
        
        signals = []
        
        # Aggregate anomaly information
        total_confidence = 0.0
        anomaly_types = []
        strongest_anomaly = max(anomalies, key=lambda x: x['confidence'])
        
        for anomaly in anomalies:
            total_confidence += anomaly['confidence']
            anomaly_types.append(anomaly['section_type'])
        
        # Determine signal direction based on anomaly types
        # This is based on our validated results:
        # - Balance sheet anomalies often indicate fundamental changes (can be + or -)
        # - Risk factor changes often indicate negative developments
        # - Management discussion changes can indicate strategic shifts
        
        risk_sections = ['risk_factors', 'management_discussion']
        financial_sections = ['balance_sheet', 'income_statement', 'cash_flow_statement']
        
        risk_anomalies = [a for a in anomalies if a['section_type'] in risk_sections]
        financial_anomalies = [a for a in anomalies if a['section_type'] in financial_sections]
        
        # Default to neutral, then determine direction
        signal_type = SignalType.HOLD
        expected_return = 0.0
        
        if risk_anomalies and not financial_anomalies:
            # Risk-only anomalies tend to be negative
            signal_type = SignalType.SELL
            expected_return = -0.08  # Expect -8% return
            
        elif financial_anomalies and not risk_anomalies:
            # Financial anomalies without risk changes can be positive
            # (like AddLife's revenue growth)
            signal_type = SignalType.BUY
            expected_return = 0.10  # Expect +10% return
            
        elif len(anomalies) >= 3:
            # Multiple section anomalies indicate major changes
            # Direction depends on the type of the strongest anomaly
            if strongest_anomaly['section_type'] in risk_sections:
                signal_type = SignalType.SELL
                expected_return = -0.12  # Expect -12% return for major risk changes
            else:
                signal_type = SignalType.BUY
                expected_return = 0.15  # Expect +15% return for major financial changes
        
        # Only generate signal if we have a clear direction
        if signal_type != SignalType.HOLD:
            
            # Calculate final confidence (average but cap at reasonable level)
            final_confidence = min(0.85, total_confidence / len(anomalies))
            
            signal = TradingSignal(
                timestamp=current_date,
                symbol=symbol,
                signal_type=signal_type,
                signal_source=SignalSource.TEMPORAL_ANOMALY,
                confidence=final_confidence,
                strength=abs(expected_return),
                target_horizon_days=self.holding_period_days,
                strategy_name=self.name,
                strategy_version=self.version,
                metadata={
                    'company_name': company_name,
                    'document_date': document['filing_date'].isoformat() if document['filing_date'] else None,
                    'anomaly_types': anomaly_types,
                    'num_anomalies': len(anomalies),
                    'strongest_anomaly': strongest_anomaly['section_type'],
                    'baseline_similarity': strongest_anomaly['baseline_similarity'],
                    'expected_return': expected_return,
                    'document_id': document['document_id']
                }
            )
            
            signals.append(signal)
        
        return signals
    
    async def _get_stock_symbol(self, company_name: str) -> Optional[str]:
        """Map company name to stock ticker symbol"""
        
        # Simplified mapping for major Nordic companies
        # In production, this would use a proper company-to-symbol mapping database
        symbol_mapping = {
            'Volvo': 'VOLV-B.ST',
            'Volvo Group': 'VOLV-B.ST', 
            'Ericsson': 'ERIC-B.ST',
            'H&M': 'HM-B.ST',
            'SEB': 'SEB-A.ST',
            'Sandvik': 'SAND.ST',
            'Tele2': 'TEL2-B.ST',
            'ASSA ABLOY': 'ASSA-B.ST',
            'SKF': 'SKF-B.ST',
            'Alfa Laval': 'ALFA.ST',
            'Investor': 'INVE-B.ST',
            'AddLife': 'ALIF.ST',  # Would need real symbol
            'AAK': 'AAK.ST',       # Would need real symbol
            'AcadeMedia': 'ACEM.ST' # Would need real symbol
        }
        
        # Try exact match first
        if company_name in symbol_mapping:
            return symbol_mapping[company_name]
        
        # Try partial matches
        for company, symbol in symbol_mapping.items():
            if company.lower() in company_name.lower() or company_name.lower() in company.lower():
                return symbol
        
        # Default mapping for testing
        return f"{company_name.upper().replace(' ', '')}.ST"
    
    async def should_exit_position(
        self,
        position: Position,
        current_date: datetime,
        market_data: MarketData,
        portfolio_state: Dict[str, Any]
    ) -> bool:
        """Determine if position should be exited based on strategy rules"""
        
        current_return = position.current_return(market_data.adjusted_close)
        days_held = (current_date - position.entry_date).days
        
        # Stop loss
        if current_return <= self.stop_loss_threshold:
            logger.info(f"🔴 Stop loss triggered for {position.symbol}: {current_return:.1%}")
            return True
        
        # Take profit
        if current_return >= self.take_profit_threshold:
            logger.info(f"🟢 Take profit triggered for {position.symbol}: {current_return:.1%}")
            return True
        
        # Time-based exit (hold for target period)
        if days_held >= self.holding_period_days:
            logger.info(f"⏰ Time exit for {position.symbol} after {days_held} days: {current_return:.1%}")
            return True
        
        # Check if new conflicting anomaly detected for same company
        # (This would require additional logic to detect new anomalies)
        
        return False
    
    def get_position_size(
        self,
        signal: TradingSignal,
        portfolio_state: Dict[str, Any],
        config: BacktestConfig
    ) -> float:
        """Calculate position size based on signal confidence and anomaly strength"""
        
        if self.position_sizing_method == 'confidence_weighted':
            # Base size on confidence and anomaly strength
            base_size = 0.04  # 4% base position
            confidence_multiplier = signal.confidence  # Scale by confidence
            strength_multiplier = min(2.0, signal.strength * 10)  # Scale by expected return
            
            position_size = base_size * confidence_multiplier * strength_multiplier
            
        elif self.position_sizing_method == 'equal_weight':
            # Equal weight across all positions
            max_positions = config.max_positions
            position_size = 0.8 / max_positions  # 80% invested equally
            
        else:  # 'fixed'
            position_size = 0.05  # Fixed 5% per position
        
        # Apply limits
        position_size = min(position_size, self.max_position_size)
        position_size = max(position_size, config.min_position_size)
        
        return position_size
    
    async def teardown(self) -> None:
        """Clean up database connection"""
        if self.db_conn:
            await self.db_conn.close()
        
        logger.info("🧹 Temporal Anomaly Strategy cleaned up")
    
    def get_description(self) -> str:
        """Return strategy description"""
        return f"Temporal Anomaly Strategy v{self.version} (confidence≥{self.min_confidence:.1%}, threshold≤{self.anomaly_threshold})"


if __name__ == "__main__":
    async def test_strategy():
        """Test the temporal anomaly strategy"""
        print("🧪 Testing Temporal Anomaly Strategy...")
        
        # Create strategy with test parameters
        strategy = TemporalAnomalyStrategy(
            min_confidence=0.6,
            anomaly_threshold=0.4,
            holding_period_days=45
        )
        
        print(f"📋 Strategy: {strategy.get_description()}")
        
        # Test basic functionality (without database)
        from ..models.backtesting import BacktestConfig
        from datetime import date
        
        config = BacktestConfig(
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
            initial_capital=1000000
        )
        
        # Test position sizing
        signal = TradingSignal(
            symbol="VOLV-B.ST",
            signal_type=SignalType.BUY,
            signal_source=SignalSource.TEMPORAL_ANOMALY,
            confidence=0.75,
            strength=0.12
        )
        
        portfolio_state = {'total_value': 1000000, 'cash': 500000, 'position_count': 5}
        position_size = strategy.get_position_size(signal, portfolio_state, config)
        
        print(f"💰 Position size for {signal.confidence:.1%} confidence signal: {position_size:.1%}")
        
        print("✅ Basic strategy test completed!")
    
    asyncio.run(test_strategy())