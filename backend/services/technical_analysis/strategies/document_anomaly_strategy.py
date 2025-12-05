"""
Document Anomaly Strategy - Bridge between document intelligence and technical analysis.

Combines temporal anomaly detection from financial documents with technical analysis
to generate high-confidence trading signals.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import json
import hashlib

from .base import BaseStrategy, Signal, SignalType, StrategyType
from ..indicators.base import IndicatorResult


class DocumentAnomalyStrategy(BaseStrategy):
    """
    Trading strategy that combines document anomaly detection with technical analysis.
    
    This strategy:
    1. Detects temporal anomalies in financial document embeddings
    2. Validates signals with technical indicators (RSI, momentum, volume)
    3. Combines document intelligence with market data for high-confidence signals
    4. Uses adaptive position sizing based on anomaly strength and technical confirmation
    
    Signal Generation Logic:
    - Document anomaly detected → Check for technical confirmation
    - Strong technical + document signal → BUY/SELL
    - Weak technical + strong document → HOLD (wait for better entry)
    - Strong technical + no document → Pure technical signal
    """
    
    def __init__(self, **config):
        super().__init__(
            name="document_anomaly_strategy",
            description="Document anomaly detection with technical analysis confirmation",
            strategy_type=StrategyType.ML_HYBRID,
            required_indicators=["rsi_14", "sma_20", "volume_sma_20"],
            config=config
        )
        
        # Document anomaly parameters
        self.anomaly_lookback_days = config.get('anomaly_lookback_days', 30)
        self.min_anomaly_confidence = config.get('min_anomaly_confidence', 0.65)
        self.anomaly_threshold = config.get('anomaly_threshold', 0.3)  # Max similarity for anomaly
        
        # Technical analysis parameters  
        self.rsi_oversold = config.get('rsi_oversold', 30)
        self.rsi_overbought = config.get('rsi_overbought', 70)
        self.volume_surge_threshold = config.get('volume_surge_threshold', 1.5)
        
        # Signal combination parameters
        self.require_technical_confirmation = config.get('require_technical_confirmation', True)
        self.min_combined_confidence = config.get('min_combined_confidence', 0.7)
        
        # Database connection for document analysis
        self.db_conn = None
        self.processed_documents = set()
        
    async def setup_db_connection(self):
        """Set up database connection for document anomaly analysis."""
        if not self.db_conn:
            DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
            self.db_conn = await asyncpg.connect(DATABASE_URL)
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to consistent company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def generate_signal(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        current_date: date,
        indicator_values: Dict[str, IndicatorResult]
    ) -> Optional[Signal]:
        """Generate combined document + technical analysis signal."""
        
        await self.setup_db_connection()
        
        # Get company symbol for document lookup
        symbol = await self.get_symbol_from_company_id(company_id)
        if not symbol:
            return None
        
        # 1. Check for document anomalies
        document_signal = await self.analyze_document_anomalies(symbol, current_date)
        
        # 2. Generate technical analysis signal
        technical_signal = await self.generate_technical_signal(
            company_id, market_data, current_date, indicator_values
        )
        
        # 3. Combine signals
        combined_signal = await self.combine_signals(
            document_signal, technical_signal, company_id, current_date
        )
        
        return combined_signal
    
    async def get_symbol_from_company_id(self, company_id: int) -> Optional[str]:
        """Map company ID back to symbol for document lookup."""
        # Simple mapping - in production would use proper company master table
        common_symbols = [
            'VOLV-B.ST', 'ERIC-B.ST', 'HM-B.ST', 'SEB-A.ST', 'SAND.ST',
            'TEL2-B.ST', 'ASSA-B.ST', 'SKF-B.ST', 'ALFA.ST', 'INVE-B.ST'
        ]
        
        for symbol in common_symbols:
            if self.get_company_id(symbol) == company_id:
                return symbol
        
        return None
    
    async def analyze_document_anomalies(
        self, symbol: str, current_date: date
    ) -> Optional[Dict[str, Any]]:
        """Analyze recent document anomalies for the company."""
        
        if not self.db_conn:
            return None
            
        # Map symbol to company name for document lookup
        company_name = await self.get_company_name_from_symbol(symbol)
        if not company_name:
            return None
        
        # Get recent documents for anomaly analysis
        cutoff_date = current_date - timedelta(days=self.anomaly_lookback_days)
        
        recent_documents = await self.db_conn.fetch("""
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
            AND ed.company_name ILIKE $1
            AND ed.filing_date >= $2
            AND ed.filing_date <= $3
            ORDER BY ed.filing_date DESC
            LIMIT 5
        """, f'%{company_name}%', cutoff_date, current_date)
        
        if not recent_documents:
            return None
        
        # Analyze each document for anomalies
        strongest_anomaly = None
        total_anomaly_score = 0.0
        anomaly_types = []
        
        for doc in recent_documents:
            document_id = doc['document_id']
            
            # Skip if already processed
            if document_id in self.processed_documents:
                continue
                
            anomalies = await self.detect_document_anomalies(doc)
            
            if anomalies:
                self.processed_documents.add(document_id)
                
                for anomaly in anomalies:
                    if strongest_anomaly is None or anomaly['confidence'] > strongest_anomaly['confidence']:
                        strongest_anomaly = anomaly
                    
                    total_anomaly_score += anomaly['confidence']
                    anomaly_types.append(anomaly['section_type'])
        
        if strongest_anomaly is None:
            return None
        
        # Calculate aggregate anomaly signal
        avg_confidence = total_anomaly_score / len(anomaly_types) if anomaly_types else 0.0
        
        # Determine signal direction based on anomaly types
        risk_sections = ['risk_factors', 'management_discussion']
        financial_sections = ['balance_sheet', 'income_statement', 'cash_flow_statement']
        
        risk_count = sum(1 for t in anomaly_types if t in risk_sections)
        financial_count = sum(1 for t in anomaly_types if t in financial_sections)
        
        # Signal direction logic
        if risk_count > financial_count:
            signal_direction = SignalType.SELL  # Risk anomalies → negative
        elif financial_count > risk_count:
            signal_direction = SignalType.BUY   # Financial anomalies → positive  
        else:
            signal_direction = SignalType.HOLD  # Mixed signals
        
        return {
            'signal_type': signal_direction,
            'confidence': avg_confidence,
            'anomaly_strength': strongest_anomaly['anomaly_strength'],
            'anomaly_types': anomaly_types,
            'num_anomalies': len(anomaly_types),
            'strongest_section': strongest_anomaly['section_type'],
            'document_date': max(doc['filing_date'] for doc in recent_documents)
        }
    
    async def detect_document_anomalies(self, document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect anomalies in a specific document."""
        document_id = document['document_id']
        company_name = document['company_name']
        current_year = document['year']
        
        # Get sections with embeddings
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
        
        for section in current_sections:
            section_type = section['section_type']
            
            # Parse embedding (stored as string)
            try:
                if isinstance(section['embedding'], str):
                    current_embedding = eval(section['embedding'])
                else:
                    current_embedding = section['embedding']
            except:
                continue
            
            # Get historical baseline
            baseline_similarity = await self.get_baseline_similarity(
                company_name, section_type, current_embedding, current_year
            )
            
            if baseline_similarity is not None and baseline_similarity < self.anomaly_threshold:
                anomaly_strength = max(0, self.anomaly_threshold - baseline_similarity)
                confidence = min(1.0, anomaly_strength * 2.5)  # Scale to confidence
                
                if confidence >= self.min_anomaly_confidence:
                    anomalies.append({
                        'section_type': section_type,
                        'section_title': section['section_title'],
                        'baseline_similarity': baseline_similarity,
                        'anomaly_strength': anomaly_strength,
                        'confidence': confidence
                    })
        
        return anomalies
    
    async def get_baseline_similarity(
        self, company_name: str, section_type: str, 
        current_embedding: List[float], current_year: int
    ) -> Optional[float]:
        """Calculate similarity to historical baseline."""
        
        # Get historical embeddings
        historical_embeddings = await self.db_conn.fetch("""
            SELECT se.embedding
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE ed.company_name ILIKE $1
            AND ds.section_type = $2
            AND ed.year < $3
            AND ed.year >= $4
            AND se.embedding_model LIKE 'local/%'
        """, f'%{company_name}%', section_type, current_year, current_year - 3)
        
        if len(historical_embeddings) < 2:
            return None
        
        # Calculate average similarity to historical embeddings
        similarities = []
        for hist_row in historical_embeddings:
            try:
                if isinstance(hist_row['embedding'], str):
                    hist_embedding = eval(hist_row['embedding'])
                else:
                    hist_embedding = hist_row['embedding']
                    
                similarity = self.cosine_similarity(current_embedding, hist_embedding)
                similarities.append(similarity)
            except:
                continue
        
        return np.mean(similarities) if similarities else None
    
    def cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between vectors."""
        if len(vec1) != len(vec2):
            return 0.0
        
        vec1_np = np.array(vec1)
        vec2_np = np.array(vec2)
        
        dot_product = np.dot(vec1_np, vec2_np)
        norm1 = np.linalg.norm(vec1_np)
        norm2 = np.linalg.norm(vec2_np)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    async def generate_technical_signal(
        self, company_id: int, market_data: pd.DataFrame,
        current_date: date, indicator_values: Dict[str, IndicatorResult]
    ) -> Optional[Dict[str, Any]]:
        """Generate pure technical analysis signal."""
        
        # Get indicator values for current date
        rsi_result = indicator_values.get('rsi_14')
        sma_result = indicator_values.get('sma_20') 
        volume_result = indicator_values.get('volume_sma_20')
        
        if not all([rsi_result, sma_result, volume_result]):
            return None
        
        rsi_value = rsi_result.get_value(current_date)
        sma_value = sma_result.get_value(current_date)
        volume_avg = volume_result.get_value(current_date)
        
        if None in [rsi_value, sma_value, volume_avg]:
            return None
        
        # Get current market data
        current_data = market_data.loc[market_data.index.date == current_date]
        if current_data.empty:
            return None
        
        current_price = float(current_data['close'].iloc[-1])
        current_volume = float(current_data['volume'].iloc[-1])
        
        # Technical analysis logic
        signal_type = SignalType.HOLD
        confidence = 0.5
        strength = 0.0
        
        # RSI signals
        if rsi_value <= self.rsi_oversold:
            signal_type = SignalType.BUY
            confidence = 0.7 + (self.rsi_oversold - rsi_value) / 100  # Higher confidence for lower RSI
            strength = (self.rsi_oversold - rsi_value) / 30  # Strength based on how oversold
            
        elif rsi_value >= self.rsi_overbought:
            signal_type = SignalType.SELL
            confidence = 0.7 + (rsi_value - self.rsi_overbought) / 100  # Higher confidence for higher RSI
            strength = (rsi_value - self.rsi_overbought) / 30  # Strength based on how overbought
        
        # Price vs SMA confirmation
        price_vs_sma = current_price / sma_value
        if signal_type == SignalType.BUY and price_vs_sma < 0.98:
            confidence += 0.1  # Price below SMA confirms oversold condition
        elif signal_type == SignalType.SELL and price_vs_sma > 1.02:
            confidence += 0.1  # Price above SMA confirms overbought condition
        
        # Volume confirmation
        volume_ratio = current_volume / volume_avg
        if volume_ratio >= self.volume_surge_threshold:
            confidence += 0.15  # Volume surge increases confidence
            strength += 0.1
        
        confidence = min(1.0, confidence)  # Cap at 100%
        
        return {
            'signal_type': signal_type,
            'confidence': confidence,
            'strength': strength,
            'rsi_value': rsi_value,
            'price_vs_sma': price_vs_sma,
            'volume_ratio': volume_ratio
        }
    
    async def combine_signals(
        self, document_signal: Optional[Dict[str, Any]], 
        technical_signal: Optional[Dict[str, Any]],
        company_id: int, current_date: date
    ) -> Optional[Signal]:
        """Combine document anomaly and technical signals."""
        
        if not document_signal and not technical_signal:
            return None
        
        # Case 1: Only technical signal
        if technical_signal and not document_signal:
            if technical_signal['signal_type'] == SignalType.HOLD:
                return None
            
            return Signal(
                signal_type=technical_signal['signal_type'],
                confidence=technical_signal['confidence'] * 0.8,  # Reduce confidence for pure technical
                strength=technical_signal['strength'],
                company_id=company_id,
                date=current_date,
                contributing_factors={
                    'signal_source': 'technical_only',
                    'rsi_value': technical_signal['rsi_value'],
                    'price_vs_sma': technical_signal['price_vs_sma'],
                    'volume_ratio': technical_signal['volume_ratio']
                },
                metadata={'strategy': 'document_anomaly', 'signal_type': 'technical_only'}
            )
        
        # Case 2: Only document signal
        if document_signal and not technical_signal:
            if not self.require_technical_confirmation:
                return Signal(
                    signal_type=document_signal['signal_type'],
                    confidence=document_signal['confidence'] * 0.9,  # Slight reduction without technical confirmation
                    strength=document_signal['anomaly_strength'],
                    company_id=company_id,
                    date=current_date,
                    contributing_factors={
                        'signal_source': 'document_only',
                        'anomaly_types': document_signal['anomaly_types'],
                        'strongest_section': document_signal['strongest_section']
                    },
                    metadata={'strategy': 'document_anomaly', 'signal_type': 'document_only'}
                )
            else:
                return None  # Require technical confirmation
        
        # Case 3: Both signals available
        if document_signal and technical_signal:
            
            # Check signal agreement
            if document_signal['signal_type'] == technical_signal['signal_type']:
                # Signals agree → High confidence combined signal
                combined_confidence = np.sqrt(
                    document_signal['confidence'] * technical_signal['confidence']
                )
                combined_strength = max(document_signal['anomaly_strength'], technical_signal['strength'])
                
                return Signal(
                    signal_type=document_signal['signal_type'],
                    confidence=min(1.0, combined_confidence * 1.2),  # Boost for agreement
                    strength=combined_strength,
                    company_id=company_id,
                    date=current_date,
                    contributing_factors={
                        'signal_source': 'combined_agreement',
                        'document_confidence': document_signal['confidence'],
                        'technical_confidence': technical_signal['confidence'],
                        'anomaly_types': document_signal['anomaly_types'],
                        'rsi_value': technical_signal['rsi_value'],
                        'volume_ratio': technical_signal['volume_ratio']
                    },
                    metadata={
                        'strategy': 'document_anomaly', 
                        'signal_type': 'combined_agreement',
                        'document_date': document_signal.get('document_date')
                    }
                )
            
            elif document_signal['signal_type'] != SignalType.HOLD and technical_signal['signal_type'] != SignalType.HOLD:
                # Signals disagree → Conservative approach, use stronger signal with reduced confidence
                if document_signal['confidence'] > technical_signal['confidence']:
                    primary_signal = document_signal
                    secondary = technical_signal
                    signal_source = 'document_primary'
                else:
                    primary_signal = technical_signal
                    secondary = document_signal
                    signal_source = 'technical_primary'
                
                return Signal(
                    signal_type=primary_signal['signal_type'],
                    confidence=primary_signal['confidence'] * 0.7,  # Reduce for disagreement
                    strength=primary_signal.get('strength', primary_signal.get('anomaly_strength', 0.1)),
                    company_id=company_id,
                    date=current_date,
                    contributing_factors={
                        'signal_source': signal_source,
                        'primary_confidence': primary_signal['confidence'],
                        'conflicting_signal': True
                    },
                    metadata={'strategy': 'document_anomaly', 'signal_type': 'conflicting_resolved'}
                )
            
            else:
                # One HOLD, one action signal → Use action signal with reduced confidence
                action_signal = document_signal if document_signal['signal_type'] != SignalType.HOLD else technical_signal
                
                return Signal(
                    signal_type=action_signal['signal_type'],
                    confidence=action_signal['confidence'] * 0.8,  # Reduce for partial confirmation
                    strength=action_signal.get('strength', action_signal.get('anomaly_strength', 0.1)),
                    company_id=company_id,
                    date=current_date,
                    contributing_factors={
                        'signal_source': 'partial_confirmation',
                        'action_signal_confidence': action_signal['confidence']
                    },
                    metadata={'strategy': 'document_anomaly', 'signal_type': 'partial_confirmation'}
                )
        
        return None
    
    async def get_company_name_from_symbol(self, symbol: str) -> Optional[str]:
        """Map symbol to company name for document lookup."""
        
        # Simplified mapping for major Nordic companies
        symbol_to_company = {
            'VOLV-B.ST': 'Volvo Group',
            'ERIC-B.ST': 'Ericsson', 
            'HM-B.ST': 'H&M',
            'SEB-A.ST': 'SEB',
            'SAND.ST': 'Sandvik',
            'TEL2-B.ST': 'Tele2',
            'ASSA-B.ST': 'ASSA ABLOY',
            'SKF-B.ST': 'SKF',
            'ALFA.ST': 'Alfa Laval',
            'INVE-B.ST': 'Investor'
        }
        
        return symbol_to_company.get(symbol)
    
    async def cleanup(self):
        """Clean up database connection."""
        if self.db_conn:
            await self.db_conn.close()
            self.db_conn = None


async def test_document_anomaly_strategy():
    """Test the document anomaly strategy."""
    print("🧪 Testing Document Anomaly Strategy...")
    
    strategy = DocumentAnomalyStrategy(
        anomaly_lookback_days=30,
        min_anomaly_confidence=0.6,
        require_technical_confirmation=True,
        min_combined_confidence=0.7
    )
    
    print(f"📋 Strategy: {strategy.name}")
    print(f"   Type: {strategy.strategy_type}")
    print(f"   Required indicators: {strategy.required_indicators}")
    
    # Test configuration
    print(f"   Anomaly lookback: {strategy.anomaly_lookback_days} days")
    print(f"   Min anomaly confidence: {strategy.min_anomaly_confidence}")
    print(f"   Require technical confirmation: {strategy.require_technical_confirmation}")
    
    print("✅ Document Anomaly Strategy test completed!")


if __name__ == "__main__":
    asyncio.run(test_document_anomaly_strategy())