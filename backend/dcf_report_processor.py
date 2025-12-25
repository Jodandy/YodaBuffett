#!/usr/bin/env python3
"""
DCF Report Processor

Processes each financial report and stores Monte Carlo DCF valuations
in the dcf_valuations table. This separates valuation computation from
daily price comparisons, allowing for efficient backtesting.
"""

import asyncio
import asyncpg
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import asdict
import logging

from clean_dcf_engine import CleanDCFEngine, DCFConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DCFReportProcessor:
    """Processes financial reports and stores DCF valuations."""
    
    def __init__(self, model_version: str = "clean_dcf_v1.0"):
        self.model_version = model_version
        self.dcf_config = DCFConfig(num_simulations=5000, projection_years=10)
        self.dcf_engine = CleanDCFEngine(self.dcf_config)
        self.db_conn = None
        
        # Performance tracking
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = None
    
    async def setup(self):
        """Initialize database connections."""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        await self.dcf_engine.setup()
        self.start_time = time.time()
        logger.info(f"DCF Report Processor initialized with model version: {self.model_version}")
    
    async def cleanup(self):
        """Close database connections."""
        if self.db_conn:
            await self.db_conn.close()
        await self.dcf_engine.cleanup()
    
    async def get_unprocessed_reports(self, limit: int = None) -> List[Dict]:
        """Get financial reports that haven't been processed with current model version."""
        
        query = """
        SELECT DISTINCT
            fs.symbol,
            fs.period_date as report_date,
            fs.publish_date,
            CASE 
                WHEN (EXTRACT(MONTH FROM fs.period_date) = 12 AND EXTRACT(DAY FROM fs.period_date) >= 28)
                     OR (EXTRACT(MONTH FROM fs.period_date) = 1 AND EXTRACT(DAY FROM fs.period_date) <= 3)
                THEN 'annual'
                ELSE 'quarterly'
            END as report_type,
            cm.stock_currency,
            cm.report_currency
        FROM financial_statements fs
        JOIN company_master cm ON fs.symbol = cm.primary_ticker
        LEFT JOIN dcf_valuations dv ON (
            dv.symbol = fs.symbol 
            AND dv.report_date = fs.period_date 
            AND dv.model_version = $1
        )
        WHERE fs.publish_date IS NOT NULL
        AND fs.total_revenue > 0
        AND dv.id IS NULL  -- Not already processed
        AND fs.publish_date >= '2020-01-01'  -- Focus on recent data
        ORDER BY fs.publish_date DESC, fs.symbol
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        rows = await self.db_conn.fetch(query, self.model_version)
        
        logger.info(f"Found {len(rows)} unprocessed financial reports")
        return [dict(row) for row in rows]
    
    def calculate_data_quality_score(self, company_data, periods_used: int) -> Tuple[float, List[str]]:
        """Calculate data quality score and estimation flags."""
        
        score = 1.0
        flags = []
        
        # Penalize for insufficient data
        if periods_used < 4:
            score -= 0.3
            flags.append("insufficient_periods")
        
        # Check for missing cash flow data
        if not company_data.operating_cash_flows or all(cf == 0 for cf in company_data.operating_cash_flows[:2]):
            score -= 0.2
            flags.append("estimated_cash_flows")
        
        # Check for missing balance sheet data
        if not company_data.total_debt or not company_data.cash_equivalents:
            score -= 0.1
            flags.append("incomplete_balance_sheet")
        
        # Check for extreme values
        if company_data.operating_margins and any(abs(m) > 1.0 for m in company_data.operating_margins[:2]):
            score -= 0.1
            flags.append("extreme_margins")
        
        # Check tax rate reasonableness
        if company_data.effective_tax_rates and any(t > 0.6 or t < 0 for t in company_data.effective_tax_rates[:2]):
            score -= 0.1
            flags.append("unreasonable_tax_rates")
        
        return max(0.0, min(1.0, score)), flags
    
    def calculate_valuation_confidence(self, dcf_results: Dict, data_quality_score: float) -> float:
        """Calculate confidence in valuation based on consistency and data quality."""
        
        # Coefficient of variation (consistency)
        cv = dcf_results['fair_value_std'] / dcf_results['fair_value_median'] if dcf_results['fair_value_median'] > 0 else 1.0
        consistency_score = max(0, 1 - min(cv, 1.0))
        
        # Range consistency (how tight is the confidence interval)
        p25 = dcf_results['fair_value_p25']
        p75 = dcf_results['fair_value_p75']
        median = dcf_results['fair_value_median']
        
        if median > 0:
            range_factor = (p75 - p25) / median
            range_score = max(0, 1 - min(range_factor, 1.0))
        else:
            range_score = 0.0
        
        # Combined confidence
        confidence = (consistency_score * 0.4) + (range_score * 0.3) + (data_quality_score * 0.3)
        
        return max(0.1, min(1.0, confidence))
    
    async def process_report(self, report: Dict) -> bool:
        """Process a single financial report and store DCF valuation."""
        
        symbol = report['symbol']
        report_date = report['report_date']
        publish_date = report['publish_date']
        
        start_time = time.time()
        
        try:
            logger.info(f"Processing {symbol} - {report_date} (published {publish_date})")
            
            # Extract company data at the publish date
            valuation_date = datetime.combine(publish_date, datetime.min.time())
            company_data = await self.dcf_engine.extract_company_data(symbol, valuation_date)
            
            if not company_data:
                logger.warning(f"No company data for {symbol} at {publish_date}")
                return False
            
            # Calculate data quality
            periods_used = len(company_data.revenues)
            data_quality_score, estimation_flags = self.calculate_data_quality_score(company_data, periods_used)
            
            # Run Monte Carlo DCF (without market price - we just want intrinsic value)
            dcf_results = self.dcf_engine.run_monte_carlo_simulation(company_data)
            
            if not dcf_results:
                logger.warning(f"DCF calculation failed for {symbol}")
                return False
            
            # Define fields to check
            fair_value_fields = [
                'fair_value_mean', 'fair_value_median', 'fair_value_std',
                'fair_value_p5', 'fair_value_p25', 'fair_value_p75', 'fair_value_p95'
            ]
            
            # Sanity check for extreme values (indicates data quality issues)
            median_value = dcf_results['fair_value_median']
            if median_value > 100000 or median_value <= 0:  # > 100K SEK per share or negative
                logger.warning(f"Extreme valuation for {symbol}: {median_value:.0f} - skipping")
                return False
            
            # Check for NaN or infinite values
            for field in fair_value_fields:
                value = dcf_results.get(field)
                if value is not None and (value != value or abs(value) == float('inf')):  # NaN or inf check
                    logger.warning(f"Invalid numerical values for {symbol} - skipping")
                    return False
            
            # Calculate confidence
            valuation_confidence = self.calculate_valuation_confidence(dcf_results, data_quality_score)
            
            # Get exchange rate
            exchange_rate = self.dcf_engine.get_exchange_rate(
                company_data.report_currency, 
                company_data.stock_currency
            )
            
            # Calculate values in stock currency (reuse fair_value_fields from above)
            
            stock_values = {}
            for field in fair_value_fields:
                stock_field = field.replace('fair_value_', 'fair_value_stock_')
                stock_values[stock_field] = dcf_results[field] * exchange_rate
            
            computation_time = int((time.time() - start_time) * 1000)
            
            # Store in database
            insert_query = """
            INSERT INTO dcf_valuations (
                symbol, report_date, publish_date, report_type, model_version,
                simulation_count, projection_years, risk_free_rate, market_premium, terminal_growth,
                fair_value_mean, fair_value_median, fair_value_std,
                fair_value_p5, fair_value_p25, fair_value_p75, fair_value_p95,
                report_currency, stock_currency, exchange_rate,
                fair_value_stock_mean, fair_value_stock_median, fair_value_stock_std,
                fair_value_stock_p5, fair_value_stock_p25, fair_value_stock_p75, fair_value_stock_p95,
                shares_outstanding, latest_revenue, latest_operating_income, latest_free_cash_flow,
                operating_margin, effective_tax_rate, wacc,
                data_quality_score, periods_used, estimation_flags, valuation_confidence,
                computation_time_ms
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27,
                $28, $29, $30, $31, $32, $33, $34,
                $35, $36, $37, $38, $39
            )
            ON CONFLICT (symbol, report_date, model_version) DO UPDATE SET
                computation_date = CURRENT_TIMESTAMP,
                fair_value_median = EXCLUDED.fair_value_median,
                fair_value_stock_median = EXCLUDED.fair_value_stock_median,
                valuation_confidence = EXCLUDED.valuation_confidence,
                computation_time_ms = EXCLUDED.computation_time_ms
            """
            
            await self.db_conn.execute(
                insert_query,
                symbol, report_date, publish_date, report['report_type'], self.model_version,
                self.dcf_config.num_simulations, self.dcf_config.projection_years,
                self.dcf_config.risk_free_rate, self.dcf_config.market_premium, self.dcf_config.terminal_growth,
                dcf_results['fair_value_mean'], dcf_results['fair_value_median'], dcf_results['fair_value_std'],
                dcf_results['fair_value_p5'], dcf_results['fair_value_p25'], dcf_results['fair_value_p75'], dcf_results['fair_value_p95'],
                company_data.report_currency, company_data.stock_currency, exchange_rate,
                stock_values['fair_value_stock_mean'], stock_values['fair_value_stock_median'], stock_values['fair_value_stock_std'],
                stock_values['fair_value_stock_p5'], stock_values['fair_value_stock_p25'], stock_values['fair_value_stock_p75'], stock_values['fair_value_stock_p95'],
                company_data.shares_outstanding,
                company_data.revenues[0] if company_data.revenues else None,
                company_data.operating_incomes[0] if company_data.operating_incomes else None,
                company_data.free_cash_flows[0] if company_data.free_cash_flows else None,
                company_data.operating_margins[0] if company_data.operating_margins else None,
                company_data.effective_tax_rates[0] if company_data.effective_tax_rates else None,
                dcf_results['wacc'],
                data_quality_score, periods_used, estimation_flags, valuation_confidence,
                computation_time
            )
            
            logger.info(f"✅ {symbol}: Fair Value {stock_values['fair_value_stock_median']:.0f} {company_data.stock_currency} "
                       f"(Confidence: {valuation_confidence:.1%}, Time: {computation_time}ms)")
            
            self.processed_count += 1
            return True
        
        except Exception as e:
            logger.error(f"❌ Error processing {symbol} {report_date}: {e}")
            self.failed_count += 1
            return False
    
    async def process_batch(self, limit: int = 50):
        """Process a batch of unprocessed reports."""
        
        reports = await self.get_unprocessed_reports(limit)
        
        if not reports:
            logger.info("✅ No unprocessed reports found")
            return
        
        logger.info(f"🚀 Processing {len(reports)} financial reports")
        
        success_count = 0
        
        for i, report in enumerate(reports, 1):
            logger.info(f"Progress: {i}/{len(reports)}")
            
            success = await self.process_report(report)
            if success:
                success_count += 1
            
            # Progress update every 10 reports
            if i % 10 == 0:
                elapsed = time.time() - self.start_time
                rate = i / elapsed
                eta = (len(reports) - i) / rate if rate > 0 else 0
                logger.info(f"📊 Progress: {i}/{len(reports)} ({success_count} successful, "
                           f"Rate: {rate:.1f}/min, ETA: {eta/60:.1f}min)")
        
        # Final summary
        elapsed = time.time() - self.start_time
        logger.info(f"🏁 Batch complete: {success_count}/{len(reports)} successful in {elapsed:.0f}s")
    
    async def get_processing_stats(self):
        """Get processing statistics."""
        
        stats_query = """
        SELECT 
            model_version,
            COUNT(*) as total_valuations,
            COUNT(DISTINCT symbol) as companies_covered,
            MIN(publish_date) as earliest_report,
            MAX(publish_date) as latest_report,
            AVG(valuation_confidence) as avg_confidence,
            AVG(data_quality_score) as avg_data_quality,
            AVG(computation_time_ms) as avg_computation_time
        FROM dcf_valuations
        WHERE model_version = $1
        GROUP BY model_version
        """
        
        stats = await self.db_conn.fetchrow(stats_query, self.model_version)
        
        if stats:
            logger.info(f"📊 Processing Statistics for {self.model_version}:")
            logger.info(f"   Total Valuations: {stats['total_valuations']:,}")
            logger.info(f"   Companies Covered: {stats['companies_covered']}")
            logger.info(f"   Date Range: {stats['earliest_report']} to {stats['latest_report']}")
            logger.info(f"   Avg Confidence: {stats['avg_confidence']:.1%}")
            logger.info(f"   Avg Data Quality: {stats['avg_data_quality']:.1%}")
            logger.info(f"   Avg Computation Time: {stats['avg_computation_time']:.0f}ms")
        else:
            logger.info("No processing statistics available")

async def main():
    """Process financial reports and generate DCF valuations."""
    
    processor = DCFReportProcessor()
    await processor.setup()
    
    try:
        # Show current stats
        await processor.get_processing_stats()
        
        # Process batch of reports
        await processor.process_batch(limit=20)  # Start with 20 for testing
        
        # Show updated stats
        await processor.get_processing_stats()
    
    finally:
        await processor.cleanup()

if __name__ == "__main__":
    asyncio.run(main())