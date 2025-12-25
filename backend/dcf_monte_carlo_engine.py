#!/usr/bin/env python3
"""
DCF Monte Carlo Valuation Engine

Performs discounted cash flow analysis with Monte Carlo simulation
to generate fair value ranges for companies based on their fundamentals.

Key features:
- Projects future cash flows using historical growth patterns
- Runs thousands of scenarios with parameter uncertainty
- Calculates probability distributions of fair values
- Stores results for backtesting and analysis
"""

import asyncio
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DCFParameters:
    """Parameters for DCF Monte Carlo simulation"""
    # Historical data requirements
    min_history_years: int = 3
    
    # Projection settings
    projection_years: int = 10
    terminal_growth_rate: float = 0.025  # 2.5% perpetual growth
    
    # Monte Carlo settings
    num_simulations: int = 10000
    
    # Market assumptions
    risk_free_rate: float = 0.04  # 4% risk-free rate
    market_premium: float = 0.08  # 8% equity risk premium
    
    # Constraints
    max_revenue_growth: float = 0.50  # 50% max annual growth
    min_revenue_growth: float = -0.30  # -30% min growth
    max_operating_margin: float = 0.40  # 40% max margin
    min_operating_margin: float = -0.10  # -10% min margin

@dataclass
class CompanyMetrics:
    """Historical metrics for a company"""
    symbol: str
    revenues: List[float]
    operating_margins: List[float]
    capex_ratios: List[float]
    nwc_ratios: List[float]
    tax_rates: List[float]
    shares_outstanding: float
    net_debt: float
    beta: float = 1.0  # Default market beta

class DCFMonteCarloEngine:
    
    def __init__(self, params: DCFParameters):
        self.params = params
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection"""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def create_tables(self):
        """Create the DCF valuation tables"""
        create_dcf_valuations = """
        CREATE TABLE IF NOT EXISTS dcf_valuations (
            id BIGSERIAL PRIMARY KEY,
            
            -- Linking to source data
            company_id INTEGER NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            financial_statement_id BIGINT,
            valuation_date DATE NOT NULL,
            report_date DATE NOT NULL,
            
            -- Monte Carlo results
            simulations_run INTEGER DEFAULT 10000,
            fair_value_mean DECIMAL(15,2),
            fair_value_median DECIMAL(15,2),
            fair_value_std DECIMAL(15,2),
            
            -- Percentile ranges
            fair_value_p5 DECIMAL(15,2),
            fair_value_p25 DECIMAL(15,2),
            fair_value_p50 DECIMAL(15,2),
            fair_value_p75 DECIMAL(15,2),
            fair_value_p95 DECIMAL(15,2),
            
            -- Current market comparison
            market_price DECIMAL(15,2),
            implied_return DECIMAL(10,4),
            valuation_signal VARCHAR(20),
            
            -- Model metadata
            model_version VARCHAR(20) DEFAULT 'v1.0',
            discount_rate DECIMAL(8,4),
            terminal_growth_rate DECIMAL(8,4),
            projection_years INTEGER DEFAULT 10,
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_dcf_company_date 
        ON dcf_valuations(company_id, valuation_date);
        
        CREATE INDEX IF NOT EXISTS idx_dcf_signal 
        ON dcf_valuations(valuation_signal, implied_return);
        """
        
        create_model_params = """
        CREATE TABLE IF NOT EXISTS dcf_model_parameters (
            id BIGSERIAL PRIMARY KEY,
            dcf_valuation_id BIGINT REFERENCES dcf_valuations(id) ON DELETE CASCADE,
            
            -- Growth assumptions
            revenue_growth_mean DECIMAL(10,4),
            revenue_growth_std DECIMAL(10,4),
            revenue_growth_min DECIMAL(10,4),
            revenue_growth_max DECIMAL(10,4),
            
            -- Margin assumptions
            operating_margin_mean DECIMAL(10,4),
            operating_margin_std DECIMAL(10,4),
            operating_margin_trend DECIMAL(10,4),
            
            -- Capital efficiency
            capex_to_revenue_mean DECIMAL(10,4),
            capex_to_revenue_std DECIMAL(10,4),
            nwc_to_revenue_mean DECIMAL(10,4),
            nwc_to_revenue_std DECIMAL(10,4),
            
            -- Tax assumptions
            tax_rate_mean DECIMAL(10,4),
            tax_rate_std DECIMAL(10,4),
            
            -- Risk parameters
            company_beta DECIMAL(8,4),
            debt_to_equity DECIMAL(10,4),
            credit_spread DECIMAL(8,4),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_backtest_table = """
        CREATE TABLE IF NOT EXISTS dcf_backtest_results (
            id BIGSERIAL PRIMARY KEY,
            dcf_valuation_id BIGINT REFERENCES dcf_valuations(id),
            
            horizon_days INTEGER,
            actual_price DECIMAL(15,2),
            predicted_price DECIMAL(15,2),
            absolute_error DECIMAL(10,4),
            percentage_error DECIMAL(10,4),
            within_confidence_interval BOOLEAN,
            
            evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_backtest_accuracy 
        ON dcf_backtest_results(horizon_days, percentage_error);
        """
        
        await self.db_conn.execute(create_dcf_valuations)
        await self.db_conn.execute(create_model_params)
        await self.db_conn.execute(create_backtest_table)
        
        logger.info("DCF valuation tables created successfully")
        
    async def get_company_fundamentals(self, symbol: str, report_date: datetime) -> Optional[CompanyMetrics]:
        """Extract historical fundamentals for DCF modeling"""
        
        # Get historical financial data
        query = """
        WITH yearly_data AS (
            SELECT 
                symbol,
                date,
                revenue_per_share,
                book_value_per_share,
                cash_per_share,
                debt_to_equity,
                market_cap,
                -- Approximate total metrics from per-share values
                revenue_per_share * 100000000 as revenue,  -- Assume 100M shares as default
                CASE 
                    WHEN revenue_per_share > 0 
                    THEN (revenue_per_share - 0.7 * revenue_per_share) / revenue_per_share 
                    ELSE 0.1 
                END as approx_margin,
                100000000 as shares_outstanding,  -- Default assumption
                ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM date) ORDER BY date DESC) as rn
            FROM historical_fundamentals_daily
            WHERE symbol = $1
            AND date <= $2
            AND date >= $2 - INTERVAL '5 years'
            AND revenue_per_share > 0
            AND book_value_per_share > 0
        )
        SELECT *
        FROM yearly_data
        WHERE rn = 1
        ORDER BY date DESC
        """
        
        records = await self.db_conn.fetch(query, symbol, report_date.date())
        
        if len(records) < 3:  # Need at least 3 years
            return None
            
        # Extract metrics
        revenues = []
        margins = []
        capex_ratios = []
        nwc_ratios = []
        tax_rates = []
        
        for i in range(len(records) - 1):
            current = records[i]
            prior = records[i + 1]
            
            # Revenue
            if current['revenue'] and prior['revenue']:
                revenues.append(float(current['revenue']))
                
            # Operating margin (approximated)
            if current['approx_margin']:
                margins.append(float(current['approx_margin']))
                
            # CapEx approximation (simplified as percentage of revenue)
            capex_ratios.append(0.05)  # 5% default capex
                
        # Fill in defaults if needed
        if not margins:
            margins = [0.10]  # 10% default margin
        if not capex_ratios:
            capex_ratios = [0.05]  # 5% default capex
        if not nwc_ratios:
            nwc_ratios = [0.10]  # 10% default NWC
        if not tax_rates:
            tax_rates = [0.25]  # 25% default tax
            
        latest_record = records[0]
        
        return CompanyMetrics(
            symbol=symbol,
            revenues=revenues,
            operating_margins=margins,
            capex_ratios=capex_ratios,
            nwc_ratios=nwc_ratios,
            tax_rates=tax_rates,
            shares_outstanding=float(latest_record['shares_outstanding'] or 100_000_000),
            net_debt=float(latest_record['debt_to_equity'] or 0) * float(latest_record['book_value_per_share'] or 100) * float(latest_record['shares_outstanding'] or 100_000_000),
            beta=1.0  # Default, could calculate from price data
        )
        
    def calculate_growth_parameters(self, historical_values: List[float]) -> Dict[str, float]:
        """Calculate growth rate distribution from historical data"""
        
        if len(historical_values) < 2:
            return {'mean': 0.05, 'std': 0.10, 'min': -0.10, 'max': 0.20}
            
        # Calculate year-over-year growth rates
        growth_rates = []
        for i in range(1, len(historical_values)):
            if historical_values[i-1] > 0:
                growth = (historical_values[i] - historical_values[i-1]) / historical_values[i-1]
                growth_rates.append(growth)
                
        if not growth_rates:
            return {'mean': 0.05, 'std': 0.10, 'min': -0.10, 'max': 0.20}
            
        mean_growth = np.mean(growth_rates)
        std_growth = np.std(growth_rates) if len(growth_rates) > 1 else 0.15
        
        # Add uncertainty to historical volatility
        std_growth = max(0.10, std_growth * 1.2)
        
        return {
            'mean': np.clip(mean_growth, -0.20, 0.30),
            'std': std_growth,
            'min': max(self.params.min_revenue_growth, mean_growth - 2 * std_growth),
            'max': min(self.params.max_revenue_growth, mean_growth + 2 * std_growth)
        }
        
    def calculate_wacc(self, beta: float, debt_to_equity: float, tax_rate: float) -> float:
        """Calculate weighted average cost of capital"""
        
        # Cost of equity (CAPM)
        cost_of_equity = self.params.risk_free_rate + beta * self.params.market_premium
        
        # Cost of debt (simplified)
        cost_of_debt = self.params.risk_free_rate + 0.02  # 2% credit spread
        
        # Weights
        equity_weight = 1 / (1 + debt_to_equity)
        debt_weight = debt_to_equity / (1 + debt_to_equity)
        
        # WACC
        wacc = (equity_weight * cost_of_equity + 
                debt_weight * cost_of_debt * (1 - tax_rate))
        
        return wacc
        
    def run_single_simulation(self, metrics: CompanyMetrics, growth_params: Dict, 
                            margin_params: Dict, wacc: float) -> float:
        """Run a single Monte Carlo simulation"""
        
        # Starting values
        base_revenue = metrics.revenues[0] if metrics.revenues else 1000
        cash_flows = []
        
        # Project cash flows
        revenue = base_revenue
        for year in range(self.params.projection_years):
            # Sample growth rate
            growth_rate = np.random.normal(growth_params['mean'], growth_params['std'])
            growth_rate = np.clip(growth_rate, growth_params['min'], growth_params['max'])
            
            # Decay growth towards terminal rate
            decay_factor = 1 - (year / self.params.projection_years) * 0.5
            growth_rate = growth_rate * decay_factor + self.params.terminal_growth_rate * (1 - decay_factor)
            
            # Project revenue
            revenue = revenue * (1 + growth_rate)
            
            # Sample operating margin
            margin = np.random.normal(margin_params['mean'], margin_params['std'])
            margin = np.clip(margin, self.params.min_operating_margin, self.params.max_operating_margin)
            
            # Calculate EBIT
            ebit = revenue * margin
            
            # Sample tax rate
            tax_rate = np.random.normal(metrics.tax_rates[0], 0.02)
            tax_rate = np.clip(tax_rate, 0.15, 0.35)
            
            # After-tax operating income
            nopat = ebit * (1 - tax_rate)
            
            # Less: CapEx
            capex_rate = np.random.normal(metrics.capex_ratios[0], 0.02)
            capex = revenue * np.clip(capex_rate, 0.01, 0.15)
            
            # Less: Change in NWC
            nwc_rate = np.random.normal(metrics.nwc_ratios[0], 0.02)
            delta_nwc = revenue * growth_rate * np.clip(nwc_rate, 0, 0.20)
            
            # Free cash flow
            fcf = nopat - capex - delta_nwc
            cash_flows.append(fcf)
            
        # Terminal value
        terminal_fcf = cash_flows[-1] * (1 + self.params.terminal_growth_rate)
        terminal_value = terminal_fcf / (wacc - self.params.terminal_growth_rate)
        
        # Discount all cash flows
        pv_cash_flows = sum(cf / (1 + wacc)**i for i, cf in enumerate(cash_flows, 1))
        pv_terminal_value = terminal_value / (1 + wacc)**self.params.projection_years
        
        # Enterprise value
        enterprise_value = pv_cash_flows + pv_terminal_value
        
        # Equity value
        equity_value = enterprise_value - metrics.net_debt
        
        # Per share value
        fair_value_per_share = equity_value / metrics.shares_outstanding
        
        return max(0, fair_value_per_share)  # Can't be negative
        
    async def value_company(self, symbol: str, valuation_date: datetime, 
                           market_price: float) -> Optional[Dict]:
        """Run full DCF Monte Carlo valuation for a company"""
        
        # Get historical data
        metrics = await self.get_company_fundamentals(symbol, valuation_date)
        if not metrics:
            logger.warning(f"Insufficient data for {symbol}")
            return None
            
        # Calculate parameters
        growth_params = self.calculate_growth_parameters(metrics.revenues)
        margin_params = {
            'mean': np.mean(metrics.operating_margins),
            'std': np.std(metrics.operating_margins) if len(metrics.operating_margins) > 1 else 0.05
        }
        
        # Calculate WACC
        wacc = self.calculate_wacc(
            metrics.beta, 
            metrics.net_debt / (market_price * metrics.shares_outstanding),
            metrics.tax_rates[0]
        )
        
        # Run Monte Carlo simulation
        fair_values = []
        for _ in range(self.params.num_simulations):
            fv = self.run_single_simulation(metrics, growth_params, margin_params, wacc)
            fair_values.append(fv)
            
        # Calculate statistics
        fair_values = np.array(fair_values)
        
        results = {
            'symbol': symbol,
            'valuation_date': valuation_date,
            'market_price': market_price,
            'fair_value_mean': np.mean(fair_values),
            'fair_value_median': np.median(fair_values),
            'fair_value_std': np.std(fair_values),
            'fair_value_p5': np.percentile(fair_values, 5),
            'fair_value_p25': np.percentile(fair_values, 25),
            'fair_value_p50': np.percentile(fair_values, 50),
            'fair_value_p75': np.percentile(fair_values, 75),
            'fair_value_p95': np.percentile(fair_values, 95),
            'implied_return': (np.median(fair_values) / market_price) - 1,
            'wacc': wacc,
            'growth_params': growth_params,
            'margin_params': margin_params
        }
        
        # Valuation signal
        if results['implied_return'] > 0.30:
            results['valuation_signal'] = 'STRONGLY_UNDERVALUED'
        elif results['implied_return'] > 0.15:
            results['valuation_signal'] = 'UNDERVALUED'
        elif results['implied_return'] < -0.15:
            results['valuation_signal'] = 'OVERVALUED'
        else:
            results['valuation_signal'] = 'FAIR_VALUE'
            
        return results
        
    async def save_valuation(self, results: Dict) -> int:
        """Save valuation results to database"""
        
        # Get company ID from company_master table
        company_id_query = """
        SELECT id FROM company_master WHERE symbol = $1
        """
        company_record = await self.db_conn.fetchrow(company_id_query, results['symbol'])
        company_id = company_record['id'] if company_record else hash(results['symbol']) % 1000000
        
        # Insert valuation
        insert_query = """
        INSERT INTO dcf_valuations (
            company_id, symbol, valuation_date, report_date,
            simulations_run, fair_value_mean, fair_value_median, fair_value_std,
            fair_value_p5, fair_value_p25, fair_value_p50, fair_value_p75, fair_value_p95,
            market_price, implied_return, valuation_signal,
            discount_rate, terminal_growth_rate, projection_years
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        ) RETURNING id
        """
        
        dcf_id = await self.db_conn.fetchval(
            insert_query,
            company_id, results['symbol'], results['valuation_date'], results['valuation_date'],
            self.params.num_simulations, results['fair_value_mean'], results['fair_value_median'],
            results['fair_value_std'], results['fair_value_p5'], results['fair_value_p25'],
            results['fair_value_p50'], results['fair_value_p75'], results['fair_value_p95'],
            results['market_price'], results['implied_return'], results['valuation_signal'],
            results['wacc'], self.params.terminal_growth_rate, self.params.projection_years
        )
        
        # Save model parameters
        params_query = """
        INSERT INTO dcf_model_parameters (
            dcf_valuation_id,
            revenue_growth_mean, revenue_growth_std, revenue_growth_min, revenue_growth_max,
            operating_margin_mean, operating_margin_std,
            company_beta, debt_to_equity
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9
        )
        """
        
        await self.db_conn.execute(
            params_query,
            dcf_id,
            results['growth_params']['mean'], results['growth_params']['std'],
            results['growth_params']['min'], results['growth_params']['max'],
            results['margin_params']['mean'], results['margin_params']['std'],
            1.0, 0.5  # Default values for now
        )
        
        return dcf_id
        
    async def run_batch_valuations(self, start_date: datetime, end_date: datetime):
        """Run valuations for all companies with sufficient data"""
        
        # Get companies to value
        companies_query = """
        SELECT 
            hfd.symbol,
            AVG(hfd.close_price) as avg_price,
            AVG(hfd.market_cap) as avg_market_cap
        FROM historical_fundamentals_daily hfd
        WHERE hfd.date BETWEEN $1 AND $2
        AND hfd.revenue_per_share > 0
        AND hfd.close_price > 5
        GROUP BY hfd.symbol
        HAVING COUNT(*) > 100
        ORDER BY AVG(hfd.market_cap) DESC
        LIMIT 50
        """
        
        companies = await self.db_conn.fetch(companies_query, start_date.date(), end_date.date())
        
        print(f"🎯 Running DCF valuations for {len(companies)} companies")
        
        successful = 0
        for i, company in enumerate(companies):
            if i % 5 == 0:
                print(f"   📊 Progress: {i}/{len(companies)}")
                
            try:
                results = await self.value_company(
                    company['symbol'], 
                    end_date,
                    float(company['avg_price'])
                )
                
                if results:
                    await self.save_valuation(results)
                    successful += 1
                    
                    print(f"   ✓ {company['symbol']}: Fair value ${results['fair_value_median']:.2f} "
                          f"vs Market ${results['market_price']:.2f} "
                          f"({results['implied_return']:+.1%} return)")
                    
            except Exception as e:
                logger.error(f"Error valuing {company['symbol']}: {e}")
                
        print(f"\n✅ Completed {successful}/{len(companies)} valuations")
        
    async def cleanup(self):
        """Close database connection"""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run DCF Monte Carlo valuation engine"""
    
    params = DCFParameters(
        num_simulations=10000,
        projection_years=10,
        terminal_growth_rate=0.025,
        risk_free_rate=0.04,
        market_premium=0.08
    )
    
    engine = DCFMonteCarloEngine(params)
    
    try:
        await engine.setup()
        
        # Create tables
        await engine.create_tables()
        
        # Run valuations for recent period
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        await engine.run_batch_valuations(start_date, end_date)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(main())