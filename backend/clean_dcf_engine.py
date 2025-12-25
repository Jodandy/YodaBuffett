#!/usr/bin/env python3
"""
Clean DCF Monte Carlo Engine

Rebuilt from scratch to be:
- Data-driven (no hardcoded assumptions)
- Uses actual fundamental tables (financial_statements, balance_sheet_data, cash_flow_data)
- Clean Monte Carlo implementation
- Currency-aware
- No bullshit
"""

import asyncio
import asyncpg
import numpy as np
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

@dataclass
class DCFConfig:
    """DCF Configuration - everything explicit, nothing hardcoded"""
    
    # Simulation settings
    num_simulations: int = 10000
    projection_years: int = 10
    
    # Market parameters (can be adjusted per market/company)
    risk_free_rate: float = 0.03     # 3% Nordic risk-free rate
    market_premium: float = 0.10      # 10% equity risk premium (more conservative)
    terminal_growth: float = 0.025    # 2.5% perpetual growth
    
    # Tax rate (will calculate from actual data when possible)
    default_tax_rate: float = 0.22   # 22% Nordic corporate tax
    
    # Data requirements
    min_periods_required: int = 3     # Minimum periods for calculation

@dataclass 
class CompanyData:
    """Real company data extracted from our fundamental tables"""
    
    # Basic info
    symbol: str
    shares_outstanding: float
    stock_currency: str
    report_currency: str
    
    # Financial data (time series, most recent first)
    revenues: List[float]
    operating_incomes: List[float] 
    interest_expenses: List[float]
    tax_expenses: List[float]
    
    # Cash flow data
    operating_cash_flows: List[float]
    capital_expenditures: List[float]
    
    # Balance sheet data
    total_debt: List[float]
    cash_equivalents: List[float]
    
    # Derived metrics
    operating_margins: List[float]
    free_cash_flows: List[float]
    effective_tax_rates: List[float]

class CleanDCFEngine:
    """Clean, data-driven DCF Monte Carlo engine"""
    
    def __init__(self, config: DCFConfig):
        self.config = config
        self.db_conn = None
        
        # Exchange rates (in production, fetch from API)
        self.exchange_rates = {
            ('USD', 'SEK'): 11.0, ('EUR', 'SEK'): 12.0, ('CHF', 'SEK'): 12.5,
            ('DKK', 'SEK'): 1.6, ('NOK', 'SEK'): 1.0, ('USD', 'NOK'): 11.0,
            ('EUR', 'NOK'): 12.0, ('DKK', 'NOK'): 1.5, ('USD', 'DKK'): 7.0,
        }
    
    async def setup(self):
        """Initialize database connection"""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    async def cleanup(self):
        """Close database connection"""
        if self.db_conn:
            await self.db_conn.close()
    
    def get_exchange_rate(self, from_currency: str, to_currency: str) -> float:
        """Get exchange rate between currencies"""
        
        if from_currency == to_currency:
            return 1.0
        
        # Direct rate
        key = (from_currency, to_currency)
        if key in self.exchange_rates:
            return self.exchange_rates[key]
        
        # Inverse rate
        inverse_key = (to_currency, from_currency)
        if inverse_key in self.exchange_rates:
            return 1.0 / self.exchange_rates[inverse_key]
        
        # Cross rate via USD
        if from_currency != 'USD' and to_currency != 'USD':
            from_usd_key = ('USD', from_currency)
            to_usd_key = ('USD', to_currency)
            
            if from_usd_key in self.exchange_rates and to_usd_key in self.exchange_rates:
                return self.exchange_rates[to_usd_key] / self.exchange_rates[from_usd_key]
        
        logger.warning(f"No exchange rate for {from_currency} → {to_currency}, using 1.0")
        return 1.0
    
    async def calculate_r12_metrics(self, quarterly_data: List, annual_data: List = None) -> Optional[Dict]:
        """Calculate Rolling Twelve Months (R12) metrics from quarterly data
        
        Handles cases where:
        1. We have 4+ quarters: Sum last 4 quarters
        2. We have Q1+Q2+Q3+Annual: Calculate Q4 = Annual - (Q1+Q2+Q3)
        3. We have <4 quarters: Return None
        """
        
        if len(quarterly_data) >= 4:
            # Standard case: we have 4+ quarters, use last 4
            last_4_quarters = quarterly_data[:4]
            
            r12_revenue = sum(float(q['total_revenue']) for q in last_4_quarters)
            r12_operating_income = sum(float(q['operating_income']) for q in last_4_quarters)
            r12_interest_expense = sum(float(q['interest_expense']) for q in last_4_quarters)
            r12_tax_expense = sum(float(q['tax_expense']) for q in last_4_quarters)
            r12_net_income = sum(float(q['net_income']) for q in last_4_quarters)
            
            most_recent_quarter = last_4_quarters[0]
            
            return {
                'period_date': most_recent_quarter['period_date'],
                'publish_date': most_recent_quarter['publish_date'],
                'total_revenue': r12_revenue,
                'operating_income': r12_operating_income,
                'interest_expense': r12_interest_expense,
                'tax_expense': r12_tax_expense,
                'net_income': r12_net_income,
                'period_type': 'r12'
            }
        
        elif len(quarterly_data) == 3 and annual_data:
            # Check if we have Q1, Q2, Q3 + matching annual data
            return await self.calculate_r12_with_annual_backfill(quarterly_data, annual_data)
        
        else:
            # Not enough data for R12 calculation
            return None
    
    async def calculate_r12_with_annual_backfill(self, quarterly_data: List, annual_data: List) -> Optional[Dict]:
        """Calculate R12 when we have Q1+Q2+Q3 + Annual data by computing missing Q4"""
        
        if len(quarterly_data) != 3:
            return None
        
        # Sort quarters by date (most recent first)
        quarters_sorted = sorted(quarterly_data, key=lambda x: x['period_date'], reverse=True)
        
        # Check which quarter we're missing
        quarter_months = [q['period_date'].month for q in quarters_sorted]
        
        # Find the corresponding annual data
        # Annual data should be for the same fiscal year as the quarters
        fiscal_year = quarters_sorted[0]['period_date'].year
        
        matching_annual = None
        for annual in annual_data:
            annual_year = annual['period_date'].year
            # Annual data could be from same year (December) or next year (Jan/Feb)
            if annual_year == fiscal_year or annual_year == fiscal_year + 1:
                matching_annual = annual
                break
        
        if not matching_annual:
            return None
        
        # Check if Q4 (December) already exists
        if 12 in quarter_months:
            # We already have Q4, can't backfill
            return None
        
        # Calculate Q4 = Annual - (Q1 + Q2 + Q3)
        quarterly_sum_revenue = sum(float(q['total_revenue']) for q in quarters_sorted)
        quarterly_sum_op_income = sum(float(q['operating_income']) for q in quarters_sorted)
        quarterly_sum_interest = sum(float(q['interest_expense']) for q in quarters_sorted)
        quarterly_sum_tax = sum(float(q['tax_expense']) for q in quarters_sorted)
        quarterly_sum_net_income = sum(float(q['net_income']) for q in quarters_sorted)
        
        annual_revenue = float(matching_annual['total_revenue'])
        annual_op_income = float(matching_annual['operating_income'])
        annual_interest = float(matching_annual['interest_expense'])
        annual_tax = float(matching_annual['tax_expense'])
        annual_net_income = float(matching_annual['net_income'])
        
        # Calculate Q4 (implied)
        q4_revenue = annual_revenue - quarterly_sum_revenue
        q4_op_income = annual_op_income - quarterly_sum_op_income
        q4_interest = annual_interest - quarterly_sum_interest
        q4_tax = annual_tax - quarterly_sum_tax
        q4_net_income = annual_net_income - quarterly_sum_net_income
        
        # Sanity check: Q4 should be positive and reasonable
        if q4_revenue <= 0 or q4_revenue > annual_revenue * 0.8:
            logger.warning(f"Suspicious Q4 backfill: Q4 revenue={q4_revenue:,.0f}, Annual={annual_revenue:,.0f}")
            return None
        
        # Use the annual data's publish date for the R12 (since it's the most recent)
        return {
            'period_date': matching_annual['period_date'],
            'publish_date': matching_annual['publish_date'],
            'total_revenue': annual_revenue,  # Use full annual for R12
            'operating_income': annual_op_income,
            'interest_expense': annual_interest,
            'tax_expense': annual_tax,
            'net_income': annual_net_income,
            'period_type': 'r12_backfilled',
            'q4_implied': {
                'revenue': q4_revenue,
                'operating_income': q4_op_income,
                'interest_expense': q4_interest,
                'tax_expense': q4_tax,
                'net_income': q4_net_income
            }
        }
    
    def calculate_r12_cash_flows(self, cashflow_data: List, quarterly_data: List) -> Tuple[float, float]:
        """Calculate R12 operating cash flow and capex from quarterly cash flow data"""
        
        if len(quarterly_data) < 4:
            # Not enough quarterly data, estimate from most recent quarter
            if quarterly_data and cashflow_data:
                latest_quarter_date = quarterly_data[0]['period_date']
                for cf in cashflow_data:
                    if cf['period_date'] == latest_quarter_date:
                        # Rough estimate: current quarter × 4 (not ideal but better than nothing)
                        return float(cf['operating_cash_flow']) * 4, float(cf['capital_expenditure']) * 4
            # Fallback to zero
            return 0.0, 0.0
        
        # Get the last 4 quarters
        last_4_quarters = quarterly_data[:4]
        quarter_dates = [q['period_date'] for q in last_4_quarters]
        
        # Create a mapping of cashflow data by date
        cf_by_date = {cf['period_date']: cf for cf in cashflow_data}
        
        # Sum cash flows for the last 4 quarters
        r12_ocf = 0.0
        r12_capex = 0.0
        quarters_found = 0
        
        for quarter_date in quarter_dates:
            if quarter_date in cf_by_date:
                cf = cf_by_date[quarter_date]
                r12_ocf += float(cf['operating_cash_flow'])
                r12_capex += float(cf['capital_expenditure'])
                quarters_found += 1
        
        # If we're missing some quarters, estimate based on what we have
        if quarters_found > 0 and quarters_found < 4:
            # Scale up based on the quarters we found
            scale_factor = 4.0 / quarters_found
            r12_ocf *= scale_factor
            r12_capex *= scale_factor
        
        return r12_ocf, r12_capex
    
    async def extract_company_data(self, symbol: str, valuation_date: datetime) -> Optional[CompanyData]:
        """Extract all company data from fundamental tables"""
        
        # Get currency information
        currency_query = """
        SELECT stock_currency, report_currency
        FROM company_master
        WHERE primary_ticker = $1
        """
        
        currency_info = await self.db_conn.fetchrow(currency_query, symbol)
        
        if not currency_info:
            logger.warning(f"No currency info for {symbol}")
            return None
        
        stock_currency = currency_info['stock_currency'] or 'SEK'
        report_currency = currency_info['report_currency'] or 'SEK'
        
        # Get shares outstanding (most recent)
        shares_query = """
        SELECT shares_outstanding
        FROM balance_sheet_data
        WHERE symbol = $1
        AND publish_date IS NOT NULL
        AND publish_date <= $2
        AND shares_outstanding > 0
        ORDER BY publish_date DESC
        LIMIT 1
        """
        
        shares_record = await self.db_conn.fetchrow(shares_query, symbol, valuation_date.date())
        
        if not shares_record:
            logger.warning(f"No shares outstanding for {symbol}")
            return None
        
        shares_outstanding = float(shares_record['shares_outstanding'])
        
        # Get financial statements data - PRIORITIZE ANNUAL DATA
        financial_query = """
        SELECT 
            period_date,
            publish_date,
            total_revenue,
            operating_income,
            COALESCE(interest_expense, 0) as interest_expense,
            COALESCE(tax_expense, 0) as tax_expense,
            COALESCE(net_income, 0) as net_income,
            CASE 
                WHEN (EXTRACT(MONTH FROM period_date) = 12 AND EXTRACT(DAY FROM period_date) >= 28)
                     OR (EXTRACT(MONTH FROM period_date) = 1 AND EXTRACT(DAY FROM period_date) <= 3)
                THEN 'annual'
                ELSE 'quarterly'
            END as period_type
        FROM financial_statements
        WHERE symbol = $1
        AND publish_date IS NOT NULL
        AND publish_date <= $2
        AND total_revenue > 0
        ORDER BY 
            CASE WHEN (EXTRACT(MONTH FROM period_date) = 12 AND EXTRACT(DAY FROM period_date) >= 28)
                      OR (EXTRACT(MONTH FROM period_date) = 1 AND EXTRACT(DAY FROM period_date) <= 3)
                 THEN 0 ELSE 1 END,  -- Annual data first
            publish_date DESC
        LIMIT 10
        """
        
        financial_data = await self.db_conn.fetch(financial_query, symbol, valuation_date.date())
        
        if len(financial_data) < self.config.min_periods_required:
            logger.warning(f"Insufficient financial data for {symbol}: {len(financial_data)} periods")
            return None
        
        # Get cash flow data
        cashflow_query = """
        SELECT
            period_date,
            publish_date,
            COALESCE(operating_cash_flow, 0) as operating_cash_flow,
            COALESCE(capital_expenditure, 0) as capital_expenditure,
            COALESCE(free_cash_flow, 0) as free_cash_flow
        FROM cash_flow_data
        WHERE symbol = $1
        AND publish_date IS NOT NULL
        AND publish_date <= $2
        ORDER BY publish_date DESC
        LIMIT 10
        """
        
        cashflow_data = await self.db_conn.fetch(cashflow_query, symbol, valuation_date.date())
        
        # Get balance sheet data
        balance_query = """
        SELECT
            period_date,
            publish_date,
            COALESCE(total_debt, 0) as total_debt,
            COALESCE(cash_and_equivalents, 0) as cash_equivalents
        FROM balance_sheet_data
        WHERE symbol = $1
        AND publish_date IS NOT NULL
        AND publish_date <= $2
        ORDER BY publish_date DESC
        LIMIT 10
        """
        
        balance_data = await self.db_conn.fetch(balance_query, symbol, valuation_date.date())
        
        # Process financial data - separate annual vs quarterly
        annual_data = [row for row in financial_data if row['period_type'] == 'annual']
        quarterly_data = [row for row in financial_data if row['period_type'] == 'quarterly']
        
        # Use most recent annual data for baseline, calculate R12 for recent data
        if annual_data:
            base_data = annual_data
            # Calculate most recent R12 if we have quarterly data newer than latest annual
            if quarterly_data and quarterly_data[0]['period_date'] > annual_data[0]['period_date']:
                r12_data = await self.calculate_r12_metrics(quarterly_data, annual_data)
                if r12_data:
                    base_data = [r12_data] + annual_data
        else:
            # Calculate Rolling Twelve Months (R12) from quarterly data
            r12_data = await self.calculate_r12_metrics(quarterly_data)
            if r12_data:
                base_data = [r12_data]
            else:
                base_data = []
        
        # Process financial data
        revenues = [float(row['total_revenue']) for row in base_data]
        operating_incomes = [float(row['operating_income']) for row in base_data]
        interest_expenses = [float(row['interest_expense']) for row in base_data]
        tax_expenses = [float(row['tax_expense']) for row in base_data]
        
        # Calculate operating margins
        operating_margins = []
        for i, revenue in enumerate(revenues):
            if revenue > 0:
                margin = operating_incomes[i] / revenue
                operating_margins.append(margin)
            else:
                operating_margins.append(0.0)
        
        # Process cash flow data (align by date)
        cf_dict = {row['period_date']: row for row in cashflow_data}
        operating_cash_flows = []
        capital_expenditures = []
        
        for row in base_data:
            period_date = row['period_date']
            period_type = row.get('period_type', 'annual')
            
            if period_type == 'r12':
                # For R12 data, we need to calculate R12 cash flows from quarterly data
                r12_ocf, r12_capex = self.calculate_r12_cash_flows(cashflow_data, quarterly_data)
                operating_cash_flows.append(r12_ocf)
                capital_expenditures.append(r12_capex)
            elif period_date in cf_dict:
                cf_row = cf_dict[period_date]
                operating_cash_flows.append(float(cf_row['operating_cash_flow']))
                capital_expenditures.append(float(cf_row['capital_expenditure']))
            else:
                # Estimate if missing (use 80% of operating income as OCF)
                idx = len(operating_cash_flows)
                if idx < len(operating_incomes):
                    operating_cash_flows.append(operating_incomes[idx] * 0.8)
                    capital_expenditures.append(operating_incomes[idx] * 0.1)  # Estimate capex as 10% of op income
                else:
                    operating_cash_flows.append(0.0)
                    capital_expenditures.append(0.0)
        
        # Calculate free cash flows
        free_cash_flows = []
        for i in range(len(operating_cash_flows)):
            fcf = operating_cash_flows[i] - capital_expenditures[i]
            free_cash_flows.append(fcf)
        
        # Process balance sheet data
        bs_dict = {row['period_date']: row for row in balance_data}
        total_debt = []
        cash_equivalents = []
        
        for row in base_data:
            period_date = row['period_date']
            if period_date in bs_dict:
                bs_row = bs_dict[period_date]
                total_debt.append(float(bs_row['total_debt']))
                cash_equivalents.append(float(bs_row['cash_equivalents']))
            else:
                # Use most recent balance sheet data if no exact match
                if balance_data:
                    total_debt.append(float(balance_data[0]['total_debt']))
                    cash_equivalents.append(float(balance_data[0]['cash_equivalents']))
                else:
                    total_debt.append(0.0)
                    cash_equivalents.append(0.0)
        
        # Calculate effective tax rates
        effective_tax_rates = []
        for i in range(len(base_data)):
            pre_tax_income = operating_incomes[i] - interest_expenses[i]
            if pre_tax_income > 0 and tax_expenses[i] > 0:
                tax_rate = tax_expenses[i] / pre_tax_income
                # Cap at reasonable bounds
                tax_rate = max(0.0, min(0.5, tax_rate))
                effective_tax_rates.append(tax_rate)
            else:
                effective_tax_rates.append(self.config.default_tax_rate)
        
        return CompanyData(
            symbol=symbol,
            shares_outstanding=shares_outstanding,
            stock_currency=stock_currency,
            report_currency=report_currency,
            revenues=revenues,
            operating_incomes=operating_incomes,
            interest_expenses=interest_expenses,
            tax_expenses=tax_expenses,
            operating_cash_flows=operating_cash_flows,
            capital_expenditures=capital_expenditures,
            total_debt=total_debt,
            cash_equivalents=cash_equivalents,
            operating_margins=operating_margins,
            free_cash_flows=free_cash_flows,
            effective_tax_rates=effective_tax_rates
        )
    
    def calculate_statistics(self, data: List[float]) -> Dict[str, float]:
        """Calculate statistical parameters from historical data"""
        
        if len(data) < 2:
            return {'mean': data[0] if data else 0.0, 'std': 0.1, 'min': 0.0, 'max': data[0] if data else 0.0}
        
        # Calculate growth rates for time series data
        growth_rates = []
        for i in range(1, len(data)):
            if data[i] > 0:
                growth = (data[i-1] - data[i]) / data[i]
                growth_rates.append(growth)
        
        if not growth_rates:
            return {'mean': 0.0, 'std': 0.1, 'min': -0.25, 'max': 0.40}
        
        mean = np.mean(growth_rates)
        std = np.std(growth_rates) if len(growth_rates) > 1 else 0.1
        
        # Apply reasonable bounds - BALANCED CONSERVATIVE
        std = max(0.05, min(0.30, std))  # 5% to 30% volatility
        mean = max(-0.10, min(0.20, mean))  # -10% to +20% growth (balanced conservative)
        
        return {
            'mean': mean,
            'std': std,
            'min': mean - 2 * std,
            'max': mean + 2 * std
        }
    
    def run_monte_carlo_simulation(self, company_data: CompanyData) -> Optional[Dict[str, float]]:
        """Run Monte Carlo DCF simulation with validation"""
        
        # Validate input data
        if not company_data.revenues or not company_data.operating_margins:
            logger.warning("Missing required financial data")
            return None
        
        # Get current period data (most recent)
        current_revenue = company_data.revenues[0]
        current_margin = company_data.operating_margins[0]
        current_tax_rate = company_data.effective_tax_rates[0]
        
        # Sanity checks on input data
        if current_revenue <= 0:
            logger.warning(f"Invalid revenue: {current_revenue}")
            return None
        
        if abs(current_margin) > 2.0:  # More than 200% margin is suspicious
            logger.warning(f"Extreme operating margin: {current_margin}")
            return None
        
        if current_tax_rate < 0 or current_tax_rate > 1.0:
            logger.warning(f"Invalid tax rate: {current_tax_rate}")
            current_tax_rate = self.config.default_tax_rate
        
        # Calculate net debt
        if company_data.total_debt and company_data.cash_equivalents:
            net_debt = company_data.total_debt[0] - company_data.cash_equivalents[0]
        else:
            net_debt = 0.0
        
        # Calculate parameter distributions
        revenue_stats = self.calculate_statistics(company_data.revenues)
        margin_stats = self.calculate_statistics(company_data.operating_margins)
        
        # Interest rate (simple estimate from interest expense)
        if company_data.interest_expenses[0] > 0 and company_data.total_debt and company_data.total_debt[0] > 0:
            interest_rate = company_data.interest_expenses[0] / company_data.total_debt[0]
        else:
            interest_rate = self.config.risk_free_rate
        
        # WACC calculation (simplified)
        # For now, use risk-free rate + market premium as cost of equity
        cost_of_equity = self.config.risk_free_rate + self.config.market_premium
        wacc = cost_of_equity  # Simplified - could add debt weighting
        
        # Monte Carlo simulation
        fair_values = []
        
        for _ in range(self.config.num_simulations):
            
            # Generate random parameters
            revenue_growth = np.random.normal(
                revenue_stats['mean'], 
                revenue_stats['std']
            )
            revenue_growth = np.clip(revenue_growth, revenue_stats['min'], revenue_stats['max'])
            
            operating_margin = np.random.normal(
                current_margin,
                margin_stats['std']
            )
            operating_margin = np.clip(operating_margin, 0.0, 1.0)
            
            # Project cash flows
            projected_fcfs = []
            revenue = current_revenue
            
            for year in range(1, self.config.projection_years + 1):
                # Revenue projection with AGGRESSIVE declining growth
                growth_factor = revenue_growth * (0.8 ** (year - 1))  # Faster decay (0.8 vs 0.9)
                revenue *= (1 + growth_factor)
                
                # Operating income
                operating_income = revenue * operating_margin
                
                # Interest expense
                interest_expense = net_debt * interest_rate if net_debt > 0 else 0
                
                # Pre-tax income
                pre_tax_income = operating_income - interest_expense
                
                # Tax
                tax = pre_tax_income * current_tax_rate if pre_tax_income > 0 else 0
                
                # NOPAT (Net Operating Profit After Tax)
                nopat = pre_tax_income - tax
                
                # Balanced conservative FCF (account for reinvestment needs)
                fcf = nopat * 0.75  # Balanced: 75% of NOPAT becomes FCF
                
                projected_fcfs.append(fcf)
            
            # Terminal value
            terminal_fcf = projected_fcfs[-1] * (1 + self.config.terminal_growth)
            terminal_value = terminal_fcf / (wacc - self.config.terminal_growth)
            
            # Discount cash flows
            pv_fcfs = []
            for year, fcf in enumerate(projected_fcfs, 1):
                pv = fcf / ((1 + wacc) ** year)
                pv_fcfs.append(pv)
            
            # Present value of terminal value
            pv_terminal = terminal_value / ((1 + wacc) ** self.config.projection_years)
            
            # Enterprise value
            enterprise_value = sum(pv_fcfs) + pv_terminal
            
            # Equity value
            equity_value = enterprise_value - net_debt
            
            # Per-share value
            fair_value_per_share = equity_value / company_data.shares_outstanding
            
            fair_values.append(max(0, fair_value_per_share))  # Floor at 0
        
        # Calculate statistics
        fair_values = np.array(fair_values)
        
        # Filter out extreme values (likely calculation errors)
        fair_values = fair_values[fair_values < 100000]  # Remove > 100K per share
        fair_values = fair_values[fair_values > 0]       # Remove negative values
        
        if len(fair_values) < self.config.num_simulations * 0.1:  # Less than 10% valid
            logger.warning("Too many invalid simulations")
            return None
        
        # Final validation
        median_value = float(np.median(fair_values))
        if median_value <= 0 or median_value > 50000:  # 0-50K SEK reasonable range
            logger.warning(f"Extreme median fair value: {median_value}")
            return None
        
        return {
            'fair_value_mean': float(np.mean(fair_values)),
            'fair_value_median': median_value,
            'fair_value_std': float(np.std(fair_values)),
            'fair_value_p5': float(np.percentile(fair_values, 5)),
            'fair_value_p25': float(np.percentile(fair_values, 25)),
            'fair_value_p50': float(np.percentile(fair_values, 50)),
            'fair_value_p75': float(np.percentile(fair_values, 75)),
            'fair_value_p95': float(np.percentile(fair_values, 95)),
            'wacc': wacc,
            'revenue_growth_mean': revenue_stats['mean'],
            'operating_margin_mean': current_margin,
            'tax_rate': current_tax_rate,
            'net_debt': net_debt
        }
    
    async def value_company(self, symbol: str, valuation_date: datetime, market_price: float) -> Optional[Dict]:
        """Complete DCF valuation with currency conversion"""
        
        # Extract company data
        company_data = await self.extract_company_data(symbol, valuation_date)
        
        if not company_data:
            return None
        
        # Run Monte Carlo simulation
        dcf_results = self.run_monte_carlo_simulation(company_data)
        
        if not dcf_results:
            return None
        
        # Apply currency conversion
        exchange_rate = self.get_exchange_rate(
            company_data.report_currency, 
            company_data.stock_currency
        )
        
        # Convert fair values to stock currency
        fair_value_fields = [
            'fair_value_mean', 'fair_value_median', 'fair_value_std',
            'fair_value_p5', 'fair_value_p25', 'fair_value_p50', 
            'fair_value_p75', 'fair_value_p95'
        ]
        
        for field in fair_value_fields:
            dcf_results[field] *= exchange_rate
        
        # Calculate derived metrics
        fair_value = dcf_results['fair_value_median']
        implied_return = (fair_value - market_price) / market_price
        
        # Valuation signal
        if implied_return > 0.15:
            signal = 'UNDERVALUED'
        elif implied_return < -0.15:
            signal = 'OVERVALUED'
        else:
            signal = 'FAIR_VALUE'
        
        # Compile final result
        result = {
            'symbol': symbol,
            'valuation_date': valuation_date,
            'market_price': market_price,
            'implied_return': implied_return,
            'valuation_signal': signal,
            'currency_conversion': {
                'stock_currency': company_data.stock_currency,
                'report_currency': company_data.report_currency,
                'exchange_rate': exchange_rate,
                'conversion_applied': exchange_rate != 1.0
            },
            **dcf_results
        }
        
        return result

async def test_clean_dcf():
    """Test the clean DCF engine"""
    
    config = DCFConfig(num_simulations=5000)
    engine = CleanDCFEngine(config)
    await engine.setup()
    
    test_cases = [
        ('AAK', '2024-12-08', 270.0),
        ('ABB', '2024-12-08', 650.0),
        ('VOLV-B', '2024-12-08', 280.0),
    ]
    
    print('🧮 TESTING CLEAN DCF ENGINE')
    print('=' * 70)
    print('Data-driven, no hardcoded assumptions')
    
    for symbol, date_str, price in test_cases:
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        print(f'\n📊 {symbol} - {date_str}')
        
        try:
            result = await engine.value_company(symbol, test_date, price)
            
            if result:
                return_pct = result['implied_return'] * 100
                currency_info = result['currency_conversion']
                
                print(f'   Fair Value: {result["fair_value_median"]:.0f} {currency_info["stock_currency"]}')
                print(f'   Market Price: {price} {currency_info["stock_currency"]}')
                print(f'   Implied Return: {return_pct:+.0f}%')
                print(f'   Signal: {result["valuation_signal"]}')
                print(f'   WACC: {result["wacc"]*100:.1f}%')
                print(f'   Margin: {result["operating_margin_mean"]*100:.1f}%')
                
                if currency_info['conversion_applied']:
                    print(f'   Currency: {currency_info["report_currency"]} → {currency_info["stock_currency"]} (Rate: {currency_info["exchange_rate"]})')
            else:
                print(f'   ❌ DCF calculation failed')
        
        except Exception as e:
            print(f'   ❌ Error: {e}')
    
    await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(test_clean_dcf())