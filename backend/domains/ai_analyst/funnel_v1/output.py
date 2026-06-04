"""
Output writer - ranked CSV and console display
"""
import pandas as pd
from typing import List
from .models import TriageResult, Side


def get_currency_symbol(ticker: str) -> str:
    """
    Get currency symbol from ticker suffix.

    Nordic exchanges use suffixes:
    - .ST = Stockholm (SEK)
    - .OL = Oslo (NOK)
    - .CO = Copenhagen (DKK)
    - .HE = Helsinki (EUR)
    - No suffix or other = assume SEK (default)
    """
    # For now, check if ticker contains space (Swedish format like "VOLV B")
    # or ends with a suffix - in either case, assume Nordic
    if ' ' in ticker or '-' in ticker:
        # Swedish format (space or hyphen in ticker) = SEK
        return 'SEK'
    # Could add more sophisticated logic here based on company_master.yahoo_symbol
    # For now, default to SEK for Nordic universe
    return 'SEK'


class OutputWriter:
    """
    Write triage results to CSV and console.
    """

    @staticmethod
    def to_dataframe(results: List[TriageResult]) -> pd.DataFrame:
        """
        Convert TriageResult list to pandas DataFrame.

        Args:
            results: List of TriageResult objects

        Returns:
            DataFrame with all fields
        """
        rows = []
        for r in results:
            rows.append({
                'ticker': r.ticker,
                'name': r.name,
                'price': r.price,
                'g_short': r.g_short,
                'g_long': r.g_long,
                'ref_growth': r.ref_growth,
                'ceiling': r.ceiling,
                'gap': r.gap,
                'disc_to_nav': r.disc_to_nav,
                'duration_dependence': r.duration_dependence,
                'roic': r.roic,
                'growth_hist': r.growth_hist,
                'side': r.side.value,
                'triage_priority': r.triage_priority,
                'hinge': r.hinge,
            })

        return pd.DataFrame(rows)

    @staticmethod
    def write_csv(results: List[TriageResult], filepath: str):
        """
        Write results to CSV file.

        Args:
            results: List of TriageResult objects
            filepath: Output CSV path
        """
        df = OutputWriter.to_dataframe(results)
        df.to_csv(filepath, index=False)
        print(f"✅ Wrote {len(results)} results to {filepath}")

    @staticmethod
    def print_top_n(results: List[TriageResult], n: int = 20, side_filter: Side = None):
        """
        Print top N results to console.

        Args:
            results: List of TriageResult objects (already sorted)
            n: Number of results to print
            side_filter: Optional side filter (e.g., Side.FEAR_PREMIUM)
        """
        # Filter by side if requested
        if side_filter:
            filtered = [r for r in results if r.side == side_filter]
            print(f"\n{'='*80}")
            print(f"TOP {n} - {side_filter.value}")
            print(f"{'='*80}")
        else:
            filtered = results
            print(f"\n{'='*80}")
            print(f"TOP {n} - ALL SIDES")
            print(f"{'='*80}")

        # Print results
        for i, result in enumerate(filtered[:n], 1):
            currency = get_currency_symbol(result.ticker)
            print(f"\n{i}. {result.ticker} - {result.name}")
            print(f"   Price: {result.price:.2f} {currency} | Side: {result.side.value}")
            print(f"   Priority: {result.triage_priority:.3f}")

            if result.g_short is not None:
                print(f"   Implied Growth (12mo): {result.g_short:.1%}")
            if result.g_long is not None:
                print(f"   Implied Growth (24mo): {result.g_long:.1%}")
            if result.ref_growth is not None:
                print(f"   Reference Growth: {result.ref_growth:.1%}")
            if result.gap is not None:
                print(f"   Gap: {result.gap:+.1%}")
            if result.disc_to_nav is not None:
                print(f"   Discount to NAV: {result.disc_to_nav:.1%}")

            print(f"   💡 {result.hinge}")

    @staticmethod
    def print_summary(results: List[TriageResult]):
        """
        Print summary statistics by side.

        Args:
            results: List of TriageResult objects
        """
        print(f"\n{'='*80}")
        print("SUMMARY BY SIDE")
        print(f"{'='*80}")

        by_side = {}
        for result in results:
            side = result.side.value
            if side not in by_side:
                by_side[side] = []
            by_side[side].append(result)

        for side in Side:
            count = len(by_side.get(side.value, []))
            print(f"{side.value:30s}: {count:4d} companies")

        print(f"\nTotal: {len(results)} companies")
