"""
Focus-Narrowing Engine - Core calculation logic

Inverts stock prices to reveal implied growth expectations and finds mispricings.
All parameters flat across the universe for comparability.
"""
from typing import List, Dict, Any
import math
from .models import CompanyInput, TriageResult, Side


class FocusNarrowingEngine:
    """
    The triage funnel. Takes company inputs, inverts prices to implied growth,
    compares to what business can deliver, classifies and ranks.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration.

        Args:
            config: Dict with keys:
                - required_return (r)
                - horizon_primary (N1, in YEARS)
                - horizon_long (N2, in YEARS)
                - exit_multiple_ref
                - roic_quality_cutoff
                - gap_material
                - reinvestment_rate
        """
        self.r = config['required_return']
        self.N1 = config['horizon_primary']
        self.N2 = config['horizon_long']
        self.exit_multiple = config['exit_multiple_ref']
        self.roic_cutoff = config['roic_quality_cutoff']
        self.gap_material = config['gap_material']
        self.gap_underpriced_min = config['gap_underpriced_min']
        self.reinvestment_rate = config['reinvestment_rate']

        # Data sanity clamps
        self.roic_cap = config['roic_cap']
        self.growth_clamp = config['growth_clamp']
        self.gap_clamp = config['gap_clamp']
        self.pe_min = config['pe_min']
        self.pe_max = config['pe_max']

        # Durable compounder gate parameters
        self.min_track_years = config['min_track_years']
        self.abs_growth_cap = config['abs_growth_cap']
        self.organic_roic_cap = config['organic_roic_cap']
        self.growth_consistency_threshold = config['growth_consistency_threshold']

    def process_company(self, company: CompanyInput) -> TriageResult:
        """
        Process a single company through the triage funnel.

        Returns:
            TriageResult with all computed fields
        """

        # Step 1: Invert price into implied growth at both horizons
        g_short, g_long = None, None

        # Data quality checks for EPS:
        # 1. Materiality floor - if EPS is trivially small relative to price, route to ASSET_ONLY
        # 2. P/E sanity check - if implied P/E outside sane range, data is suspect (currency mismatch)
        eps_is_usable = False
        if company.eps_norm and company.eps_norm > 0:
            implied_pe = company.price / company.eps_norm
            # EPS must be material (>0.1% yield) and P/E must be sane
            if (company.eps_norm / company.price) > 0.001 and self.pe_min <= implied_pe <= self.pe_max:
                eps_is_usable = True

        if eps_is_usable:
            g_short = self._implied_growth(
                price=company.price,
                eps_norm=company.eps_norm,
                horizon_years=self.N1
            )
            g_long = self._implied_growth(
                price=company.price,
                eps_norm=company.eps_norm,
                horizon_years=self.N2
            )

        # Step 2: Calculate reference growth and gap
        ref_growth = None
        ceiling = None
        gap = None

        if company.roic and company.growth_hist is not None:
            # Check if this is a durable compounder (anti-Nordrest guard)
            is_durable = self._is_durable_compounder(company)

            if is_durable and company.growth_cagr_robust is not None:
                # DURABLE BRANCH: Use demonstrated CAGR with absolute cap
                # This admits proven compounders without admitting short-history spikes
                ref_growth = min(company.growth_cagr_robust, self.abs_growth_cap)

                # For ceiling display (not used in ref_growth for durable)
                # Use organic ROIC if available, otherwise regular ROIC
                effective_roic = company.organic_roic if company.organic_roic is not None else company.roic
                effective_roic = min(effective_roic, self.organic_roic_cap) if effective_roic else company.roic
                ceiling = min(effective_roic * self.reinvestment_rate, self.growth_clamp) if effective_roic else None
            else:
                # STANDARD BRANCH: Conservative ROIC ceiling (current behavior)
                # Cap ROIC to prevent tiny-denominator artifacts
                roic_capped = min(company.roic, self.roic_cap)

                # Clamp growth_hist to prevent sign-flipping artifacts
                growth_hist_clamped = max(-self.growth_clamp, min(self.growth_clamp, company.growth_hist))

                # Sustainable growth ceiling from capped ROIC
                ceiling = roic_capped * self.reinvestment_rate

                # Clamp ceiling to sane range
                ceiling = max(-self.growth_clamp, min(self.growth_clamp, ceiling))

                # Reference = minimum of clamped historical growth and ceiling
                ref_growth = min(growth_hist_clamped, ceiling)

            # Gap (positive = market requires less than business can deliver)
            if g_short is not None:
                gap = ref_growth - g_short
                # Clamp gap to prevent explosions
                gap = max(-self.gap_clamp, min(self.gap_clamp, gap))

        # Step 3: Asset play - discount to NAV
        disc_to_nav = None
        if company.nav_ps and company.nav_ps > 0:
            disc_to_nav = (company.nav_ps - company.price) / company.nav_ps

        # Step 4: Compounder diagnostic (duration dependence)
        duration_dependence = None
        if g_short is not None and g_long is not None:
            duration_dependence = g_short - g_long

        # Step 4.5: Data quality gate - check for corrupted/distressed data
        is_suspect = self._is_data_suspect(
            roic=company.roic,
            growth_hist=company.growth_hist,
            nav_ps=company.nav_ps,
            disc_to_nav=disc_to_nav,
            ref_growth=ref_growth
        )

        # Step 5: Classify into side
        if is_suspect:
            side = Side.DATA_SUSPECT
        else:
            side = self._classify_side(
                company=company,
                g_short=g_short,
                g_long=g_long,
                ref_growth=ref_growth,
                roic=company.roic,
                growth_hist=company.growth_hist,
                disc_to_nav=disc_to_nav,
                duration_dependence=duration_dependence
            )

        # Step 6: Calculate triage priority
        triage_priority = self._calculate_priority(
            gap=gap,
            disc_to_nav=disc_to_nav,
            side=side
        )

        # Step 7: Generate hinge message
        hinge = self._generate_hinge(
            side=side,
            g_short=g_short,
            g_long=g_long,
            ref_growth=ref_growth,
            growth_hist=company.growth_hist,
            disc_to_nav=disc_to_nav,
            horizon_short=self.N1,
            horizon_long=self.N2
        )

        return TriageResult(
            ticker=company.ticker,
            name=company.name,
            price=company.price,
            g_short=g_short,
            g_long=g_long,
            ref_growth=ref_growth,
            ceiling=ceiling,
            gap=gap,
            disc_to_nav=disc_to_nav,
            duration_dependence=duration_dependence,
            side=side,
            triage_priority=triage_priority,
            hinge=hinge,
            roic=company.roic,
            growth_hist=company.growth_hist
        )

    def _implied_growth(self, price: float, eps_norm: float, horizon_years: float) -> float:
        """
        Invert price to calculate implied growth rate.

        Formula: Price = eps_norm * (1+g)^N * exit_multiple / (1+r)^N
        Solve for g: g = ((price * (1+r)^N) / (eps_norm * exit_multiple))^(1/N) - 1

        Args:
            price: Current stock price
            eps_norm: Normalized earnings per share
            horizon_years: Time horizon in YEARS

        Returns:
            Implied annual growth rate (as decimal, e.g. 0.08 = 8%)
        """
        if eps_norm <= 0:
            return None

        N = horizon_years

        # (price * (1+r)^N) / (eps_norm * exit_multiple)
        numerator = price * ((1 + self.r) ** N)
        denominator = eps_norm * self.exit_multiple

        if denominator <= 0:
            return None

        ratio = numerator / denominator

        if ratio <= 0:
            return None

        # Take Nth root and subtract 1
        g = (ratio ** (1 / N)) - 1

        return g

    def _is_durable_compounder(self, company: CompanyInput) -> bool:
        """
        Check if company qualifies as a durable compounder.

        A durable compounder must have ALL of:
        - Track record >= min_track_years (config, default 5)
        - Growth consistency >= threshold (positive growth in most years)
        - Profitable / positive operating cash flow (indicated by positive ROIC)

        This is the anti-Nordrest guard: short-history acquisition spikes fail
        and stay on the conservative branch.

        Returns:
            True if company is a durable compounder
        """
        # Must have minimum track record
        if not company.track_record_years or company.track_record_years < self.min_track_years:
            return False

        # Must have consistent growth (positive in most years)
        if not company.growth_consistency_score or company.growth_consistency_score < self.growth_consistency_threshold:
            return False

        # Must have robust CAGR data
        if company.growth_cagr_robust is None:
            return False

        # Must be profitable (positive ROIC across the record)
        # Use organic ROIC if available for goodwill-heavy companies, otherwise regular ROIC
        effective_roic = company.organic_roic if company.organic_roic is not None else company.roic
        if not effective_roic or effective_roic <= 0:
            return False

        return True

    def _is_data_suspect(
        self,
        roic: float,
        growth_hist: float,
        nav_ps: float,
        disc_to_nav: float,
        ref_growth: float
    ) -> bool:
        """
        Gate for physically implausible data indicating distressed/corrupted company.

        Catches:
        - Negative or near-zero book equity (nav_ps <= 0)
        - Negative ROIC (< -100%)
        - Absurd growth rates (< -90% or > 300%)

        NOTE: Does NOT trigger on large negative disc_to_nav alone.
        High P/B (showing as large negative disc_to_nav) is normal for high-ROIC quality businesses.
        Only actual negative/missing book equity is suspect.
        """
        # Negative or near-zero book equity (actual distress signal)
        if nav_ps is not None and nav_ps <= 0:
            return True

        # Negative ROIC (bankruptcy / negative invested capital)
        if roic is not None and roic < -1.0:  # < -100%
            return True

        # Absurd growth rate (sign-flipping earnings)
        # Check BEFORE clamping to catch truly broken data
        if growth_hist is not None:
            if growth_hist < -0.90 or growth_hist > 3.0:  # < -90% or > 300%
                return True

        return False

    def _classify_side(
        self,
        company: CompanyInput,
        g_short: float,
        g_long: float,
        ref_growth: float,
        roic: float,
        growth_hist: float,
        disc_to_nav: float,
        duration_dependence: float
    ) -> Side:
        """
        Classify company into one of five sides.

        Rules:
        - ASSET_ONLY: No usable earnings (g_short is None)
        - FEAR_PREMIUM: Low/negative g_short OR deep discount to NAV
        - UNDERPRICED_DURABILITY: High ROIC, g_long achievable, duration-dependent
        - EXPENSIVE_FRAGILE: g_long implausible, mediocre ROIC
        - FAIR_NO_EDGE: Gap immaterial
        """

        # No earnings base - judge on NAV
        if g_short is None:
            return Side.ASSET_ONLY

        # Deep discount to NAV = fear premium
        if disc_to_nav and disc_to_nav > 0.30:  # 30%+ discount
            return Side.FEAR_PREMIUM

        # Low implied growth = pessimism priced in
        if g_short < 0.05:  # Less than 5% growth priced in
            return Side.FEAR_PREMIUM

        # Check for quality compounder characteristics
        # Fix 3: Quality gauge for acquirers
        # - Durable + profitable = quality signal (even if reported ROIC low due to goodwill)
        # - High organic ROIC = quality signal (for goodwill-heavy companies)
        # - High reported ROIC = quality signal (standard case)

        is_quality_compounder = False
        passes_quality_gate = False

        if duration_dependence and roic is not None and g_long is not None and growth_hist is not None and duration_dependence > 0.02:
            # Route 1: Durable compounder + profitable (simpler route)
            if self._is_durable_compounder(company):
                passes_quality_gate = True

            # Route 2: High ROIC (use organic if available for goodwill-heavy companies)
            if not passes_quality_gate:
                # For high-goodwill companies (>40% of invested capital), use organic ROIC if available
                if company.goodwill_fraction and company.goodwill_fraction > 0.40:
                    effective_roic = company.organic_roic if company.organic_roic is not None else roic
                else:
                    effective_roic = roic

                if effective_roic and effective_roic >= self.roic_cutoff:
                    passes_quality_gate = True

            # If passes quality gate, check if g_long is achievable
            if passes_quality_gate:
                # Use the better of growth_hist or ref_growth (for durable compounders, ref_growth is higher)
                demonstrated_growth = max(growth_hist, ref_growth) if ref_growth is not None else growth_hist

                if g_long <= demonstrated_growth * 2.0:  # Within 2x of demonstrated history
                    is_quality_compounder = True

        # CRITICAL GUARD: Quality businesses with achievable g_long should never be EXPENSIVE_FRAGILE
        # Even if they don't show duration dependence (serial acquirers, etc.)
        # Compare g_long to demonstrated growth_hist, NOT ceiling-crushed ref_growth
        # Use a lower threshold (12%) here to catch quality compounders just below the cutoff
        if not is_quality_compounder and g_long is not None and growth_hist is not None:
            # Use organic ROIC for goodwill-heavy companies
            effective_roic = roic
            if company.goodwill_fraction and company.goodwill_fraction > 0.40:
                effective_roic = company.organic_roic if company.organic_roic is not None else roic

            if effective_roic and effective_roic >= 0.12:  # Reasonable quality threshold
                # If g_long is within reasonable range of demonstrated growth, it's fair/compounder territory
                demonstrated_growth = max(growth_hist, ref_growth) if ref_growth is not None else growth_hist

                if g_long <= demonstrated_growth * 1.5:  # Within 50% of what they've proven
                    # High ROIC + duration dependent = compounder
                    if effective_roic >= self.roic_cutoff and duration_dependence and duration_dependence > 0.02:
                        is_quality_compounder = True

        # If it's a quality compounder, check if it's cheap (UNDERPRICED) or just on the watchlist
        if is_quality_compounder:
            gap_val = ref_growth - g_short if ref_growth is not None and g_short is not None else None
            if gap_val is not None and gap_val >= self.gap_underpriced_min:
                return Side.UNDERPRICED_DURABILITY
            else:
                return Side.QUALITY_WATCH  # Quality business, but not cheap enough

        # Expensive and fragile (heroic assumptions)
        # Only gets here if:
        # 1. Mediocre ROIC, OR
        # 2. g_long exceeds 1.5x demonstrated growth (implausible even for quality)
        if g_long is not None and growth_hist is not None:
            # Use growth_hist directly, not ceiling-crushed ref_growth
            if roic is None or roic < self.roic_cutoff:
                # Mediocre business - check against ref_growth (more conservative)
                if ref_growth is not None and g_long > ref_growth * 1.5:
                    return Side.EXPENSIVE_FRAGILE
            elif g_long > growth_hist * 2.5:
                # Even quality businesses have limits - 2.5x demonstrated is heroic
                return Side.EXPENSIVE_FRAGILE

        # Gap immaterial = fair value
        if ref_growth is not None:
            gap = ref_growth - g_short
            if abs(gap) < self.gap_material:
                return Side.FAIR_NO_EDGE

        # Default: No strong signal
        return Side.FAIR_NO_EDGE

    def _calculate_priority(self, gap: float, disc_to_nav: float, side: Side) -> float:
        """
        Calculate triage priority score for ranking.

        Higher priority = bigger opportunity/mispricing.

        WINSORIZATION: Cap gap and disc_to_nav at ±100% to prevent
        corrupted data from dominating rankings.
        """
        # Winsorize inputs to sane range (±100%)
        gap_winsorized = max(-1.0, min(1.0, gap)) if gap is not None else 0.0
        nav_winsorized = max(-1.0, min(1.0, disc_to_nav)) if disc_to_nav is not None else 0.0

        if side == Side.DATA_SUSPECT:
            # Push to bottom of rankings
            return 0.0

        if side == Side.ASSET_ONLY:
            # Rank by discount to NAV (winsorized)
            return nav_winsorized

        if side == Side.FEAR_PREMIUM:
            # Rank by gap or NAV discount, whichever is bigger
            gap_score = abs(gap_winsorized)
            nav_score = nav_winsorized
            return max(gap_score, nav_score)

        if side == Side.UNDERPRICED_DURABILITY:
            # Rank by gap descending (most underpriced first)
            # Positive gap = market requires less than business can deliver
            return gap_winsorized

        if side == Side.QUALITY_WATCH:
            # Rank by ROIC or quality proxy (not by gap - they're all expensive)
            # Use negative gap magnitude to show most overvalued quality names
            return -abs(gap_winsorized)

        if side == Side.EXPENSIVE_FRAGILE:
            # Rank by how implausible (negative gap = market requires more)
            return abs(gap_winsorized)

        # FAIR_NO_EDGE - low priority
        return 0.01

    def _generate_hinge(
        self,
        side: Side,
        g_short: float,
        g_long: float,
        ref_growth: float,
        growth_hist: float,
        disc_to_nav: float,
        horizon_short: int,
        horizon_long: int
    ) -> str:
        """
        Generate "what you'd have to believe" hinge message.

        The human handoff - prime the deep dive.
        """
        if side == Side.ASSET_ONLY:
            if disc_to_nav and disc_to_nav > 0:
                return f"Trading {disc_to_nav:.0%} below NAV; no earnings base"
            return "No earnings base; asset play only"

        if side == Side.FEAR_PREMIUM:
            if g_short is not None and growth_hist is not None:
                return f"Market prices {g_short:.1%} growth; has done {growth_hist:.1%}"
            if disc_to_nav:
                return f"Trading {disc_to_nav:.0%} below NAV"
            return "Pessimism priced in"

        if side == Side.UNDERPRICED_DURABILITY:
            if g_long is not None and ref_growth is not None:
                return f"Priced for {g_long:.1%} over {horizon_long:.0f}y; franchise supports {ref_growth:.1%}"
            return "Long-duration compounder undervalued"

        if side == Side.QUALITY_WATCH:
            if g_long is not None and ref_growth is not None:
                return f"Quality franchise; priced for {g_long:.1%}, supports {ref_growth:.1%} — watch for entry"
            return "Quality compounder; not cheap yet — watchlist"

        if side == Side.EXPENSIVE_FRAGILE:
            if g_long is not None and ref_growth is not None:
                return f"Needs {g_long:.1%} for {horizon_long:.0f}y; has done {growth_hist:.1%}"
            return "Heroic assumptions priced in"

        if side == Side.FAIR_NO_EDGE:
            return "Fair value; no material edge"

        if side == Side.DATA_SUSPECT:
            return "Data quality issues (distressed/corrupted)"

        return "Unknown"

    def process_universe(self, companies: List[CompanyInput]) -> List[TriageResult]:
        """
        Process entire universe and return ranked results.

        Args:
            companies: List of CompanyInput objects

        Returns:
            List of TriageResult, sorted by triage_priority descending
        """
        results = []

        for company in companies:
            result = self.process_company(company)
            results.append(result)

        # Sort by priority descending (biggest opportunities first)
        results.sort(key=lambda x: x.triage_priority, reverse=True)

        return results
