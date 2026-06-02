"""
Screen Repository

Database operations for screen results and definitions.
"""

import json
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict, Any
from uuid import UUID

import asyncpg

from ..models.screen_result import ScreenResult
from ..models.screen_definition import ScreenDefinition


# Default expiry periods by run frequency
EXPIRY_PERIODS = {
    'daily': timedelta(days=7),
    'weekly': timedelta(days=14),
    'monthly': timedelta(days=45),
    'quarterly': timedelta(days=120),
    'annually': timedelta(days=400),
}


class ScreenRepository:
    """Repository for screen results and definitions."""

    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    # =========================================================================
    # SCREEN DEFINITIONS
    # =========================================================================

    async def get_all_definitions(self) -> List[ScreenDefinition]:
        """Get all screen definitions."""
        rows = await self.conn.fetch("""
            SELECT * FROM bsd_screen_definitions
            ORDER BY screen_type
        """)
        return [ScreenDefinition.from_db_row(dict(r)) for r in rows]

    async def get_definition(self, screen_type: int) -> Optional[ScreenDefinition]:
        """Get a specific screen definition."""
        row = await self.conn.fetchrow("""
            SELECT * FROM bsd_screen_definitions
            WHERE screen_type = $1
        """, screen_type)
        return ScreenDefinition.from_db_row(dict(row)) if row else None

    async def get_active_tier_a_screens(self) -> List[ScreenDefinition]:
        """Get all active screens that have Tier A enabled."""
        rows = await self.conn.fetch("""
            SELECT * FROM bsd_screen_definitions
            WHERE is_active = TRUE AND tier_a_enabled = TRUE
            ORDER BY screen_type
        """)
        return [ScreenDefinition.from_db_row(dict(r)) for r in rows]

    # =========================================================================
    # SCREEN RESULTS - SAVE
    # =========================================================================

    async def save_result(self, result: ScreenResult) -> int:
        """
        Save a screen result to the database.

        Uses delete + insert pattern to avoid complex upsert with partial indexes.
        Returns the ID of the inserted row.
        """
        # Get expiry period based on screen frequency
        definition = await self.get_definition(result.screen_type)
        if definition and not result.expires_at:
            result.expires_at = datetime.now() + EXPIRY_PERIODS.get(
                definition.run_frequency, timedelta(days=14)
            )

        # Deactivate any existing active result for this company/screen/tier
        await self.conn.execute("""
            UPDATE bsd_screen_results
            SET is_active = FALSE, updated_at = NOW()
            WHERE company_id = $1
              AND screen_type = $2
              AND tier = $3
              AND is_active = TRUE
        """, result.company_id, result.screen_type, result.tier)

        # Insert new result
        row_id = await self.conn.fetchval("""
            INSERT INTO bsd_screen_results
                (company_id, screen_type, tier, score, metrics, flags,
                 is_active, triggered_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """,
            result.company_id,
            result.screen_type,
            result.tier,
            result.score,
            json.dumps(result.metrics),
            result.flags,
            result.is_active,
            result.triggered_at,
            result.expires_at
        )

        return row_id

    async def save_results(self, results: List[ScreenResult]) -> int:
        """
        Save multiple screen results.

        Returns the number of results saved.
        """
        count = 0
        for result in results:
            await self.save_result(result)
            count += 1
        return count

    async def deactivate_old_results(self, screen_type: int) -> int:
        """
        Deactivate previous results for a screen type before saving new ones.

        Returns the number of deactivated rows.
        """
        result = await self.conn.execute("""
            UPDATE bsd_screen_results
            SET is_active = FALSE, updated_at = NOW()
            WHERE screen_type = $1 AND is_active = TRUE
        """, screen_type)
        return int(result.split()[-1]) if result else 0

    # =========================================================================
    # SCREEN RESULTS - QUERY
    # =========================================================================

    async def get_results_by_screen(
        self,
        screen_type: int,
        active_only: bool = True,
        limit: int = 100
    ) -> List[ScreenResult]:
        """Get results for a specific screen type."""
        query = """
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.screen_type = $1
        """
        if active_only:
            query += " AND sr.is_active = TRUE"
        query += " ORDER BY sr.score DESC LIMIT $2"

        rows = await self.conn.fetch(query, screen_type, limit)
        return [ScreenResult.from_db_row(dict(r)) for r in rows]

    async def get_results_by_company(
        self,
        company_id: UUID,
        active_only: bool = True
    ) -> List[ScreenResult]:
        """Get all screen results for a specific company."""
        query = """
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.company_id = $1
        """
        if active_only:
            query += " AND sr.is_active = TRUE"
        query += " ORDER BY sr.screen_type"

        rows = await self.conn.fetch(query, company_id)
        return [ScreenResult.from_db_row(dict(r)) for r in rows]

    async def get_all_active_results(self, limit: int = 2000) -> List[ScreenResult]:
        """Get all active screen results."""
        rows = await self.conn.fetch("""
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.is_active = TRUE AND sr.score > 0
            ORDER BY sr.screen_type, sr.score DESC
            LIMIT $1
        """, limit)
        return [ScreenResult.from_db_row(dict(r)) for r in rows]

    async def get_results_as_of_date(
        self,
        score_date: date,
        limit: int = 500
    ) -> List[ScreenResult]:
        """
        Get screen results as they were on a specific date (point-in-time).

        Returns results that were triggered on or before the given date
        and had not expired yet as of that date.

        This is critical for backtesting to avoid look-ahead bias.
        """
        rows = await self.conn.fetch("""
            WITH ranked_results AS (
                SELECT
                    sr.*,
                    c.company_name,
                    c.primary_ticker,
                    ROW_NUMBER() OVER (
                        PARTITION BY sr.company_id, sr.screen_type
                        ORDER BY sr.triggered_at DESC
                    ) as rn
                FROM bsd_screen_results sr
                JOIN bsd_v_companies c ON sr.company_id = c.id
                WHERE sr.triggered_at::date <= $1
                  AND (sr.expires_at IS NULL OR sr.expires_at::date > $1)
                  AND sr.score > 0
            )
            SELECT * FROM ranked_results
            WHERE rn = 1
            ORDER BY score DESC
            LIMIT $2
        """, score_date, limit)
        return [ScreenResult.from_db_row(dict(r)) for r in rows]

    async def get_multi_screen_hits(self, min_screens: int = 2) -> List[Dict[str, Any]]:
        """Get companies that triggered multiple screens."""
        rows = await self.conn.fetch("""
            SELECT * FROM bsd_v_multi_screen_hits
            WHERE screens_triggered >= $1
            ORDER BY screens_triggered DESC, total_score DESC
        """, min_screens)
        return [dict(r) for r in rows]

    async def get_warnings(self) -> List[ScreenResult]:
        """Get all results with warning flags."""
        rows = await self.conn.fetch("""
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.is_active = TRUE
              AND sr.flags IS NOT NULL
              AND ARRAY_LENGTH(sr.flags, 1) > 0
            ORDER BY sr.triggered_at DESC
        """)
        return [ScreenResult.from_db_row(dict(r)) for r in rows]

    async def get_result_by_company(
        self,
        screen_type: int,
        company_id: UUID
    ) -> Optional[ScreenResult]:
        """Get active result for a specific company and screen."""
        row = await self.conn.fetchrow("""
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.screen_type = $1
              AND sr.company_id = $2
              AND sr.is_active = TRUE
            ORDER BY sr.triggered_at DESC
            LIMIT 1
        """, screen_type, company_id)
        return ScreenResult.from_db_row(dict(row)) if row else None

    async def get_result_by_ticker(
        self,
        screen_type: int,
        ticker: str
    ) -> Optional[ScreenResult]:
        """Get active result for a company by ticker and screen."""
        row = await self.conn.fetchrow("""
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.screen_type = $1
              AND (c.primary_ticker ILIKE $2 OR c.primary_ticker ILIKE $3)
              AND sr.is_active = TRUE
            ORDER BY sr.triggered_at DESC
            LIMIT 1
        """, screen_type, ticker, ticker.replace(' ', '-'))
        return ScreenResult.from_db_row(dict(row)) if row else None

    async def update_result(self, result: ScreenResult) -> bool:
        """
        Update an existing screen result (e.g., after Tier B analysis).

        Returns True if update succeeded, False otherwise.
        """
        try:
            await self.conn.execute("""
                UPDATE bsd_screen_results
                SET
                    tier = $1,
                    score = $2,
                    metrics = $3,
                    flags = $4,
                    updated_at = NOW()
                WHERE company_id = $5
                  AND screen_type = $6
                  AND is_active = TRUE
            """,
                result.tier,
                result.score,
                json.dumps(result.metrics),
                result.flags,
                result.company_id,
                result.screen_type
            )
            return True
        except Exception:
            return False

    # =========================================================================
    # CYCLICAL ANTI-SCREEN WARNING (CRITICAL)
    # =========================================================================

    async def get_cyclicals_at_peak_on_value_screens(self) -> List[Dict[str, Any]]:
        """
        CRITICAL: Find cyclicals appearing on value screens (3, 4, 5) at peak earnings.

        This is the most dangerous oversight in screening - a shipping company
        at peak earnings will show up on Screen 5 with a beautiful P/E of 6,
        but it's actually a cyclical about to peak, not a distressed stable earner.

        Returns companies where:
        - Classified as CYCLICAL
        - Has mid_cycle_ebitda defined
        - Current EBITDA > mid_cycle_ebitda * 1.2 (above mid-cycle = near peak)
        - Appears on screens 3, 4, or 5 (value screens)
        """
        rows = await self.conn.fetch("""
            SELECT
                sr.id AS result_id,
                sr.screen_type,
                sr.company_id,
                sr.score,
                sr.flags,
                c.company_name,
                c.primary_ticker,
                cc.classification,
                cc.cycle_position,
                cc.mid_cycle_ebitda,
                cc.peak_to_trough_ratio,
                f.ebitda AS current_ebitda,
                CASE
                    WHEN cc.mid_cycle_ebitda > 0 THEN
                        f.ebitda / cc.mid_cycle_ebitda
                    ELSE NULL
                END AS ebitda_vs_mid_cycle
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            JOIN bsd_company_classifications cc ON sr.company_id = cc.company_id
            LEFT JOIN LATERAL (
                SELECT fs.ebitda
                FROM financial_statements fs
                WHERE fs.symbol = c.primary_ticker
                   OR fs.symbol = REPLACE(c.primary_ticker, '-', ' ')
                ORDER BY fs.period_date DESC
                LIMIT 1
            ) f ON TRUE
            WHERE sr.is_active = TRUE
              AND sr.screen_type IN (3, 4, 5)  -- Value screens
              AND cc.classification = 'CYCLICAL'
              AND cc.mid_cycle_ebitda IS NOT NULL
              AND cc.mid_cycle_ebitda > 0
              AND f.ebitda > cc.mid_cycle_ebitda * 1.2  -- Above mid-cycle = peak territory
            ORDER BY ebitda_vs_mid_cycle DESC
        """)
        return [dict(r) for r in rows]

    async def flag_cyclicals_at_peak(self) -> int:
        """
        Add WARNING flags to value screen results for cyclicals at peak earnings.

        This should be called after running value screens (3, 4, 5).

        Returns the number of results flagged.
        """
        # Get cyclicals at peak on value screens
        cyclicals_at_peak = await self.get_cyclicals_at_peak_on_value_screens()

        if not cyclicals_at_peak:
            return 0

        flagged_count = 0
        for row in cyclicals_at_peak:
            result_id = row['result_id']
            current_flags = row['flags'] or []
            ebitda_ratio = row['ebitda_vs_mid_cycle']

            # Build warning message
            warning = f"⚠️ CYCLICAL_AT_PEAK: {row['primary_ticker']} is classified as CYCLICAL " \
                      f"with EBITDA at {ebitda_ratio:.1f}x mid-cycle. Low P/E may be peak earnings, not value."

            # Add warning if not already present
            if warning not in current_flags:
                current_flags.insert(0, warning)  # Put warning first

                await self.conn.execute("""
                    UPDATE bsd_screen_results
                    SET flags = $1, updated_at = NOW()
                    WHERE id = $2
                """, current_flags, result_id)

                flagged_count += 1

        return flagged_count

    async def get_cyclical_warnings(self) -> List[Dict[str, Any]]:
        """
        Get all active results that have cyclical-at-peak warnings.

        This is a specialized warning endpoint for dangerous value traps.
        """
        rows = await self.conn.fetch("""
            SELECT
                sr.*,
                c.company_name,
                c.primary_ticker,
                cc.classification,
                cc.cycle_position,
                cc.mid_cycle_ebitda
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            LEFT JOIN bsd_company_classifications cc ON sr.company_id = cc.company_id
            WHERE sr.is_active = TRUE
              AND EXISTS (
                  SELECT 1 FROM unnest(sr.flags) AS flag
                  WHERE flag LIKE '%CYCLICAL_AT_PEAK%'
              )
            ORDER BY sr.triggered_at DESC
        """)
        return [dict(r) for r in rows]

    # =========================================================================
    # MAINTENANCE
    # =========================================================================

    async def expire_stale_results(self) -> int:
        """Deactivate results past their expiry date."""
        result = await self.conn.execute("""
            UPDATE bsd_screen_results
            SET is_active = FALSE, updated_at = NOW()
            WHERE expires_at < NOW() AND is_active = TRUE
        """)
        return int(result.split()[-1]) if result else 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics about screen results."""
        stats = await self.conn.fetchrow("""
            SELECT
                COUNT(*) AS total_results,
                COUNT(CASE WHEN is_active THEN 1 END) AS active_results,
                COUNT(DISTINCT company_id) AS unique_companies,
                COUNT(DISTINCT screen_type) AS screens_with_results,
                MAX(triggered_at) AS last_run
            FROM bsd_screen_results
        """)

        by_screen = await self.conn.fetch("""
            SELECT
                sd.screen_type,
                sd.short_name,
                COUNT(sr.id) AS result_count,
                AVG(sr.score) AS avg_score
            FROM bsd_screen_definitions sd
            LEFT JOIN bsd_screen_results sr
                ON sd.screen_type = sr.screen_type AND sr.is_active = TRUE
            GROUP BY sd.screen_type, sd.short_name
            ORDER BY sd.screen_type
        """)

        return {
            'total_results': stats['total_results'],
            'active_results': stats['active_results'],
            'unique_companies': stats['unique_companies'],
            'screens_with_results': stats['screens_with_results'],
            'last_run': stats['last_run'].isoformat() if stats['last_run'] else None,
            'by_screen': [dict(r) for r in by_screen]
        }
