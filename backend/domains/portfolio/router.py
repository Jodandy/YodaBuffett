"""
Portfolio API endpoints for the Hub app.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime
from uuid import UUID
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://yodabuffett:password@localhost:5432/yodabuffett"
)


# --- Pydantic Models ---


class CreatePortfolioRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    currency: str = Field(default="SEK", max_length=10)


class UpdatePortfolioRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = None
    currency: Optional[str] = Field(None, max_length=10)


class AddHoldingRequest(BaseModel):
    company_id: Optional[UUID] = None
    symbol: str = Field(..., max_length=30)
    company_name: Optional[str] = Field(None, max_length=200)
    quantity: float = Field(..., gt=0)
    purchase_price: float = Field(..., ge=0)
    purchase_date: date
    currency: str = Field(default="SEK", max_length=10)
    notes: Optional[str] = None


class UpdateHoldingRequest(BaseModel):
    quantity: Optional[float] = Field(None, gt=0)
    purchase_price: Optional[float] = Field(None, ge=0)
    purchase_date: Optional[date] = None
    notes: Optional[str] = None


class HoldingResponse(BaseModel):
    id: UUID
    portfolio_id: UUID
    company_id: Optional[UUID]
    symbol: str
    company_name: Optional[str]
    quantity: float
    purchase_price: float
    purchase_date: date
    currency: str
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime
    # Computed from view
    current_price: Optional[float] = None
    current_value: Optional[float] = None
    cost_basis: Optional[float] = None
    gain_loss: Optional[float] = None
    gain_loss_percent: Optional[float] = None


class PortfolioResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str]
    currency: str
    is_active: bool
    is_default: bool
    created_at: datetime
    updated_at: datetime
    holdings: list[HoldingResponse] = []
    # Computed totals
    total_value: Optional[float] = None
    total_cost: Optional[float] = None
    total_gain_loss: Optional[float] = None
    total_gain_loss_percent: Optional[float] = None


class PortfolioListResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str]
    currency: str
    created_at: datetime
    holdings_count: int = 0
    total_value: Optional[float] = None
    total_gain_loss: Optional[float] = None
    total_gain_loss_percent: Optional[float] = None


# --- Helper Functions ---


async def get_db():
    """Get database connection."""
    return await asyncpg.connect(DATABASE_URL)


def row_to_holding(row: asyncpg.Record) -> HoldingResponse:
    """Convert database row to HoldingResponse."""
    return HoldingResponse(
        id=row["id"],
        portfolio_id=row["portfolio_id"],
        company_id=row.get("company_id"),
        symbol=row["symbol"],
        company_name=row.get("company_name"),
        quantity=float(row["quantity"]),
        purchase_price=float(row["purchase_price"]),
        purchase_date=row["purchase_date"],
        currency=row["currency"],
        notes=row.get("notes"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        current_price=float(row["current_price"]) if row.get("current_price") else None,
        current_value=float(row["current_value"]) if row.get("current_value") else None,
        cost_basis=float(row["cost_basis"]) if row.get("cost_basis") else None,
        gain_loss=float(row["gain_loss"]) if row.get("gain_loss") else None,
        gain_loss_percent=float(row["gain_loss_percent"]) if row.get("gain_loss_percent") else None,
    )


# --- Portfolio Endpoints ---


@router.get("", response_model=list[PortfolioListResponse])
async def list_portfolios():
    """List all portfolios with summary stats."""
    conn = await get_db()
    try:
        rows = await conn.fetch("""
            SELECT
                p.id,
                p.name,
                p.description,
                p.currency,
                p.created_at,
                COUNT(h.id) as holdings_count,
                SUM(h.current_value) as total_value,
                SUM(h.gain_loss) as total_gain_loss,
                CASE
                    WHEN SUM(h.cost_basis) > 0
                    THEN (SUM(h.gain_loss) / SUM(h.cost_basis) * 100)
                    ELSE 0
                END as total_gain_loss_percent
            FROM portfolios p
            LEFT JOIN portfolio_holdings_with_prices h ON h.portfolio_id = p.id
            WHERE p.is_active = true
            GROUP BY p.id
            ORDER BY p.created_at DESC
        """)

        return [
            PortfolioListResponse(
                id=row["id"],
                name=row["name"],
                description=row.get("description"),
                currency=row["currency"],
                created_at=row["created_at"],
                holdings_count=row["holdings_count"] or 0,
                total_value=float(row["total_value"]) if row["total_value"] else None,
                total_gain_loss=float(row["total_gain_loss"]) if row["total_gain_loss"] else None,
                total_gain_loss_percent=float(row["total_gain_loss_percent"]) if row["total_gain_loss_percent"] else None,
            )
            for row in rows
        ]
    finally:
        await conn.close()


@router.get("/{portfolio_id}", response_model=PortfolioResponse)
async def get_portfolio(portfolio_id: UUID):
    """Get a single portfolio with all holdings."""
    conn = await get_db()
    try:
        # Get portfolio
        portfolio = await conn.fetchrow(
            "SELECT * FROM portfolios WHERE id = $1 AND is_active = true",
            portfolio_id
        )
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found")

        # Get holdings with current prices
        holdings = await conn.fetch(
            "SELECT * FROM portfolio_holdings_with_prices WHERE portfolio_id = $1 ORDER BY symbol",
            portfolio_id
        )

        holding_responses = [row_to_holding(h) for h in holdings]

        # Calculate totals
        total_value = sum(h.current_value or 0 for h in holding_responses)
        total_cost = sum(h.cost_basis or 0 for h in holding_responses)
        total_gain_loss = sum(h.gain_loss or 0 for h in holding_responses)
        total_gain_loss_percent = (total_gain_loss / total_cost * 100) if total_cost > 0 else 0

        return PortfolioResponse(
            id=portfolio["id"],
            name=portfolio["name"],
            description=portfolio.get("description"),
            currency=portfolio["currency"],
            is_active=portfolio["is_active"],
            is_default=portfolio["is_default"],
            created_at=portfolio["created_at"],
            updated_at=portfolio["updated_at"],
            holdings=holding_responses,
            total_value=total_value if holding_responses else None,
            total_cost=total_cost if holding_responses else None,
            total_gain_loss=total_gain_loss if holding_responses else None,
            total_gain_loss_percent=total_gain_loss_percent if holding_responses else None,
        )
    finally:
        await conn.close()


@router.post("", response_model=PortfolioResponse, status_code=201)
async def create_portfolio(request: CreatePortfolioRequest):
    """Create a new portfolio."""
    conn = await get_db()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO portfolios (name, description, currency)
            VALUES ($1, $2, $3)
            RETURNING *
            """,
            request.name,
            request.description,
            request.currency,
        )

        return PortfolioResponse(
            id=row["id"],
            name=row["name"],
            description=row.get("description"),
            currency=row["currency"],
            is_active=row["is_active"],
            is_default=row["is_default"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            holdings=[],
        )
    finally:
        await conn.close()


@router.patch("/{portfolio_id}", response_model=PortfolioResponse)
async def update_portfolio(portfolio_id: UUID, request: UpdatePortfolioRequest):
    """Update a portfolio."""
    conn = await get_db()
    try:
        # Build dynamic update
        updates = []
        values = []
        param_idx = 1

        if request.name is not None:
            updates.append(f"name = ${param_idx}")
            values.append(request.name)
            param_idx += 1

        if request.description is not None:
            updates.append(f"description = ${param_idx}")
            values.append(request.description)
            param_idx += 1

        if request.currency is not None:
            updates.append(f"currency = ${param_idx}")
            values.append(request.currency)
            param_idx += 1

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        values.append(portfolio_id)
        query = f"""
            UPDATE portfolios
            SET {', '.join(updates)}
            WHERE id = ${param_idx} AND is_active = true
            RETURNING *
        """

        row = await conn.fetchrow(query, *values)
        if not row:
            raise HTTPException(status_code=404, detail="Portfolio not found")

        # Get updated portfolio with holdings
        return await get_portfolio(portfolio_id)
    finally:
        await conn.close()


@router.delete("/{portfolio_id}", status_code=204)
async def delete_portfolio(portfolio_id: UUID):
    """Delete a portfolio (soft delete)."""
    conn = await get_db()
    try:
        result = await conn.execute(
            "UPDATE portfolios SET is_active = false WHERE id = $1 AND is_active = true",
            portfolio_id
        )
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Portfolio not found")
    finally:
        await conn.close()


# --- Holdings Endpoints ---


@router.post("/{portfolio_id}/holdings", response_model=HoldingResponse, status_code=201)
async def add_holding(portfolio_id: UUID, request: AddHoldingRequest):
    """Add a holding to a portfolio."""
    conn = await get_db()
    try:
        # Verify portfolio exists
        portfolio = await conn.fetchrow(
            "SELECT id FROM portfolios WHERE id = $1 AND is_active = true",
            portfolio_id
        )
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found")

        # Insert holding
        row = await conn.fetchrow(
            """
            INSERT INTO portfolio_holdings
                (portfolio_id, company_id, symbol, company_name, quantity, purchase_price, purchase_date, currency, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            """,
            portfolio_id,
            request.company_id,
            request.symbol.upper(),
            request.company_name,
            request.quantity,
            request.purchase_price,
            request.purchase_date,
            request.currency,
            request.notes,
        )

        # Get full holding with computed fields
        holding = await conn.fetchrow(
            "SELECT * FROM portfolio_holdings_with_prices WHERE id = $1",
            row["id"]
        )

        return row_to_holding(holding)
    finally:
        await conn.close()


@router.patch("/holdings/{holding_id}", response_model=HoldingResponse)
async def update_holding(holding_id: UUID, request: UpdateHoldingRequest):
    """Update a holding."""
    conn = await get_db()
    try:
        # Build dynamic update
        updates = []
        values = []
        param_idx = 1

        if request.quantity is not None:
            updates.append(f"quantity = ${param_idx}")
            values.append(request.quantity)
            param_idx += 1

        if request.purchase_price is not None:
            updates.append(f"purchase_price = ${param_idx}")
            values.append(request.purchase_price)
            param_idx += 1

        if request.purchase_date is not None:
            updates.append(f"purchase_date = ${param_idx}")
            values.append(request.purchase_date)
            param_idx += 1

        if request.notes is not None:
            updates.append(f"notes = ${param_idx}")
            values.append(request.notes)
            param_idx += 1

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        values.append(holding_id)
        query = f"""
            UPDATE portfolio_holdings
            SET {', '.join(updates)}
            WHERE id = ${param_idx}
            RETURNING id
        """

        row = await conn.fetchrow(query, *values)
        if not row:
            raise HTTPException(status_code=404, detail="Holding not found")

        # Get updated holding with computed fields
        holding = await conn.fetchrow(
            "SELECT * FROM portfolio_holdings_with_prices WHERE id = $1",
            holding_id
        )

        return row_to_holding(holding)
    finally:
        await conn.close()


@router.delete("/holdings/{holding_id}", status_code=204)
async def delete_holding(holding_id: UUID):
    """Delete a holding."""
    conn = await get_db()
    try:
        result = await conn.execute(
            "DELETE FROM portfolio_holdings WHERE id = $1",
            holding_id
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Holding not found")
    finally:
        await conn.close()


# --- Stock Search Endpoint ---


@router.get("/stocks/search")
async def search_stocks(q: str, limit: int = 10):
    """Search stocks from company_master for autocomplete."""
    if len(q) < 1:
        return []

    conn = await get_db()
    try:
        rows = await conn.fetch(
            """
            SELECT
                id,
                company_name,
                primary_ticker as symbol,
                yahoo_symbol,
                sector,
                country
            FROM company_master
            WHERE
                primary_ticker ILIKE $1
                OR company_name ILIKE $1
                OR yahoo_symbol ILIKE $1
            ORDER BY
                CASE WHEN primary_ticker ILIKE $2 THEN 0 ELSE 1 END,
                company_name
            LIMIT $3
            """,
            f"%{q}%",
            f"{q}%",
            limit,
        )

        return [
            {
                "id": str(row["id"]),
                "symbol": row["symbol"] or row["yahoo_symbol"],
                "name": row["company_name"],
                "sector": row.get("sector"),
                "country": row.get("country"),
            }
            for row in rows
        ]
    finally:
        await conn.close()
