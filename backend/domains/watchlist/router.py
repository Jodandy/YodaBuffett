"""
Watchlist API Router

REST API endpoints for managing screen watchlists.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from uuid import UUID
import asyncpg

router = APIRouter(prefix="/watchlists", tags=["Watchlists"])

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


# === Pydantic Models ===

class WatchlistCreate(BaseModel):
    name: str
    description: Optional[str] = None


class WatchlistUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class CompanyAdd(BaseModel):
    tickers: List[str]
    source: Optional[str] = None


class WatchlistItemResponse(BaseModel):
    id: str
    company_id: str
    ticker: str
    company_name: str
    added_at: datetime
    source: Optional[str] = None
    notes: Optional[str] = None
    current_price: Optional[float] = None
    price_when_added: Optional[float] = None
    return_pct: Optional[float] = None


class WatchlistResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    company_count: int


class WatchlistDetailResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    companies: List[WatchlistItemResponse]


# === Dependency ===

async def get_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


# === Endpoints ===

@router.get("", response_model=List[WatchlistResponse])
async def list_watchlists(conn: asyncpg.Connection = Depends(get_db_connection)):
    """List all watchlists with company counts."""
    rows = await conn.fetch("""
        SELECT sw.id, sw.name, sw.description, sw.created_at, sw.updated_at,
               COUNT(swi.id) as company_count
        FROM screen_watchlists sw
        LEFT JOIN screen_watchlist_items swi ON sw.id = swi.watchlist_id
        GROUP BY sw.id
        ORDER BY sw.updated_at DESC
    """)
    
    return [WatchlistResponse(
        id=str(row['id']),
        name=row['name'],
        description=row['description'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
        company_count=row['company_count'],
    ) for row in rows]


@router.post("", response_model=WatchlistResponse)
async def create_watchlist(
    data: WatchlistCreate,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Create a new watchlist."""
    try:
        row = await conn.fetchrow("""
            INSERT INTO screen_watchlists (name, description)
            VALUES ($1, $2)
            RETURNING id, name, description, created_at, updated_at
        """, data.name, data.description)
        
        return WatchlistResponse(
            id=str(row['id']),
            name=row['name'],
            description=row['description'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            company_count=0,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=400, detail=f"Watchlist '{data.name}' already exists")


@router.get("/{watchlist_id}", response_model=WatchlistDetailResponse)
async def get_watchlist(
    watchlist_id: str,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Get watchlist details with all companies and their returns."""
    # Get watchlist
    watchlist = await conn.fetchrow("""
        SELECT id, name, description, created_at, updated_at
        FROM screen_watchlists WHERE id = $1
    """, watchlist_id)
    
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    
    # Get companies with prices
    items = await conn.fetch("""
        SELECT 
            swi.id,
            swi.company_id,
            cm.primary_ticker as ticker,
            cm.company_name,
            swi.added_at,
            swi.source,
            swi.notes,
            (SELECT close_price FROM daily_price_data 
             WHERE symbol = cm.primary_ticker 
             ORDER BY date DESC LIMIT 1) as current_price,
            (SELECT close_price FROM daily_price_data 
             WHERE symbol = cm.primary_ticker AND date <= swi.added_at::date
             ORDER BY date DESC LIMIT 1) as price_when_added
        FROM screen_watchlist_items swi
        JOIN company_master cm ON swi.company_id = cm.id
        WHERE swi.watchlist_id = $1
        ORDER BY swi.added_at DESC
    """, watchlist_id)
    
    companies = []
    for item in items:
        current = float(item['current_price']) if item['current_price'] else None
        added = float(item['price_when_added']) if item['price_when_added'] else None
        return_pct = ((current - added) / added) if current and added else None
        
        companies.append(WatchlistItemResponse(
            id=str(item['id']),
            company_id=str(item['company_id']),
            ticker=item['ticker'],
            company_name=item['company_name'],
            added_at=item['added_at'],
            source=item['source'],
            notes=item['notes'],
            current_price=current,
            price_when_added=added,
            return_pct=return_pct,
        ))
    
    return WatchlistDetailResponse(
        id=str(watchlist['id']),
        name=watchlist['name'],
        description=watchlist['description'],
        created_at=watchlist['created_at'],
        updated_at=watchlist['updated_at'],
        companies=companies,
    )


@router.put("/{watchlist_id}", response_model=WatchlistResponse)
async def update_watchlist(
    watchlist_id: str,
    data: WatchlistUpdate,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Update watchlist name or description."""
    # Build update query dynamically
    updates = []
    params = []
    param_idx = 1
    
    if data.name is not None:
        updates.append(f"name = ${param_idx}")
        params.append(data.name)
        param_idx += 1
    
    if data.description is not None:
        updates.append(f"description = ${param_idx}")
        params.append(data.description)
        param_idx += 1
    
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")
    
    updates.append("updated_at = NOW()")
    params.append(watchlist_id)
    
    query = f"""
        UPDATE screen_watchlists
        SET {', '.join(updates)}
        WHERE id = ${param_idx}
        RETURNING id, name, description, created_at, updated_at
    """
    
    row = await conn.fetchrow(query, *params)
    
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    
    # Get company count
    count = await conn.fetchval(
        "SELECT COUNT(*) FROM screen_watchlist_items WHERE watchlist_id = $1",
        watchlist_id
    )
    
    return WatchlistResponse(
        id=str(row['id']),
        name=row['name'],
        description=row['description'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
        company_count=count,
    )


@router.delete("/{watchlist_id}")
async def delete_watchlist(
    watchlist_id: str,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Delete a watchlist and all its items."""
    result = await conn.execute(
        "DELETE FROM screen_watchlists WHERE id = $1",
        watchlist_id
    )
    
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Watchlist not found")
    
    return {"message": "Watchlist deleted"}


@router.post("/{watchlist_id}/companies")
async def add_companies(
    watchlist_id: str,
    data: CompanyAdd,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Add companies to a watchlist by ticker."""
    # Verify watchlist exists
    watchlist = await conn.fetchval(
        "SELECT id FROM screen_watchlists WHERE id = $1",
        watchlist_id
    )
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    
    added = 0
    skipped = 0
    not_found = []
    
    for ticker in data.tickers:
        ticker = ticker.strip()
        ticker_space = ticker.replace('-', ' ')
        
        # Find company
        company = await conn.fetchrow("""
            SELECT id FROM company_master
            WHERE primary_ticker = $1 OR primary_ticker = $2
        """, ticker, ticker_space)
        
        if not company:
            not_found.append(ticker)
            continue
        
        # Add to watchlist
        try:
            await conn.execute("""
                INSERT INTO screen_watchlist_items (watchlist_id, company_id, source)
                VALUES ($1, $2, $3)
            """, watchlist_id, company['id'], data.source)
            added += 1
        except asyncpg.UniqueViolationError:
            skipped += 1
    
    # Update watchlist modified date
    if added > 0:
        await conn.execute(
            "UPDATE screen_watchlists SET updated_at = NOW() WHERE id = $1",
            watchlist_id
        )
    
    return {
        "added": added,
        "skipped": skipped,
        "not_found": not_found,
    }


@router.delete("/{watchlist_id}/companies/{ticker}")
async def remove_company(
    watchlist_id: str,
    ticker: str,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Remove a company from a watchlist."""
    ticker_space = ticker.replace('-', ' ')
    
    result = await conn.execute("""
        DELETE FROM screen_watchlist_items
        WHERE watchlist_id = $1
          AND company_id = (
              SELECT id FROM company_master
              WHERE primary_ticker = $2 OR primary_ticker = $3
          )
    """, watchlist_id, ticker, ticker_space)
    
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Company not found in watchlist")
    
    # Update watchlist modified date
    await conn.execute(
        "UPDATE screen_watchlists SET updated_at = NOW() WHERE id = $1",
        watchlist_id
    )
    
    return {"message": f"Removed {ticker} from watchlist"}
