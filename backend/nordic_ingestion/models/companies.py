"""
Nordic Company Models
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Text, Integer, DateTime, JSON, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from shared.database import Base


class NordicCompany(Base):
    """Nordic company registry"""
    __tablename__ = "nordic_companies"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    ticker: Mapped[Optional[str]] = mapped_column(String(50))
    exchange: Mapped[Optional[str]] = mapped_column(String(50))  # 'OMXS30', 'Oslo Bors', etc.
    country: Mapped[str] = mapped_column(String(2), nullable=False)  # 'SE', 'NO', 'DK', 'FI'
    market_cap_category: Mapped[Optional[str]] = mapped_column(String(50))  # 'Large', 'Mid', 'Small'
    sector: Mapped[Optional[str]] = mapped_column(String(100))
    ir_email: Mapped[Optional[str]] = mapped_column(String(255))
    ir_website: Mapped[Optional[str]] = mapped_column(Text)
    website: Mapped[Optional[str]] = mapped_column(Text)
    reporting_language: Mapped[str] = mapped_column(String(10), default='sv')
    metadata_: Mapped[Optional[dict]] = mapped_column(JSON, name='metadata')
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    data_sources: Mapped[list["NordicDataSource"]] = relationship("NordicDataSource", back_populates="company")
    documents: Mapped[list["NordicDocument"]] = relationship("NordicDocument", back_populates="company")
    calendar_events: Mapped[list["NordicCalendarEvent"]] = relationship("NordicCalendarEvent", back_populates="company")
    manual_tasks: Mapped[list["ManualCollectionTask"]] = relationship("ManualCollectionTask", back_populates="company")
    
    def __repr__(self):
        return f"<NordicCompany(name='{self.name}', ticker='{self.ticker}', country='{self.country}')>"


class NordicDataSource(Base):
    """Nordic company data sources configuration"""
    __tablename__ = "nordic_data_sources"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'rss_feed', 'ir_email', 'web_scraping'
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[Optional[dict]] = mapped_column(JSON)  # RSS URL, email patterns, scraping selectors
    status: Mapped[str] = mapped_column(String(50), default='active')  # 'active', 'broken', 'rate_limited'
    last_success: Mapped[Optional[datetime]] = mapped_column(DateTime)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    company: Mapped["NordicCompany"] = relationship("NordicCompany", back_populates="data_sources")
    
    def __repr__(self):
        return f"<NordicDataSource(company_id='{self.company_id}', type='{self.source_type}', status='{self.status}')>"