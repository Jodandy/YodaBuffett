"""
SQLAlchemy models for screener functionality
"""
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, Date
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PGUUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class ScreenerQuery(Base):
    """Saved screening queries"""
    __tablename__ = "screener_queries"
    
    id: UUID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: str = Column(String(255), nullable=False)
    description: Optional[str] = Column(Text)
    query_json: Dict[str, Any] = Column(JSONB, nullable=False)
    user_id: Optional[UUID] = Column(PGUUID(as_uuid=True))  # Will link to user system
    is_public: bool = Column(Boolean, default=False)
    tags: List[str] = Column(ARRAY(String), default=[])
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: datetime = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    results = relationship("ScreenerResult", back_populates="query", cascade="all, delete-orphan")
    backtests = relationship("BacktestRun", back_populates="query", cascade="all, delete-orphan")
    user_screens = relationship("UserScreen", back_populates="query", cascade="all, delete-orphan")


class ScreenerResult(Base):
    """Cached screening results"""
    __tablename__ = "screener_results"
    
    id: UUID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    query_id: UUID = Column(PGUUID(as_uuid=True), ForeignKey("screener_queries.id", ondelete="CASCADE"), nullable=False)
    as_of_date: datetime = Column(Date, nullable=False)
    results_json: Dict[str, Any] = Column(JSONB, nullable=False)
    summary_json: Dict[str, Any] = Column(JSONB, nullable=False)
    execution_time_ms: int = Column(Integer, nullable=False)
    total_matches: int = Column(Integer, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    query = relationship("ScreenerQuery", back_populates="results")


class BacktestRun(Base):
    """Backtesting execution history"""
    __tablename__ = "backtest_runs"
    
    id: UUID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    query_id: UUID = Column(PGUUID(as_uuid=True), ForeignKey("screener_queries.id", ondelete="CASCADE"), nullable=False)
    start_date: datetime = Column(Date, nullable=False)
    end_date: datetime = Column(Date, nullable=False)
    frequency: str = Column(String(20), nullable=False)  # daily, weekly, monthly
    forward_periods: List[str] = Column(ARRAY(String), nullable=False)
    results_json: Dict[str, Any] = Column(JSONB, nullable=False)
    summary_json: Dict[str, Any] = Column(JSONB, nullable=False)
    total_execution_time_ms: int = Column(Integer, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    query = relationship("ScreenerQuery", back_populates="backtests")


class UserScreen(Base):
    """User preferences for saved screens"""
    __tablename__ = "user_screens"
    
    id: UUID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: UUID = Column(PGUUID(as_uuid=True), nullable=False)
    query_id: UUID = Column(PGUUID(as_uuid=True), ForeignKey("screener_queries.id", ondelete="CASCADE"), nullable=False)
    is_favorite: bool = Column(Boolean, default=False)
    custom_name: Optional[str] = Column(String(255))
    last_run_at: Optional[datetime] = Column(DateTime(timezone=True))
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    query = relationship("ScreenerQuery", back_populates="user_screens")


class MetricDefinition(Base):
    """Available metrics for screening"""
    __tablename__ = "metric_definitions"
    
    id: str = Column(String(100), primary_key=True)
    name: str = Column(String(255), nullable=False)
    description: str = Column(Text, nullable=False)
    category: str = Column(String(50), nullable=False)  # fundamental, technical, derived, market
    data_type: str = Column(String(20), nullable=False)  # number, percentage, ratio, currency
    unit: Optional[str] = Column(String(20))
    is_relative: bool = Column(Boolean, default=False)
    source_table: Optional[str] = Column(String(100))
    source_column: Optional[str] = Column(String(100))
    calculation_method: Optional[str] = Column(Text)
    is_active: bool = Column(Boolean, default=True)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())