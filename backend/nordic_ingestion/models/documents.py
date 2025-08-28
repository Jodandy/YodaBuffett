"""
Nordic Document Models
"""
from datetime import datetime
from typing import Optional
from decimal import Decimal
from sqlalchemy import String, Text, Integer, DateTime, JSON, ForeignKey, DECIMAL
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from shared.database import Base


class NordicDocument(Base):
    """Nordic financial reports and news"""
    __tablename__ = "nordic_documents"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"), nullable=False)
    document_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'Q1', 'Q2', 'Q3', 'annual', 'press_release'
    report_period: Mapped[str] = mapped_column(String(50), nullable=False)  # 'Q1_2024', 'FY_2024', '2024-01-15'
    title: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text)
    storage_path: Mapped[Optional[str]] = mapped_column(Text)  # S3 path or local path
    file_hash: Mapped[Optional[str]] = mapped_column(String(64))  # SHA256 for deduplication
    language: Mapped[str] = mapped_column(String(10), default='sv')  # 'sv', 'no', 'da', 'fi', 'en'
    
    ingestion_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    processing_status: Mapped[str] = mapped_column(String(50), default='pending')  # 'pending', 'extracted', 'indexed', 'failed'
    page_count: Mapped[Optional[int]] = mapped_column(Integer)
    file_size_mb: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(10, 2))
    metadata_: Mapped[Optional[dict]] = mapped_column(JSON, name='metadata')  # extraction results, key metrics
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    company: Mapped["NordicCompany"] = relationship("NordicCompany", back_populates="documents")
    
    def __repr__(self):
        return f"<NordicDocument(company_id='{self.company_id}', type='{self.document_type}', period='{self.report_period}')>"


class NordicIngestionLog(Base):
    """Nordic ingestion logs"""
    __tablename__ = "nordic_ingestion_logs"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_data_sources.id"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    collection_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'scheduled', 'manual', 'retry'
    status: Mapped[str] = mapped_column(String(50), nullable=False)  # 'success', 'partial', 'failed'
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    reports_found: Mapped[int] = mapped_column(Integer, default=0)
    reports_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    news_items_found: Mapped[int] = mapped_column(Integer, default=0)
    processing_time_seconds: Mapped[Optional[int]] = mapped_column(Integer)
    
    def __repr__(self):
        return f"<NordicIngestionLog(source_id='{self.source_id}', status='{self.status}', timestamp='{self.timestamp}')>"