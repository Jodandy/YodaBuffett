"""
Nordic Calendar Event Models
"""
from datetime import datetime, date, time
from typing import Optional
from sqlalchemy import String, Text, DateTime, Date, Time, Boolean, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from shared.database import Base


class NordicCalendarEvent(Base):
    """Nordic financial calendar events"""
    __tablename__ = "nordic_calendar_events"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'Q1_report', 'Q2_report', 'annual_report', 'agm', 'earnings_call'
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    event_time: Mapped[Optional[time]] = mapped_column(Time)  # Some companies specify time
    report_period: Mapped[Optional[str]] = mapped_column(String(50))  # 'Q1_2025', 'FY_2024'
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    location: Mapped[Optional[str]] = mapped_column(String(255))  # For AGMs, etc.
    webcast_url: Mapped[Optional[str]] = mapped_column(Text)
    source_url: Mapped[Optional[str]] = mapped_column(Text)  # Where we found this info
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False)  # true if from official source
    
    created_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    metadata_: Mapped[Optional[dict]] = mapped_column(JSON, name='metadata')
    
    # Relationships
    company: Mapped["NordicCompany"] = relationship("NordicCompany", back_populates="calendar_events")
    
    def __repr__(self):
        return f"<NordicCalendarEvent(company_id='{self.company_id}', type='{self.event_type}', date='{self.event_date}')>"
    
    @property
    def is_upcoming(self) -> bool:
        """Check if event is in the future"""
        return self.event_date >= date.today()
    
    @property
    def days_until_event(self) -> int:
        """Days until event (negative if past)"""
        return (self.event_date - date.today()).days