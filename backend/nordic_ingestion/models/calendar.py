"""
Nordic Calendar Event Models
"""
from datetime import datetime, date, time
from typing import Optional
from sqlalchemy import String, Text, DateTime, Date, Time, Boolean, ForeignKey, JSON, Numeric
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from shared.database import Base


class NordicCalendarEvent(Base):
    """Nordic financial calendar events with dividend support"""
    __tablename__ = "nordic_calendar_events"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'dividend', 'earnings', 'agm', 'egm'
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    event_time: Mapped[Optional[time]] = mapped_column(Time)  # Some companies specify time
    report_period: Mapped[Optional[str]] = mapped_column(String(50))  # 'Q1_2025', 'FY_2024'
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    location: Mapped[Optional[str]] = mapped_column(String(255))  # For AGMs, etc.
    webcast_url: Mapped[Optional[str]] = mapped_column(Text)
    source_url: Mapped[Optional[str]] = mapped_column(Text)  # Where we found this info
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False)  # true if from official source
    
    # Dividend-specific fields (Swedish X-dag support)
    dividend_amount: Mapped[Optional[Numeric]] = mapped_column(Numeric(10, 4))  # Amount per share (e.g., 2.5000)
    dividend_currency: Mapped[Optional[str]] = mapped_column(String(3))  # SEK, EUR, USD
    dividend_type: Mapped[Optional[str]] = mapped_column(String(20))  # 'regular', 'special', 'interim', 'extra'
    ex_dividend_date: Mapped[Optional[date]] = mapped_column(Date)  # Ex-dag (last day to buy for dividend)
    record_date: Mapped[Optional[date]] = mapped_column(Date)  # Avstämningsdag (record date)
    payment_date: Mapped[Optional[date]] = mapped_column(Date)  # Utbetalningsdag (payment date)
    
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
    
    @property
    def is_dividend_event(self) -> bool:
        """Check if this is a dividend-related event"""
        return self.event_type == 'dividend' and self.dividend_amount is not None
    
    @property
    def dividend_amount_formatted(self) -> Optional[str]:
        """Format dividend amount with currency"""
        if self.dividend_amount and self.dividend_currency:
            return f"{float(self.dividend_amount):.4f} {self.dividend_currency}"
        return None
    
    @property
    def dividend_timeline(self) -> dict:
        """Get complete dividend timeline (Swedish X-dag dates)"""
        timeline = {}
        if self.ex_dividend_date:
            timeline['ex_dividend'] = self.ex_dividend_date
        if self.record_date:
            timeline['record'] = self.record_date  # Avstämningsdag
        if self.payment_date:
            timeline['payment'] = self.payment_date  # Utbetalningsdag
        return timeline