"""
Manual Collection Task Models
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Text, DateTime, Integer, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from shared.database import Base


class ManualCollectionTask(Base):
    """Manual collection tasks when automation fails"""
    __tablename__ = "manual_collection_tasks"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"), nullable=False)
    document_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'Q1', 'Q2', 'annual', etc.
    report_period: Mapped[str] = mapped_column(String(50), nullable=False)  # 'Q1_2025', 'FY_2024'
    priority: Mapped[str] = mapped_column(String(20), default='medium')  # 'urgent', 'high', 'medium', 'low'
    status: Mapped[str] = mapped_column(String(20), default='pending')  # 'pending', 'in_progress', 'completed', 'failed'
    
    # Failure information
    failed_methods: Mapped[Optional[dict]] = mapped_column(JSON)  # Which collection methods failed
    failure_reason: Mapped[str] = mapped_column(Text, nullable=False)
    error_details: Mapped[Optional[str]] = mapped_column(Text)
    
    # Task details
    expected_url: Mapped[Optional[str]] = mapped_column(Text)
    manual_steps: Mapped[Optional[str]] = mapped_column(Text)
    deadline: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # GitHub integration
    github_issue_number: Mapped[Optional[int]] = mapped_column(Integer)
    github_issue_url: Mapped[Optional[str]] = mapped_column(Text)
    
    # Assignment and completion
    assigned_to: Mapped[Optional[str]] = mapped_column(String(100))
    completed_by: Mapped[Optional[str]] = mapped_column(String(100))
    completion_notes: Mapped[Optional[str]] = mapped_column(Text)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Relationships
    company: Mapped["NordicCompany"] = relationship("NordicCompany", back_populates="manual_tasks")
    
    def __repr__(self):
        return f"<ManualCollectionTask(company_id='{self.company_id}', type='{self.document_type}', status='{self.status}')>"
    
    @property
    def is_overdue(self) -> bool:
        """Check if task is overdue"""
        if not self.deadline:
            return False
        return datetime.utcnow() > self.deadline and self.status in ['pending', 'in_progress']
    
    def mark_completed(self, completed_by: str, notes: Optional[str] = None):
        """Mark task as completed"""
        self.status = 'completed'
        self.completed_by = completed_by
        self.completion_notes = notes
        self.completed_at = datetime.utcnow()
    
    def mark_failed(self, reason: str):
        """Mark task as failed"""
        self.status = 'failed'
        self.completion_notes = reason
        self.completed_at = datetime.utcnow()