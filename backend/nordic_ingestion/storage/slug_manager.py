"""
MFN Slug Management - Store and retrieve successful slugs in company metadata
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func
from typing import Optional
import json
from datetime import datetime

from ..models import NordicCompany

class SlugManager:
    """Manages MFN slug storage and retrieval from company metadata"""
    
    @staticmethod
    async def get_stored_slug(db: AsyncSession, company_id: str) -> Optional[str]:
        """Get stored MFN slug from company metadata"""
        result = await db.execute(
            select(NordicCompany.metadata_).where(NordicCompany.id == company_id)
        )
        metadata = result.scalar_one_or_none()
        
        if metadata and isinstance(metadata, dict):
            return metadata.get('mfn_slug')
        return None
    
    @staticmethod
    async def store_successful_slug(db: AsyncSession, company_id: str, mfn_slug: str) -> None:
        """Store successful MFN slug in company metadata"""
        try:
            # Get current metadata
            result = await db.execute(
                select(NordicCompany).where(NordicCompany.id == company_id)
            )
            company = result.scalar_one_or_none()
            
            if not company:
                print(f"⚠️ Company {company_id} not found for slug storage")
                return
            
            # Update metadata with slug
            current_metadata = company.metadata_ or {}
            current_metadata['mfn_slug'] = mfn_slug
            current_metadata['mfn_slug_verified'] = datetime.utcnow().isoformat()
            
            # Update in database
            await db.execute(
                update(NordicCompany)
                .where(NordicCompany.id == company_id)
                .values(metadata_=current_metadata)
            )
            await db.commit()
            
            print(f"💾 Stored MFN slug '{mfn_slug}' for company {company.name}")
            
        except Exception as e:
            print(f"❌ Error storing slug: {e}")
            await db.rollback()
    
    @staticmethod
    async def get_slug_for_company_name(db: AsyncSession, company_name: str) -> Optional[str]:
        """Get stored MFN slug by company name"""
        result = await db.execute(
            select(NordicCompany.metadata_).where(
                func.lower(NordicCompany.name) == func.lower(company_name)
            ).order_by(NordicCompany.created_at).limit(1)
        )
        metadata = result.scalar_one_or_none()
        
        if metadata and isinstance(metadata, dict):
            return metadata.get('mfn_slug')
        return None

