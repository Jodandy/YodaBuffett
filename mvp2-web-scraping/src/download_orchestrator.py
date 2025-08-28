#!/usr/bin/env python3

"""
Download Orchestrator - Multi-stage automated attempts with manual fallback
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable
from enum import Enum
import json
import os
import asyncio
from pathlib import Path


class DownloadMethod(Enum):
    RSS_NOTIFICATION = "rss_notification"
    DIRECT_HTTP = "direct_http"
    PLAYWRIGHT_BROWSER = "playwright_browser"
    MANUAL_FALLBACK = "manual_fallback"


class DownloadStatus(Enum):
    PENDING = "pending"
    ATTEMPTING = "attempting"
    SUCCESS = "success"
    FAILED = "failed"
    MANUAL_QUEUE = "manual_queue"
    COMPLETED = "completed"


@dataclass
class DownloadAttempt:
    method: DownloadMethod
    timestamp: datetime
    success: bool
    error_message: Optional[str] = None
    file_path: Optional[str] = None
    response_time: Optional[float] = None


@dataclass
class ReportDownloadTask:
    """Single report download task with multiple attempt strategies"""
    
    # Core identification
    company_name: str
    report_type: str  # 'Q1', 'Q2', 'Q3', 'Annual'
    year: int
    
    # Multiple URL sources to try
    direct_pdf_url: Optional[str] = None
    company_ir_page: Optional[str] = None
    rss_event_url: Optional[str] = None
    
    # Tracking
    status: DownloadStatus = DownloadStatus.PENDING
    priority: str = "medium"  # 'high', 'medium', 'low'
    created_date: datetime = field(default_factory=datetime.now)
    target_date: Optional[datetime] = None  # When report is expected
    
    # Attempts history
    attempts: List[DownloadAttempt] = field(default_factory=list)
    
    # Results
    final_file_path: Optional[str] = None
    manual_instructions: Optional[str] = None
    
    def add_attempt(self, method: DownloadMethod, success: bool, 
                   error_message: Optional[str] = None, 
                   file_path: Optional[str] = None):
        """Record a download attempt"""
        attempt = DownloadAttempt(
            method=method,
            timestamp=datetime.now(),
            success=success,
            error_message=error_message,
            file_path=file_path
        )
        self.attempts.append(attempt)
        
        if success:
            self.status = DownloadStatus.SUCCESS
            self.final_file_path = file_path
        
    def should_attempt_method(self, method: DownloadMethod) -> bool:
        """Check if we should try this method"""
        recent_attempts = [a for a in self.attempts 
                         if a.method == method and 
                         a.timestamp > datetime.now() - timedelta(hours=24)]
        return len(recent_attempts) < 2  # Max 2 attempts per day per method
    
    def get_manual_priority(self) -> int:
        """Calculate manual queue priority (lower = higher priority)"""
        priority_scores = {'high': 1, 'medium': 5, 'low': 10}
        base_score = priority_scores.get(self.priority, 5)
        
        # Boost priority if report is overdue
        if self.target_date and datetime.now() > self.target_date:
            days_overdue = (datetime.now() - self.target_date).days
            base_score -= days_overdue
            
        return max(1, base_score)


class DownloadOrchestrator:
    """
    Manages multi-stage download attempts with manual fallback queue
    """
    
    def __init__(self, output_dir: str = "data/managed_downloads"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.tasks: List[ReportDownloadTask] = []
        self.download_methods = {
            DownloadMethod.DIRECT_HTTP: self._try_direct_http,
            DownloadMethod.PLAYWRIGHT_BROWSER: self._try_playwright,
        }
        
    def add_task(self, company: str, report_type: str, year: int,
                 direct_pdf_url: Optional[str] = None,
                 company_ir_page: Optional[str] = None,
                 priority: str = "medium",
                 target_date: Optional[datetime] = None) -> ReportDownloadTask:
        """Add a new download task"""
        
        task = ReportDownloadTask(
            company_name=company,
            report_type=report_type,
            year=year,
            direct_pdf_url=direct_pdf_url,
            company_ir_page=company_ir_page,
            priority=priority,
            target_date=target_date
        )
        
        self.tasks.append(task)
        return task
    
    async def process_pending_tasks(self) -> Dict[str, int]:
        """Process all pending download tasks"""
        stats = {
            'attempted': 0,
            'succeeded': 0,
            'failed': 0,
            'queued_for_manual': 0
        }
        
        pending_tasks = [t for t in self.tasks if t.status == DownloadStatus.PENDING]
        
        for task in pending_tasks:
            print(f"🎯 Processing: {task.company_name} {task.report_type} {task.year}")
            
            task.status = DownloadStatus.ATTEMPTING
            success = False
            
            # Try each automated method
            for method, handler in self.download_methods.items():
                if not task.should_attempt_method(method):
                    continue
                    
                print(f"  Trying {method.value}...")
                
                try:
                    result = await handler(task)
                    task.add_attempt(method, result['success'], 
                                   result.get('error'), result.get('file_path'))
                    
                    if result['success']:
                        success = True
                        print(f"  ✅ Success with {method.value}")
                        break
                    else:
                        print(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    task.add_attempt(method, False, str(e))
                    print(f"  ❌ Exception: {e}")
                
                # Be polite between attempts
                await asyncio.sleep(2)
            
            # Update stats
            stats['attempted'] += 1
            if success:
                stats['succeeded'] += 1
            else:
                stats['failed'] += 1
                # Queue for manual processing
                task.status = DownloadStatus.MANUAL_QUEUE
                task.manual_instructions = self._generate_manual_instructions(task)
                stats['queued_for_manual'] += 1
        
        return stats
    
    async def _try_direct_http(self, task: ReportDownloadTask) -> Dict:
        """Try direct HTTP download"""
        if not task.direct_pdf_url:
            return {'success': False, 'error': 'No direct URL provided'}
        
        try:
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                }
                
                async with session.get(task.direct_pdf_url, headers=headers) as response:
                    if response.status != 200:
                        return {'success': False, 'error': f'HTTP {response.status}'}
                    
                    content = await response.read()
                    
                    # Check if it's actually a PDF
                    if not content.startswith(b'%PDF'):
                        return {'success': False, 'error': 'Response is not a PDF'}
                    
                    # Save file
                    filename = f"{task.company_name}_{task.year}_{task.report_type}.pdf"
                    file_path = self.output_dir / filename
                    
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    
                    return {
                        'success': True,
                        'file_path': str(file_path),
                        'size_mb': len(content) / 1024 / 1024
                    }
                    
        except ImportError:
            return {'success': False, 'error': 'aiohttp not available'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _try_playwright(self, task: ReportDownloadTask) -> Dict:
        """Try Playwright browser automation"""
        if not task.company_ir_page:
            return {'success': False, 'error': 'No IR page provided'}
        
        try:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                )
                page = await context.new_page()
                
                download_path = None
                
                async def handle_download(download):
                    nonlocal download_path
                    filename = f"{task.company_name}_{task.year}_{task.report_type}.pdf"
                    download_path = self.output_dir / filename
                    await download.save_as(download_path)
                
                page.on("download", handle_download)
                
                # Navigate to IR page
                await page.goto(task.company_ir_page, timeout=30000)
                await asyncio.sleep(3)
                
                # Look for recent report links
                report_keywords = [task.report_type.lower(), str(task.year), 'rapport', 'report']
                pdf_links = await page.locator('a[href*=".pdf"]').all()
                
                for link in pdf_links:
                    link_text = await link.text_content()
                    if any(keyword in link_text.lower() for keyword in report_keywords):
                        await link.click()
                        await asyncio.sleep(5)
                        break
                
                await browser.close()
                
                if download_path and download_path.exists():
                    return {
                        'success': True,
                        'file_path': str(download_path),
                        'size_mb': download_path.stat().st_size / 1024 / 1024
                    }
                else:
                    return {'success': False, 'error': 'No download occurred'}
                    
        except ImportError:
            return {'success': False, 'error': 'Playwright not available'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _generate_manual_instructions(self, task: ReportDownloadTask) -> str:
        """Generate step-by-step manual download instructions"""
        
        failed_methods = [a.method.value for a in task.attempts if not a.success]
        
        instructions = f"""
📋 MANUAL DOWNLOAD REQUIRED

🏢 Company: {task.company_name}
📊 Report: {task.report_type} {task.year}
⚠️  Failed Methods: {', '.join(failed_methods)}

📝 STEPS:
1. Visit IR page: {task.company_ir_page or 'Google: ' + task.company_name + ' investor relations'}
2. Look for {task.report_type} {task.year} report
3. Download PDF manually
4. Save as: {task.company_name}_{task.year}_{task.report_type}.pdf
5. Save to: {self.output_dir}
6. Run: python complete_manual.py "{task.company_name}" "{task.report_type}" {task.year}

🕒 Priority: {task.priority.upper()}
📅 Target Date: {task.target_date.strftime('%Y-%m-%d') if task.target_date else 'Unknown'}
"""
        
        return instructions
    
    def get_manual_queue(self) -> List[ReportDownloadTask]:
        """Get tasks that need manual intervention, sorted by priority"""
        manual_tasks = [t for t in self.tasks if t.status == DownloadStatus.MANUAL_QUEUE]
        return sorted(manual_tasks, key=lambda x: x.get_manual_priority())
    
    def generate_manual_work_list(self) -> str:
        """Generate a prioritized manual work list"""
        manual_tasks = self.get_manual_queue()
        
        if not manual_tasks:
            return "🎉 No manual downloads needed!"
        
        work_list = f"""
📋 MANUAL DOWNLOAD QUEUE - {datetime.now().strftime('%Y-%m-%d %H:%M')}
{'='*60}

📊 Total Tasks: {len(manual_tasks)}
⏱️  Estimated Time: {len(manual_tasks) * 2} minutes

"""
        
        for i, task in enumerate(manual_tasks, 1):
            priority_icon = "🔥" if task.priority == "high" else "📄" if task.priority == "medium" else "📋"
            
            work_list += f"""
{i}. {priority_icon} {task.company_name} - {task.report_type} {task.year}
   Priority: {task.priority.upper()}
   Target: {task.target_date.strftime('%Y-%m-%d') if task.target_date else 'Unknown'}
   Failed attempts: {len(task.attempts)}
   
"""
        
        return work_list
    
    def save_state(self) -> str:
        """Save current state to JSON"""
        state_file = self.output_dir / 'orchestrator_state.json'
        
        state = {
            'last_updated': datetime.now().isoformat(),
            'tasks': []
        }
        
        for task in self.tasks:
            task_data = {
                'company_name': task.company_name,
                'report_type': task.report_type,
                'year': task.year,
                'status': task.status.value,
                'priority': task.priority,
                'created_date': task.created_date.isoformat(),
                'target_date': task.target_date.isoformat() if task.target_date else None,
                'direct_pdf_url': task.direct_pdf_url,
                'company_ir_page': task.company_ir_page,
                'final_file_path': task.final_file_path,
                'manual_instructions': task.manual_instructions,
                'attempts': [
                    {
                        'method': a.method.value,
                        'timestamp': a.timestamp.isoformat(),
                        'success': a.success,
                        'error_message': a.error_message
                    }
                    for a in task.attempts
                ]
            }
            state['tasks'].append(task_data)
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        
        return str(state_file)
    
    def print_summary(self):
        """Print system status summary"""
        status_counts = {}
        for task in self.tasks:
            status_counts[task.status.value] = status_counts.get(task.status.value, 0) + 1
        
        print(f"""
📊 DOWNLOAD ORCHESTRATOR STATUS
{'='*40}

📈 Tasks by Status:
""")
        for status, count in status_counts.items():
            icon = "✅" if status == "success" else "❌" if status == "failed" else "⏳" if status == "pending" else "👤"
            print(f"   {icon} {status.replace('_', ' ').title()}: {count}")
        
        manual_count = status_counts.get('manual_queue', 0)
        if manual_count > 0:
            print(f"\n👤 {manual_count} tasks need manual intervention")
            print(f"⏱️  Estimated manual work: {manual_count * 2} minutes")


async def main():
    """Demo of the download orchestrator"""
    
    orchestrator = DownloadOrchestrator()
    
    # Add some example tasks
    orchestrator.add_task(
        "Volvo Group", "Q3", 2024,
        direct_pdf_url="https://www.volvogroup.com/content/dam/volvo-group/markets/master/investors/reports-and-presentations/interim-reports/2025/volvo-group-q2-2025-sve.pdf",
        company_ir_page="https://www.volvogroup.com/investors/",
        priority="high",
        target_date=datetime(2024, 10, 25)
    )
    
    orchestrator.add_task(
        "H&M", "Q3", 2024,
        company_ir_page="https://hmgroup.com/investors/reports/",
        priority="high",
        target_date=datetime(2024, 10, 20)
    )
    
    print("🚀 Processing download tasks...")
    stats = await orchestrator.process_pending_tasks()
    
    print(f"\n📊 PROCESSING RESULTS:")
    print(f"   Attempted: {stats['attempted']}")
    print(f"   Succeeded: {stats['succeeded']}")
    print(f"   Failed: {stats['failed']}")
    print(f"   Queued for manual: {stats['queued_for_manual']}")
    
    # Show manual queue
    manual_work_list = orchestrator.generate_manual_work_list()
    print(f"\n{manual_work_list}")
    
    # Save state
    state_file = orchestrator.save_state()
    print(f"💾 State saved to: {state_file}")
    
    orchestrator.print_summary()


if __name__ == "__main__":
    asyncio.run(main())