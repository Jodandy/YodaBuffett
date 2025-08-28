"""
LLM Analysis Engine for Financial Documents
Integrates with OpenAI to analyze extracted document content.
"""

import os
import json
import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# OpenAI integration
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from document_processor import ProcessedDocument, DocumentSection


@dataclass
class AnalysisInsight:
    """Represents a single insight from LLM analysis."""
    insight: str
    supporting_evidence: str
    confidence: float  # 0.0 to 1.0
    source_section: Optional[str] = None
    page_reference: Optional[str] = None


@dataclass
class AnalysisResult:
    """Complete analysis result from LLM."""
    analysis_type: str
    company_name: str
    filing_type: str
    insights: List[AnalysisInsight]
    executive_summary: str
    risk_level: str  # "Low", "Medium", "High"
    model_used: str
    tokens_used: int
    analysis_date: datetime
    confidence_score: float  # Overall confidence in analysis


class LLMAnalyzer:
    """Main class for LLM-powered financial document analysis."""
    
    def __init__(self):
        """Initialize the LLM analyzer."""
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI library not available. Install with: pip install openai")
        
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        self.client = OpenAI(api_key=api_key)
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        self.max_tokens = 6000  # Increased for detailed P/E analysis
        self.temperature = 0.1  # Low temperature for consistent analysis
    
    async def analyze_document(self, doc: ProcessedDocument, analysis_type: str = "comprehensive") -> AnalysisResult:
        """
        Analyze a processed document using LLM.
        
        Args:
            doc: ProcessedDocument with extracted content
            analysis_type: Type of analysis to perform
            
        Returns:
            AnalysisResult with insights and findings
        """
        if analysis_type == "comprehensive":
            return await self._comprehensive_analysis(doc)
        elif analysis_type == "risk_assessment" or analysis_type == "risk":
            return await self._risk_assessment(doc)
        elif analysis_type == "growth_analysis" or analysis_type == "growth":
            return await self._growth_analysis(doc)
        elif analysis_type == "financial_health" or analysis_type == "financial":
            return await self._financial_health_analysis(doc)
        else:
            raise ValueError(f"Unknown analysis type: {analysis_type}")
    
    async def _comprehensive_analysis(self, doc: ProcessedDocument) -> AnalysisResult:
        """Perform comprehensive analysis of the document."""
        
        # Prepare context for LLM
        context = self._prepare_context(doc)
        
        prompt = f"""You are a senior financial analyst with 20+ years of experience. Analyze this SEC filing comprehensively.

DOCUMENT CONTEXT:
Company: {doc.company_name or 'Unknown'}
Filing Type: {doc.filing_type or 'Unknown'}
Filing Date: {doc.filing_date or 'Unknown'}

DOCUMENT SECTIONS AVAILABLE:
{self._format_sections_summary(doc)}

ANALYSIS INSTRUCTIONS:
1. Provide 5-7 key business insights
2. Assess overall financial health
3. Identify major opportunities and risks
4. Rate overall risk level (Low/Medium/High)
5. Include specific evidence from the filing
6. Rate confidence for each insight (0.0-1.0)

CONTENT TO ANALYZE:
{context[:15000]}  # Limit context to fit in prompt

Please respond in the following JSON format:
{{
    "executive_summary": "2-3 sentence overview of the company's current situation",
    "risk_level": "Low|Medium|High",
    "insights": [
        {{
            "insight": "Clear, actionable insight",
            "supporting_evidence": "Specific quote or data from the filing",
            "confidence": 0.85,
            "source_section": "Business|Risk Factors|MD&A|Financial Statements"
        }}
    ],
    "overall_confidence": 0.80
}}"""
        
        try:
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert financial analyst. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            # Parse response
            response_text = response.choices[0].message.content
            tokens_used = response.usage.total_tokens
            
            # Parse JSON response
            try:
                analysis_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                print(f"Raw response length: {len(response_text)} chars")
                
                # Try to extract JSON from response if it's embedded
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    try:
                        analysis_data = json.loads(json_match.group())
                        print("Successfully extracted JSON from response")
                    except json.JSONDecodeError:
                        return self._create_fallback_result(doc, response_text, tokens_used)
                else:
                    return self._create_fallback_result(doc, response_text, tokens_used)
            
            # Create insights
            insights = []
            for insight_data in analysis_data.get('insights', []):
                insights.append(AnalysisInsight(
                    insight=insight_data.get('insight', ''),
                    supporting_evidence=insight_data.get('supporting_evidence', ''),
                    confidence=float(insight_data.get('confidence', 0.5)),
                    source_section=insight_data.get('source_section')
                ))
            
            return AnalysisResult(
                analysis_type="comprehensive",
                company_name=doc.company_name or 'Unknown',
                filing_type=doc.filing_type or 'Unknown',
                insights=insights,
                executive_summary=analysis_data.get('executive_summary', ''),
                risk_level=analysis_data.get('risk_level', 'Medium'),
                model_used=self.model,
                tokens_used=tokens_used,
                analysis_date=datetime.now(),
                confidence_score=float(analysis_data.get('overall_confidence', 0.7))
            )
            
        except Exception as e:
            raise RuntimeError(f"LLM analysis failed: {e}")
    
    async def _risk_assessment(self, doc: ProcessedDocument) -> AnalysisResult:
        """Focus specifically on risk analysis."""
        # Find risk factors section
        risk_content = ""
        for section in doc.sections:
            if "risk" in section.name.lower():
                risk_content = section.content
                break
        
        if not risk_content and doc.full_text:
            # Use first 10K characters if no risk section found
            risk_content = doc.full_text[:10000]
        
        prompt = f"""You are a risk management expert. Analyze the following content for business risks.

COMPANY: {doc.company_name or 'Unknown'}
FILING: {doc.filing_type or 'Unknown'}

CONTENT:
{risk_content[:12000]}

Identify the top 5 risks and rate them by severity and likelihood. Respond in JSON format:
{{
    "executive_summary": "Overall risk assessment summary",
    "risk_level": "Low|Medium|High",
    "insights": [
        {{
            "insight": "Specific risk identified",
            "supporting_evidence": "Evidence from the filing",
            "confidence": 0.90,
            "source_section": "Risk Factors"
        }}
    ],
    "overall_confidence": 0.85
}}"""
        
        # Similar API call structure as comprehensive analysis
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a risk management expert. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            response_text = response.choices[0].message.content
            tokens_used = response.usage.total_tokens
            analysis_data = json.loads(response_text)
            
            insights = []
            for insight_data in analysis_data.get('insights', []):
                insights.append(AnalysisInsight(
                    insight=insight_data.get('insight', ''),
                    supporting_evidence=insight_data.get('supporting_evidence', ''),
                    confidence=float(insight_data.get('confidence', 0.5)),
                    source_section=insight_data.get('source_section', 'Risk Factors')
                ))
            
            return AnalysisResult(
                analysis_type="risk_assessment",
                company_name=doc.company_name or 'Unknown',
                filing_type=doc.filing_type or 'Unknown',
                insights=insights,
                executive_summary=analysis_data.get('executive_summary', ''),
                risk_level=analysis_data.get('risk_level', 'Medium'),
                model_used=self.model,
                tokens_used=tokens_used,
                analysis_date=datetime.now(),
                confidence_score=float(analysis_data.get('overall_confidence', 0.7))
            )
            
        except Exception as e:
            raise RuntimeError(f"Risk analysis failed: {e}")
    
    async def _growth_analysis(self, doc: ProcessedDocument) -> AnalysisResult:
        """Analyze growth opportunities and P/E scenarios with bull/bear cases."""
        
        # Focus on financial statements and MD&A for growth analysis
        growth_content = self._prepare_context(doc)
        
        prompt = f"""You are a senior financial analyst specializing in earnings analysis. Extract and analyze earnings metrics from this SEC filing.

COMPANY: {doc.company_name or 'Unknown'}
FILING: {doc.filing_type or 'Unknown'}

CONTENT:
{growth_content[:12000]}

Extract financial metrics and provide growth scenarios. Focus on actual data from the filing, NOT market prices or P/E ratios. Respond with this EXACT JSON format:
{{
    "executive_summary": "Financial performance and earnings overview with key takeaways",
    "risk_level": "Low|Medium|High",
    "earnings_metrics": {{
        "eps_basic": "Basic EPS for most recent year (e.g., $2.45)",
        "eps_diluted": "Diluted EPS for most recent year (e.g., $2.41)",
        "eps_prior_year": "Prior year EPS for comparison (e.g., $2.20)",
        "eps_growth": "YoY EPS growth rate (e.g., +9.5%)",
        "net_income": "Net income in millions (e.g., $1,234M)",
        "revenue": "Total revenue in millions (e.g., $5,678M)",
        "revenue_growth": "YoY revenue growth rate (e.g., +12.3%)"
    }},
    "forward_scenarios": {{
        "bull_case": {{
            "scenario": "Optimistic earnings growth scenario based on company guidance/trends",
            "key_assumptions": "Revenue growth drivers, margin expansion, market opportunities",
            "eps_growth_estimate": "Projected EPS growth rate (e.g., +15-20%)",
            "probability": "0.3"
        }},
        "bear_case": {{
            "scenario": "Conservative earnings scenario based on risks identified", 
            "key_assumptions": "Revenue headwinds, margin pressure, competitive threats",
            "eps_growth_estimate": "Projected EPS growth rate (e.g., -5% to +5%)",
            "probability": "0.2"
        }},
        "base_case": {{
            "scenario": "Most likely earnings scenario based on current trajectory",
            "key_assumptions": "Continued current trends, stable margins, market conditions", 
            "eps_growth_estimate": "Projected EPS growth rate (e.g., +8-12%)",
            "probability": "0.5"
        }}
    }},
    "insights": [
        {{
            "insight": "Specific earnings or financial performance insight",
            "supporting_evidence": "Exact data from the filing (include numbers)",
            "confidence": 0.85,
            "source_section": "Financial Statements"
        }}
    ],
    "overall_confidence": 0.80
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert equity research analyst specializing in valuation. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            response_text = response.choices[0].message.content
            tokens_used = response.usage.total_tokens
            
            try:
                analysis_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                print(f"Raw response length: {len(response_text)} chars")
                print(f"Raw response preview: {response_text[:500]}...")
                
                # Try to extract JSON from response if it's embedded
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    try:
                        analysis_data = json.loads(json_match.group())
                        print("Successfully extracted JSON from response")
                    except json.JSONDecodeError:
                        return self._create_fallback_result(doc, response_text, tokens_used)
                else:
                    return self._create_fallback_result(doc, response_text, tokens_used)
            
            # Create insights
            insights = []
            for insight_data in analysis_data.get('insights', []):
                insights.append(AnalysisInsight(
                    insight=insight_data.get('insight', ''),
                    supporting_evidence=insight_data.get('supporting_evidence', ''),
                    confidence=float(insight_data.get('confidence', 0.5)),
                    source_section=insight_data.get('source_section', 'Growth Analysis')
                ))
            
            # Create custom result with P/E analysis
            result = AnalysisResult(
                analysis_type="growth_analysis",
                company_name=doc.company_name or 'Unknown',
                filing_type=doc.filing_type or 'Unknown',
                insights=insights,
                executive_summary=analysis_data.get('executive_summary', ''),
                risk_level=analysis_data.get('risk_level', 'Medium'),
                model_used=self.model,
                tokens_used=tokens_used,
                analysis_date=datetime.now(),
                confidence_score=float(analysis_data.get('overall_confidence', 0.7))
            )
            
            # Store additional earnings data in a custom attribute
            result.earnings_analysis = {
                'earnings_metrics': analysis_data.get('earnings_metrics', {}),
                'scenarios': analysis_data.get('forward_scenarios', {})
            }
            
            return result
            
        except Exception as e:
            raise RuntimeError(f"Growth/P/E analysis failed: {e}")
    
    async def _financial_health_analysis(self, doc: ProcessedDocument) -> AnalysisResult:
        """Analyze financial health and performance metrics."""
        # Implementation for financial health analysis
        pass
    
    def _prepare_context(self, doc: ProcessedDocument) -> str:
        """Prepare document context for LLM analysis."""
        context = f"COMPANY: {doc.company_name}\n"
        context += f"FILING TYPE: {doc.filing_type}\n"
        context += f"FILING DATE: {doc.filing_date}\n\n"
        
        # Add sections in order of importance
        section_priority = ['business', 'mda', 'risk_factors', 'financial_statements', 'controls']
        
        for priority_section in section_priority:
            for section in doc.sections:
                if priority_section in section.name.lower().replace(' ', '_'):
                    context += f"\n=== {section.name.upper()} ===\n"
                    context += section.content[:3000]  # Limit each section
                    context += "\n"
                    break
        
        # If we don't have enough content from sections, add from full text
        if len(context) < 2000 and doc.full_text:
            context += "\n=== ADDITIONAL CONTENT ===\n"
            context += doc.full_text[:5000]
        
        return context
    
    def _format_sections_summary(self, doc: ProcessedDocument) -> str:
        """Create a summary of available sections."""
        if not doc.sections:
            return "No structured sections identified"
        
        summary = ""
        for section in doc.sections:
            summary += f"- {section.name}: {len(section.content):,} characters\n"
        
        return summary
    
    def _create_fallback_result(self, doc: ProcessedDocument, response_text: str, tokens_used: int) -> AnalysisResult:
        """Create a fallback result if JSON parsing fails."""
        return AnalysisResult(
            analysis_type="comprehensive",
            company_name=doc.company_name or 'Unknown',
            filing_type=doc.filing_type or 'Unknown',
            insights=[AnalysisInsight(
                insight="Analysis completed but response format was unexpected",
                supporting_evidence=response_text[:500],
                confidence=0.3
            )],
            executive_summary="Analysis was performed but encountered formatting issues.",
            risk_level="Medium",
            model_used=self.model,
            tokens_used=tokens_used,
            analysis_date=datetime.now(),
            confidence_score=0.3
        )
    
    def print_analysis_result(self, result: AnalysisResult):
        """Print a formatted analysis result."""
        print(f"\n{'='*80}")
        print(f"FINANCIAL ANALYSIS REPORT")
        print(f"{'='*80}")
        print(f"Company: {result.company_name}")
        print(f"Filing Type: {result.filing_type}")
        print(f"Analysis Type: {result.analysis_type.title()}")
        print(f"Analysis Date: {result.analysis_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Model Used: {result.model_used}")
        print(f"Tokens Used: {result.tokens_used:,}")
        print(f"Overall Risk Level: {result.risk_level}")
        print(f"Confidence Score: {result.confidence_score:.2f}")
        
        print(f"\n{'='*60}")
        print(f"EXECUTIVE SUMMARY")
        print(f"{'='*60}")
        print(result.executive_summary)
        
        # Special formatting for growth analysis with earnings scenarios
        if hasattr(result, 'earnings_analysis') and result.earnings_analysis:
            self._print_earnings_analysis(result.earnings_analysis)
        
        print(f"\n{'='*60}")
        print(f"KEY INSIGHTS ({len(result.insights)} found)")
        print(f"{'='*60}")
        
        for i, insight in enumerate(result.insights, 1):
            print(f"\n{i}. {insight.insight}")
            print(f"   Evidence: {insight.supporting_evidence}")
            print(f"   Confidence: {insight.confidence:.2f}")
            if insight.source_section:
                print(f"   Source: {insight.source_section}")
    
    def _print_earnings_analysis(self, earnings_analysis):
        """Print formatted earnings metrics and scenario analysis."""
        print(f"\n{'='*60}")
        print(f"EARNINGS METRICS & GROWTH ANALYSIS")
        print(f"{'='*60}")
        
        # Current Earnings Metrics
        earnings_metrics = earnings_analysis.get('earnings_metrics', {})
        if earnings_metrics:
            print(f"\n📊 HISTORICAL EARNINGS METRICS:")
            print(f"   Basic EPS: {earnings_metrics.get('eps_basic', 'N/A')}")
            print(f"   Diluted EPS: {earnings_metrics.get('eps_diluted', 'N/A')}")
            print(f"   Prior Year EPS: {earnings_metrics.get('eps_prior_year', 'N/A')}")
            print(f"   EPS Growth: {earnings_metrics.get('eps_growth', 'N/A')}")
            print(f"   Net Income: {earnings_metrics.get('net_income', 'N/A')}")
            print(f"   Revenue: {earnings_metrics.get('revenue', 'N/A')}")
            print(f"   Revenue Growth: {earnings_metrics.get('revenue_growth', 'N/A')}")
        
        # Scenario Analysis
        scenarios = earnings_analysis.get('scenarios', {})
        if scenarios:
            print(f"\n🎯 FORWARD EARNINGS SCENARIOS:")
            
            for case_name, case_data in scenarios.items():
                if not case_data:
                    continue
                    
                case_title = case_name.replace('_', ' ').title()
                prob = case_data.get('probability', 'N/A')
                eps_growth = case_data.get('eps_growth_estimate', 'N/A')
                
                if case_name == 'bull_case':
                    emoji = "🐂"
                elif case_name == 'bear_case':
                    emoji = "🐻"
                else:
                    emoji = "⚖️"
                
                print(f"\n   {emoji} {case_title} (Probability: {prob})")
                print(f"      Scenario: {case_data.get('scenario', 'N/A')}")
                print(f"      Key Assumptions: {case_data.get('key_assumptions', 'N/A')}")
                print(f"      EPS Growth Estimate: {eps_growth}")


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    from document_processor import DocumentProcessor
    
    async def test_analysis():
        # Test the LLM analyzer
        print("Testing LLM Analysis...")
        
        # Process a document first
        processor = DocumentProcessor()
        analyzer = LLMAnalyzer()
        
        # You can test with any document in data/ folder
        test_files = ["apple-10-k.pdf", "sample_filing.html"]
        
        for filename in test_files:
            try:
                doc = processor.process_file(f"data/{filename}")
                print(f"\nAnalyzing: {filename}")
                
                # Perform comprehensive analysis
                result = await analyzer.analyze_document(doc, "comprehensive")
                analyzer.print_analysis_result(result)
                
                break  # Just test one file
                
            except FileNotFoundError:
                print(f"File not found: {filename}")
                continue
            except Exception as e:
                print(f"Error analyzing {filename}: {e}")
    
    # Run the test
    # asyncio.run(test_analysis())