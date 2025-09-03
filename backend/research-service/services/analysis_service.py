"""
LLM Analysis Service
Orchestrates AI-powered document analysis
"""
import asyncio
import json
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

from openai import AsyncOpenAI
import anthropic
import tiktoken

from ..config import settings
from ..processors.financial_parser import FinancialData
from ..processors.language_detector import LanguageDetector
from .local_llm_service import LocalAnalysisService

logger = logging.getLogger(__name__)


@dataclass
class AnalysisRequest:
    """Request for document analysis"""
    company_id: str
    company_name: str
    analysis_type: str  # comprehensive, financial, risk, competitive, growth
    documents: List[Dict]  # Document metadata
    time_range: Optional[str] = None
    focus_areas: Optional[List[str]] = None
    language: str = "en"
    streaming: bool = False


@dataclass
class AnalysisInsight:
    """Single insight from analysis"""
    category: str
    insight: str
    confidence: float
    supporting_evidence: List[str]
    source_documents: List[str]
    metrics: Optional[Dict] = None


@dataclass 
class AnalysisResult:
    """Complete analysis result"""
    request_id: str
    company_name: str
    analysis_type: str
    executive_summary: str
    insights: List[AnalysisInsight]
    key_metrics: Dict[str, Any]
    risk_assessment: Dict[str, Any]
    recommendations: List[str]
    model_used: str
    tokens_used: int
    cost: float
    timestamp: datetime


class AnalysisService:
    """Orchestrates LLM-based document analysis"""
    
    ANALYSIS_PROMPTS = {
        "comprehensive": """
Analyze these financial documents for {company_name} and provide comprehensive insights.

Documents provided:
{document_list}

Focus on:
1. Financial performance and trends
2. Strategic initiatives and progress
3. Key risks and challenges
4. Competitive position
5. Management quality and strategy
6. Growth opportunities

Provide specific metrics, quotes, and evidence from the documents.
Format your response as structured JSON with clear insights.
        """,
        
        "financial": """
Perform detailed financial analysis of {company_name} based on these documents:

{document_list}

Extract and analyze:
1. Revenue trends and growth
2. Profitability metrics (margins, EBITDA, net income)
3. Cash flow analysis
4. Balance sheet strength
5. Key financial ratios
6. YoY and QoQ comparisons

Include specific numbers and calculations.
        """,
        
        "risk": """
Conduct risk assessment for {company_name} using these documents:

{document_list}

Identify and analyze:
1. Market risks
2. Operational risks
3. Financial risks
4. Regulatory risks
5. Strategic risks
6. ESG risks

Rate each risk as Low/Medium/High with supporting evidence.
        """,
        
        "competitive": """
Analyze {company_name}'s competitive position from these documents:

{document_list}

Focus on:
1. Market share and position
2. Competitive advantages/moats
3. Threats from competitors
4. Industry dynamics
5. Pricing power
6. Innovation and R&D

Compare with industry peers where mentioned.
        """,
        
        "growth": """
Evaluate growth prospects for {company_name} based on:

{document_list}

Analyze:
1. Organic growth drivers
2. New market opportunities
3. Product pipeline
4. M&A strategy
5. Investment plans
6. Long-term guidance

Project future growth potential with supporting evidence.
        """
    }
    
    def __init__(self, use_local_llm: bool = False):
        self.use_local_llm = use_local_llm or settings.get("use_local_llm", False)
        
        # Initialize local LLM service if requested
        if self.use_local_llm:
            self.local_service = LocalAnalysisService()
            self.openai_client = None
            self.anthropic_client = None
        else:
            self.openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
            self.anthropic_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key) if settings.anthropic_api_key else None
            self.local_service = None
        
        self.tokenizer = tiktoken.encoding_for_model("gpt-4") if not self.use_local_llm else None
        self.language_detector = LanguageDetector()
        
        # Cost per 1K tokens (input/output)
        self.model_costs = {
            "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
            "gpt-4o": {"input": 0.005, "output": 0.015},
            "claude-3-haiku": {"input": 0.00025, "output": 0.00125},
            "claude-3-sonnet": {"input": 0.003, "output": 0.015}
        }
    
    async def analyze_company(
        self, 
        request: AnalysisRequest,
        document_chunks: List[Dict]
    ) -> AnalysisResult:
        """Perform comprehensive company analysis"""
        
        # Use local LLM if enabled
        if self.use_local_llm:
            return await self._analyze_with_local_llm(request, document_chunks)
        
        # Prepare context from documents
        context = self._prepare_analysis_context(request, document_chunks)
        
        # Select appropriate prompt
        prompt = self._build_analysis_prompt(request, context)
        
        # Run analysis
        if request.streaming:
            # Return async generator for streaming
            return self._stream_analysis(prompt, request)
        else:
            # Return complete result
            return await self._complete_analysis(prompt, request)
    
    def _prepare_analysis_context(
        self, 
        request: AnalysisRequest,
        document_chunks: List[Dict]
    ) -> str:
        """Prepare document context for analysis"""
        
        # Group chunks by document
        docs_by_id = {}
        for chunk in document_chunks:
            doc_id = chunk['document_id']
            if doc_id not in docs_by_id:
                docs_by_id[doc_id] = []
            docs_by_id[doc_id].append(chunk['text'])
        
        # Build context
        context_parts = []
        for doc in request.documents:
            doc_id = doc['id']
            if doc_id in docs_by_id:
                doc_text = "\n".join(docs_by_id[doc_id])
                
                # Truncate if too long
                if len(doc_text) > 50000:
                    doc_text = doc_text[:50000] + "\n[... truncated ...]"
                
                context_parts.append(f"""
Document: {doc['title']} ({doc['document_type']} - {doc['report_period']})
Content:
{doc_text}
---
                """)
        
        return "\n".join(context_parts)
    
    def _build_analysis_prompt(self, request: AnalysisRequest, context: str) -> str:
        """Build the analysis prompt"""
        
        base_prompt = self.ANALYSIS_PROMPTS.get(
            request.analysis_type, 
            self.ANALYSIS_PROMPTS["comprehensive"]
        )
        
        # Format document list
        doc_list = "\n".join([
            f"- {doc['title']} ({doc['document_type']} - {doc['report_period']})"
            for doc in request.documents
        ])
        
        prompt = base_prompt.format(
            company_name=request.company_name,
            document_list=doc_list
        )
        
        # Add focus areas if specified
        if request.focus_areas:
            prompt += f"\n\nPay special attention to: {', '.join(request.focus_areas)}"
        
        # Add language instruction if not English
        if request.language != 'en':
            lang_name = "Swedish" if request.language == 'sv' else request.language
            prompt += f"\n\nProvide your analysis in {lang_name}."
        
        # Add the actual document content
        prompt += f"\n\nDocument Content:\n{context}"
        
        # Add output format instruction
        prompt += """

Provide your analysis in the following JSON structure:
{
    "executive_summary": "2-3 paragraph summary of key findings",
    "insights": [
        {
            "category": "financial|strategic|operational|risk|competitive",
            "insight": "Clear, specific insight",
            "confidence": 0.0-1.0,
            "supporting_evidence": ["Quote or data from documents"],
            "source_documents": ["Document names"],
            "metrics": {"metric_name": value} // optional
        }
    ],
    "key_metrics": {
        "revenue_growth": "15%",
        "ebitda_margin": "22%",
        // ... other relevant metrics
    },
    "risk_assessment": {
        "overall_risk": "low|medium|high",
        "key_risks": [
            {"risk": "description", "severity": "low|medium|high", "evidence": "..."}
        ]
    },
    "recommendations": [
        "Specific, actionable recommendations based on analysis"
    ]
}
"""
        
        return prompt
    
    async def _complete_analysis(
        self, 
        prompt: str, 
        request: AnalysisRequest
    ) -> AnalysisResult:
        """Run complete analysis and return result"""
        
        try:
            # Count tokens
            prompt_tokens = len(self.tokenizer.encode(prompt))
            
            # Select model based on token count
            model = self._select_model(prompt_tokens)
            
            # Run analysis
            response = await self.openai_client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a senior financial analyst providing institutional-grade research."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,  # Lower temperature for more focused analysis
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result_data = json.loads(response.choices[0].message.content)
            
            # Calculate costs
            output_tokens = response.usage.completion_tokens
            total_tokens = response.usage.total_tokens
            cost = self._calculate_cost(model, prompt_tokens, output_tokens)
            
            # Build result
            insights = [
                AnalysisInsight(**insight_data) 
                for insight_data in result_data.get("insights", [])
            ]
            
            result = AnalysisResult(
                request_id=f"analysis_{datetime.now().timestamp()}",
                company_name=request.company_name,
                analysis_type=request.analysis_type,
                executive_summary=result_data.get("executive_summary", ""),
                insights=insights,
                key_metrics=result_data.get("key_metrics", {}),
                risk_assessment=result_data.get("risk_assessment", {}),
                recommendations=result_data.get("recommendations", []),
                model_used=model,
                tokens_used=total_tokens,
                cost=cost,
                timestamp=datetime.now()
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Analysis error: {e}")
            raise
    
    async def _stream_analysis(
        self, 
        prompt: str, 
        request: AnalysisRequest
    ) -> AsyncGenerator[str, None]:
        """Stream analysis results"""
        
        # This would stream partial results
        # Implementation depends on frontend requirements
        pass
    
    def _select_model(self, prompt_tokens: int) -> str:
        """Select appropriate model based on context size"""
        
        if prompt_tokens > 100000:
            # Use Claude for very long contexts
            return "claude-3-sonnet" if self.anthropic_client else "gpt-4o"
        elif prompt_tokens > 50000:
            return "gpt-4o"
        else:
            # Use cheaper model for shorter contexts
            return settings.default_llm_model
    
    def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate analysis cost"""
        
        costs = self.model_costs.get(model, self.model_costs["gpt-4o-mini"])
        input_cost = (input_tokens / 1000) * costs["input"]
        output_cost = (output_tokens / 1000) * costs["output"]
        
        return input_cost + output_cost
    
    async def compare_companies(
        self,
        companies: List[Dict],
        metrics: List[str],
        period: str
    ) -> Dict[str, Any]:
        """Compare multiple companies"""
        
        # This would implement comparative analysis
        # Comparing metrics across companies
        pass
    
    async def track_metric_evolution(
        self,
        company_id: str,
        metric: str,
        periods: List[str]
    ) -> Dict[str, Any]:
        """Track how a metric evolves over time"""
        
        # This would analyze metric trends
        pass
    
    async def _analyze_with_local_llm(
        self, 
        request: AnalysisRequest, 
        document_chunks: List[Dict]
    ) -> AnalysisResult:
        """Perform analysis using local LLM"""
        
        # Prepare document text
        documents_text = self._prepare_analysis_context(request, document_chunks)
        
        # Run local analysis
        result_data = await self.local_service.analyze_company_local(
            company_name=request.company_name,
            documents_text=documents_text,
            analysis_type=request.analysis_type
        )
        
        # Convert to AnalysisResult format
        insights = []
        if "insights" in result_data and isinstance(result_data["insights"], list):
            for insight_data in result_data["insights"]:
                if isinstance(insight_data, dict):
                    insights.append(AnalysisInsight(
                        category=insight_data.get("category", "general"),
                        insight=insight_data.get("insight", ""),
                        confidence=insight_data.get("confidence", 0.5),
                        supporting_evidence=insight_data.get("supporting_evidence", []),
                        source_documents=insight_data.get("source_documents", []),
                        metrics=insight_data.get("metrics", {})
                    ))
        
        # Build standard AnalysisResult
        result = AnalysisResult(
            request_id=f"local_analysis_{datetime.now().timestamp()}",
            company_name=request.company_name,
            analysis_type=request.analysis_type,
            executive_summary=result_data.get("executive_summary", ""),
            insights=insights,
            key_metrics=result_data.get("key_metrics", {}),
            risk_assessment=result_data.get("risk_assessment", {}),
            recommendations=result_data.get("recommendations", []),
            model_used=result_data.get("model_used", "local-llm"),
            tokens_used=result_data.get("tokens_used", 0),
            cost=result_data.get("cost", 0.0),  # Local LLM is free!
            timestamp=datetime.now()
        )
        
        return result