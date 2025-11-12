"""
Local LLM Service using Ollama
Provides local AI analysis capabilities
"""
import httpx
import json
import asyncio
from typing import Dict, List, Optional, Any, AsyncGenerator
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OllamaModel:
    """Ollama model configuration"""
    name: str
    context_length: int
    good_for: List[str]  # ['analysis', 'chat', 'code', 'embedding']
    size_gb: float


class OllamaService:
    """Service for interacting with local Ollama models"""
    
    # Recommended models for financial analysis
    RECOMMENDED_MODELS = {
        "llama3.1:8b": OllamaModel(
            name="llama3.1:8b",
            context_length=128000,
            good_for=["analysis", "chat"],
            size_gb=4.7
        ),
        "llama3.1:70b": OllamaModel(
            name="llama3.1:70b", 
            context_length=128000,
            good_for=["analysis", "complex_reasoning"],
            size_gb=40.0
        ),
        "phi3:14b": OllamaModel(
            name="phi3:14b",
            context_length=128000,
            good_for=["analysis", "efficient"],
            size_gb=7.9
        ),
        "qwen2.5:14b": OllamaModel(
            name="qwen2.5:14b",
            context_length=32000,
            good_for=["analysis", "multilingual"],
            size_gb=8.7
        ),
        "mistral-nemo:12b": OllamaModel(
            name="mistral-nemo:12b",
            context_length=128000,
            good_for=["analysis", "chat"],
            size_gb=7.1
        )
    }
    
    def __init__(self, base_url: str = "http://localhost:11434", default_model: str = "llama3.1:8b"):
        self.base_url = base_url
        self.default_model = default_model
        self.client = httpx.AsyncClient(timeout=300.0)  # 5 minute timeout
        
    async def check_connection(self) -> bool:
        """Check if Ollama is running"""
        try:
            response = await self.client.get(f"{self.base_url}/api/tags")
            return response.status_code == 200
        except:
            return False
    
    async def list_models(self) -> List[Dict]:
        """List available models"""
        try:
            response = await self.client.get(f"{self.base_url}/api/tags")
            if response.status_code == 200:
                data = response.json()
                return data.get("models", [])
            return []
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            return []
    
    async def pull_model(self, model_name: str) -> bool:
        """Pull/download a model"""
        try:
            logger.info(f"Pulling model {model_name}...")
            
            async with self.client.stream(
                "POST",
                f"{self.base_url}/api/pull",
                json={"name": model_name}
            ) as response:
                if response.status_code == 200:
                    async for line in response.aiter_lines():
                        if line:
                            data = json.loads(line)
                            status = data.get("status", "")
                            if "downloading" in status.lower():
                                logger.info(f"Downloading {model_name}: {status}")
                            elif data.get("status") == "success":
                                logger.info(f"Successfully pulled {model_name}")
                                return True
                return False
        except Exception as e:
            logger.error(f"Failed to pull model {model_name}: {e}")
            return False
    
    async def generate_text(
        self,
        prompt: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        stream: bool = False
    ) -> str:
        """Generate text using Ollama"""
        
        model = model or self.default_model
        
        # Prepare messages
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        request_data = {
            "model": model,
            "messages": messages,
            "options": {
                "temperature": temperature,
                "num_predict": -1,  # Generate until stop
            },
            "stream": stream
        }
        
        try:
            if stream:
                return await self._stream_generate(request_data)
            else:
                response = await self.client.post(
                    f"{self.base_url}/api/chat",
                    json=request_data
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return data["message"]["content"]
                else:
                    logger.error(f"Ollama API error: {response.status_code}")
                    return ""
                    
        except Exception as e:
            logger.error(f"Error generating text: {e}")
            return ""
    
    async def _stream_generate(self, request_data: dict) -> str:
        """Handle streaming generation"""
        full_response = ""
        
        async with self.client.stream(
            "POST",
            f"{self.base_url}/api/chat", 
            json=request_data
        ) as response:
            if response.status_code == 200:
                async for line in response.aiter_lines():
                    if line:
                        data = json.loads(line)
                        if "message" in data:
                            content = data["message"].get("content", "")
                            full_response += content
                            if data.get("done", False):
                                break
        
        return full_response
    
    async def generate_analysis(
        self,
        documents_text: str,
        company_name: str,
        analysis_type: str = "comprehensive",
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate structured financial analysis"""
        
        system_prompt = """You are a senior financial analyst providing institutional-grade research. 
        Always provide structured, detailed analysis with specific evidence from the documents.
        Focus on actionable insights and quantifiable metrics where possible.
        
        Respond in valid JSON format with the following structure:
        {
            "executive_summary": "2-3 paragraph summary",
            "insights": [
                {
                    "category": "financial|strategic|operational|risk|competitive",
                    "insight": "specific insight with evidence",
                    "confidence": 0.0-1.0,
                    "supporting_evidence": ["quotes or data from documents"],
                    "metrics": {"metric_name": value}
                }
            ],
            "key_metrics": {"metric_name": "value"},
            "risk_assessment": {
                "overall_risk": "low|medium|high",
                "key_risks": [{"risk": "description", "severity": "low|medium|high"}]
            },
            "recommendations": ["specific recommendations"]
        }"""
        
        analysis_prompts = {
            "comprehensive": f"""
            Analyze these financial documents for {company_name} and provide comprehensive insights.
            
            Focus on:
            1. Financial performance and trends
            2. Strategic initiatives and progress  
            3. Key risks and challenges
            4. Competitive position
            5. Growth opportunities
            
            Documents:
            {documents_text}
            
            Provide detailed analysis with specific metrics and evidence.
            """,
            
            "financial": f"""
            Perform detailed financial analysis of {company_name} based on these documents:
            
            {documents_text}
            
            Extract and analyze:
            1. Revenue trends and growth
            2. Profitability metrics (margins, EBITDA, net income)
            3. Cash flow analysis  
            4. Key financial ratios
            5. YoY and QoQ comparisons
            
            Include specific numbers and calculations.
            """,
            
            "risk": f"""
            Conduct risk assessment for {company_name} using these documents:
            
            {documents_text}
            
            Identify and analyze:
            1. Market risks
            2. Operational risks
            3. Financial risks
            4. Regulatory risks
            5. Strategic risks
            
            Rate each risk as Low/Medium/High with supporting evidence.
            """
        }
        
        prompt = analysis_prompts.get(analysis_type, analysis_prompts["comprehensive"])
        
        # Generate analysis
        response_text = await self.generate_text(
            prompt=prompt,
            system_prompt=system_prompt,
            model=model,
            temperature=0.1  # Lower temperature for more focused analysis
        )
        
        # Try to parse JSON response
        try:
            # Clean response (remove any markdown formatting and extra text)
            clean_response = response_text.strip()
            
            # Remove markdown code blocks
            if clean_response.startswith("```json"):
                clean_response = clean_response[7:]
            if clean_response.endswith("```"):
                clean_response = clean_response[:-3]
            
            # Find JSON object - look for first { and last }
            start_idx = clean_response.find('{')
            end_idx = clean_response.rfind('}')
            
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                clean_response = clean_response[start_idx:end_idx+1]
            
            return json.loads(clean_response)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Raw response: {response_text[:500]}...")
            
            # Fallback: create structured response from raw text
            return {
                "executive_summary": response_text[:1000] + "...",
                "insights": [
                    {
                        "category": "general",
                        "insight": "Analysis generated but not in expected JSON format",
                        "confidence": 0.5,
                        "supporting_evidence": ["Raw LLM output"],
                        "metrics": {}
                    }
                ],
                "key_metrics": {},
                "risk_assessment": {"overall_risk": "unknown", "key_risks": []},
                "recommendations": ["Review raw analysis output"]
            }
    
    async def answer_question(
        self,
        question: str,
        context: str,
        company_name: str,
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """Answer questions about a company using document context"""
        
        system_prompt = f"""You are analyzing documents for {company_name}. 
        Answer the user's question based solely on the provided document context.
        Be specific and cite evidence from the documents.
        If information is not available in the context, clearly state that."""
        
        prompt = f"""
        Question: {question}
        
        Context from {company_name} documents:
        {context}
        
        Please provide a detailed answer with supporting evidence from the documents.
        """
        
        response = await self.generate_text(
            prompt=prompt,
            system_prompt=system_prompt,
            model=model
        )
        
        return {
            "question": question,
            "answer": response,
            "confidence": 0.8,  # Could be improved with confidence estimation
            "sources": ["Document context provided"]
        }
    
    async def setup_recommended_model(self, model_name: str = "llama3.1:8b") -> bool:
        """Set up a recommended model for financial analysis"""
        
        logger.info(f"Setting up {model_name} for financial analysis...")
        
        # Check if Ollama is running
        if not await self.check_connection():
            logger.error("Ollama is not running. Please start it with: ollama serve")
            return False
        
        # Check if model is already available
        models = await self.list_models()
        model_names = [m["name"] for m in models]
        
        if model_name not in model_names:
            logger.info(f"Model {model_name} not found locally. Pulling...")
            success = await self.pull_model(model_name)
            if not success:
                logger.error(f"Failed to pull model {model_name}")
                return False
        
        # Test the model
        test_response = await self.generate_text(
            "Hello! Can you analyze financial documents?",
            model=model_name
        )
        
        if test_response:
            logger.info(f"Model {model_name} is ready for financial analysis")
            self.default_model = model_name
            return True
        else:
            logger.error(f"Model {model_name} failed test generation")
            return False


# Integration with existing analysis service
class LocalAnalysisService:
    """Analysis service using local Ollama models"""
    
    def __init__(self, ollama_base_url: str = "http://localhost:11434"):
        self.ollama = OllamaService(ollama_base_url)
        
    async def analyze_company_local(
        self,
        company_name: str,
        documents_text: str,
        analysis_type: str = "comprehensive",
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """Run company analysis using local LLM"""
        
        start_time = asyncio.get_event_loop().time()
        
        # Generate analysis
        result = await self.ollama.generate_analysis(
            documents_text=documents_text,
            company_name=company_name,
            analysis_type=analysis_type,
            model=model
        )
        
        # Add metadata
        result["model_used"] = model or self.ollama.default_model
        result["processing_time"] = asyncio.get_event_loop().time() - start_time
        result["cost"] = 0.0  # Local models are free!
        result["tokens_used"] = len(documents_text.split())  # Rough estimate
        
        return result