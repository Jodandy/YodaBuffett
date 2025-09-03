"""
Insight Service
Generates insights from document analysis
"""
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime
from collections import defaultdict
import re
import logging

from .analysis_service import AnalysisResult, AnalysisInsight

logger = logging.getLogger(__name__)


class InsightService:
    """Generates and manages research insights"""
    
    # Key phrases that indicate important insights
    INSIGHT_INDICATORS = {
        'growth': [
            'growth', 'increase', 'expansion', 'rising', 'växer', 'ökning', 
            'tillväxt', 'expanding', 'accelerating'
        ],
        'decline': [
            'decline', 'decrease', 'falling', 'reduction', 'minskning', 
            'nedgång', 'deteriorating', 'weakening'
        ],
        'risk': [
            'risk', 'threat', 'challenge', 'concern', 'uncertainty', 'risk', 
            'hot', 'utmaning', 'osäkerhet', 'vulnerable'
        ],
        'opportunity': [
            'opportunity', 'potential', 'prospect', 'möjlighet', 'potential', 
            'favorable', 'positive', 'promising'
        ],
        'strategic': [
            'strategy', 'strategic', 'initiative', 'transformation', 'strategi', 
            'strategisk', 'restructuring', 'pivot'
        ]
    }
    
    def extract_key_insights(
        self,
        analysis_results: List[AnalysisResult]
    ) -> Dict[str, List[AnalysisInsight]]:
        """Extract and categorize key insights from multiple analyses"""
        
        categorized_insights = defaultdict(list)
        
        for result in analysis_results:
            for insight in result.insights:
                # Categorize by type
                categorized_insights[insight.category].append(insight)
                
                # Also categorize by theme
                themes = self._identify_themes(insight.insight)
                for theme in themes:
                    categorized_insights[f"theme_{theme}"].append(insight)
        
        # Sort by confidence and deduplicate
        for category in categorized_insights:
            insights = categorized_insights[category]
            # Sort by confidence
            insights.sort(key=lambda x: x.confidence, reverse=True)
            # Deduplicate similar insights
            categorized_insights[category] = self._deduplicate_insights(insights)
        
        return dict(categorized_insights)
    
    def _identify_themes(self, text: str) -> Set[str]:
        """Identify themes in insight text"""
        themes = set()
        text_lower = text.lower()
        
        for theme, indicators in self.INSIGHT_INDICATORS.items():
            if any(indicator in text_lower for indicator in indicators):
                themes.add(theme)
        
        return themes
    
    def _deduplicate_insights(
        self,
        insights: List[AnalysisInsight]
    ) -> List[AnalysisInsight]:
        """Remove duplicate or very similar insights"""
        
        unique_insights = []
        seen_texts = set()
        
        for insight in insights:
            # Simple deduplication based on text similarity
            insight_key = self._normalize_text(insight.insight)[:100]
            
            if insight_key not in seen_texts:
                seen_texts.add(insight_key)
                unique_insights.append(insight)
        
        return unique_insights
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison"""
        # Remove numbers and special characters
        text = re.sub(r'[0-9%$€,.]', '', text)
        # Convert to lowercase and remove extra spaces
        text = ' '.join(text.lower().split())
        return text
    
    def generate_trend_insights(
        self,
        timeline_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate insights from timeline/trend data"""
        
        insights = []
        
        # Sort by date
        timeline_data.sort(key=lambda x: x.get('date', ''))
        
        # Analyze metric trends
        if len(timeline_data) >= 2:
            metrics_by_name = defaultdict(list)
            
            for entry in timeline_data:
                if 'metric_value' in entry:
                    metrics_by_name[entry.get('metric_name', 'unknown')].append({
                        'period': entry['period'],
                        'value': entry['metric_value'],
                        'date': entry['date']
                    })
            
            # Generate trend insights for each metric
            for metric_name, values in metrics_by_name.items():
                trend = self._calculate_trend(values)
                if trend:
                    insights.append(trend)
        
        return insights
    
    def _calculate_trend(self, values: List[Dict]) -> Optional[Dict]:
        """Calculate trend from metric values"""
        
        if len(values) < 2:
            return None
        
        # Simple trend calculation
        first_val = values[0]['value']
        last_val = values[-1]['value']
        
        if isinstance(first_val, (int, float)) and isinstance(last_val, (int, float)):
            change = ((last_val - first_val) / first_val) * 100
            
            trend_type = 'increasing' if change > 0 else 'decreasing'
            significance = 'significant' if abs(change) > 10 else 'moderate'
            
            return {
                'type': 'trend',
                'metric': values[0].get('metric_name', 'metric'),
                'direction': trend_type,
                'change_percent': round(change, 1),
                'significance': significance,
                'period_start': values[0]['period'],
                'period_end': values[-1]['period'],
                'data_points': len(values)
            }
        
        return None
    
    def find_cross_company_patterns(
        self,
        company_insights: Dict[str, List[AnalysisInsight]]
    ) -> List[Dict[str, Any]]:
        """Find patterns across multiple companies"""
        
        patterns = []
        
        # Group insights by theme across companies
        theme_by_company = defaultdict(lambda: defaultdict(list))
        
        for company, insights in company_insights.items():
            for insight in insights:
                themes = self._identify_themes(insight.insight)
                for theme in themes:
                    theme_by_company[theme][company].append(insight)
        
        # Find common themes
        for theme, companies in theme_by_company.items():
            if len(companies) >= 2:  # Pattern found in multiple companies
                pattern = {
                    'type': 'cross_company_pattern',
                    'theme': theme,
                    'companies': list(companies.keys()),
                    'company_count': len(companies),
                    'examples': []
                }
                
                # Add examples from each company
                for company, insights in companies.items():
                    if insights:
                        pattern['examples'].append({
                            'company': company,
                            'insight': insights[0].insight
                        })
                
                patterns.append(pattern)
        
        return patterns
    
    def rank_insights_by_importance(
        self,
        insights: List[AnalysisInsight],
        criteria: Optional[Dict[str, float]] = None
    ) -> List[Tuple[float, AnalysisInsight]]:
        """Rank insights by importance"""
        
        # Default importance criteria
        if criteria is None:
            criteria = {
                'confidence': 0.3,
                'evidence_count': 0.2,
                'financial_impact': 0.3,
                'strategic_importance': 0.2
            }
        
        ranked_insights = []
        
        for insight in insights:
            score = 0.0
            
            # Base score from confidence
            score += insight.confidence * criteria.get('confidence', 0.3)
            
            # Score from evidence
            evidence_score = min(len(insight.supporting_evidence) / 5, 1.0)
            score += evidence_score * criteria.get('evidence_count', 0.2)
            
            # Score from financial impact (if metrics present)
            if insight.metrics:
                # Simple heuristic: higher numbers = higher impact
                impact_score = 0.5  # Base score for having metrics
                score += impact_score * criteria.get('financial_impact', 0.3)
            
            # Score from strategic importance (based on keywords)
            strategic_keywords = ['strategic', 'transformation', 'competitive', 'market position']
            if any(keyword in insight.insight.lower() for keyword in strategic_keywords):
                score += 1.0 * criteria.get('strategic_importance', 0.2)
            
            ranked_insights.append((score, insight))
        
        # Sort by score descending
        ranked_insights.sort(key=lambda x: x[0], reverse=True)
        
        return ranked_insights
    
    def generate_executive_summary(
        self,
        insights: List[AnalysisInsight],
        company_name: str,
        analysis_period: str
    ) -> str:
        """Generate executive summary from insights"""
        
        # Rank insights
        ranked = self.rank_insights_by_importance(insights)
        
        # Take top insights
        top_insights = [insight for _, insight in ranked[:5]]
        
        # Categorize
        financial_insights = [i for i in top_insights if i.category == 'financial']
        strategic_insights = [i for i in top_insights if i.category == 'strategic']
        risk_insights = [i for i in top_insights if i.category == 'risk']
        
        # Build summary
        summary_parts = [
            f"Executive Summary for {company_name} ({analysis_period})"
        ]
        
        if financial_insights:
            summary_parts.append(
                f"\nFinancial Performance: {financial_insights[0].insight}"
            )
        
        if strategic_insights:
            summary_parts.append(
                f"\nStrategic Position: {strategic_insights[0].insight}"
            )
        
        if risk_insights:
            summary_parts.append(
                f"\nKey Risks: {risk_insights[0].insight}"
            )
        
        # Add overall assessment
        overall_sentiment = self._assess_overall_sentiment(insights)
        summary_parts.append(
            f"\nOverall Assessment: {overall_sentiment}"
        )
        
        return "\n".join(summary_parts)
    
    def _assess_overall_sentiment(self, insights: List[AnalysisInsight]) -> str:
        """Assess overall sentiment from insights"""
        
        positive_count = 0
        negative_count = 0
        
        for insight in insights:
            themes = self._identify_themes(insight.insight)
            if 'growth' in themes or 'opportunity' in themes:
                positive_count += 1
            elif 'decline' in themes or 'risk' in themes:
                negative_count += 1
        
        if positive_count > negative_count * 2:
            return "The company shows strong positive momentum with multiple growth drivers."
        elif negative_count > positive_count * 2:
            return "The company faces significant challenges that require careful monitoring."
        else:
            return "The company shows mixed signals with both opportunities and challenges ahead."