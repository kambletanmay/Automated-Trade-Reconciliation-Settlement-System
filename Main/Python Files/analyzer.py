from typing import List, Dict, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import numpy as np

class BreakAnalyzer:
    """Analyze breaks to identify patterns and root causes"""
    
    def __init__(self):
        self.break_patterns = defaultdict(list)
        
    def categorize_break(self, break_data: Dict) -> Dict:
        """Categorize and enrich break with additional metadata"""
        break_type = break_data.get('break_type')
        severity = self._determine_severity(break_data)
        root_cause = self._identify_root_cause(break_data)
        
        return {
            **break_data,
            'severity': severity,
            'root_cause_category': root_cause,
            'auto_resolvable': self._is_auto_resolvable(break_data),
            'sla_hours': self._get_sla_hours(severity),
            'priority_score': self._calculate_priority_score(break_data, severity)
        }
    
    def _determine_severity(self, break_data: Dict) -> str:
        """Determine break severity based on multiple factors"""
        break_type = break_data.get('break_type')
        
        # Missing trades are always high severity
        if 'MISSING' in break_type:
            return 'CRITICAL'
        
        # Check monetary impact
        if 'difference' in break_data:
            diff = abs(float(break_data['difference']))
            
            # For price mismatches
            if 'PRICE' in break_type:
                trade = break_data.get('trade', {})
                quantity = trade.get('quantity', 0)
                impact = diff * quantity
                
                if impact > 100000:
                    return 'CRITICAL'
                elif impact > 10000:
                    return 'HIGH'
                elif impact > 1000:
                    return 'MEDIUM'
                else:
                    return 'LOW'
            
            # For quantity mismatches
            if 'QUANTITY' in break_type:
                trade = break_data.get('trade', {})
                price = trade.get('price', 0)
                impact = diff * price
                
                if impact > 100000:
                    return 'CRITICAL'
                elif impact > 10000:
                    return 'HIGH'
                else:
                    return 'MEDIUM'
        
        # Default severity based on break type
        severity_map = {
            'SETTLEMENT_DATE_MISMATCH': 'MEDIUM',
            'COUNTERPARTY_MISMATCH': 'HIGH',
            'ACCOUNT_MISMATCH': 'HIGH',
            'CURRENCY_MISMATCH': 'CRITICAL'
        }
        
        return severity_map.get(break_type, 'MEDIUM')
    
    def _identify_root_cause(self, break_data: Dict) -> str:
        """Identify likely root cause category"""
        break_type = break_data.get('break_type')
        trade = break_data.get('trade', {})
        
        # Pattern-based root cause identification
        if 'MISSING_EXTERNAL' in break_type:
            # Check if near EOD
            trade_time = trade.get('trade_date')
            if trade_time and trade_time.hour >= 16:
                return 'LATE_BOOKING'
            return 'BROKER_FEED_ISSUE'
        
        if 'MISSING_INTERNAL' in break_type:
            return 'INTERNAL_BOOKING_ERROR'
        
        if 'PRICE_MISMATCH' in break_type:
            diff_pct = abs(break_data.get('difference', 0)) / break_data.get('expected_value', 1)
            if diff_pct > 0.1:
                return 'DATA_ENTRY_ERROR'
            else:
                return 'ROUNDING_DIFFERENCE'
        
        if 'QUANTITY_MISMATCH' in break_type:
            return 'PARTIAL_FILL'
        
        return 'UNKNOWN'
    
    def _is_auto_resolvable(self, break_data: Dict) -> bool:
        """Determine if break can be auto-resolved"""
        break_type = break_data.get('break_type')
        severity = break_data.get('severity', 'HIGH')
        
        # Only auto-resolve low severity, specific break types
        if severity in ['CRITICAL', 'HIGH']:
            return False
        
        auto_resolvable_types = [
            'SETTLEMENT_DATE_MISMATCH',  # If within T+1/T+2
            'ROUNDING_DIFFERENCE'
        ]
        
        root_cause = break_data.get('root_cause_category')
        if root_cause in auto_resolvable_types:
            return True
        
        # Price/quantity differences within tight tolerances
        if 'MISMATCH' in break_type and 'difference' in break_data:
            if abs(break_data['difference']) < 0.01:  # Less than 1 cent
                return True
        
        return False
    
    def _get_sla_hours(self, severity: str) -> int:
        """Get SLA hours based on severity"""
        sla_map = {
            'CRITICAL': 2,
            'HIGH': 4,
            'MEDIUM': 24,
            'LOW': 48
        }
        return sla_map.get(severity, 24)
    
    def _calculate_priority_score(self, break_data: Dict, severity: str) -> int:
        """Calculate priority score for break resolution queue"""
        base_scores = {
            'CRITICAL': 1000,
            'HIGH': 500,
            'MEDIUM': 100,
            'LOW': 10
        }
        
        score = base_scores.get(severity, 100)
        
        # Increase priority for aged breaks
        created_at = break_data.get('created_at')
        if created_at:
            age_hours = (datetime.utcnow() - created_at).total_seconds() / 3600
            score += int(age_hours * 10)
        
        # Increase priority for high-value trades
        trade = break_data.get('trade', {})
        notional = trade.get('price', 0) * trade.get('quantity', 0)
        if notional > 1000000:
            score += 200
        elif notional > 100000:
            score += 100
        
        return score
    
    def detect_patterns(self, breaks: List[Dict]) -> List[Dict]:
        """Detect patterns in breaks using clustering"""
        if len(breaks) < 5:
            return []
        
        # Convert breaks to feature vectors
        features = []
        break_metadata = []
        
        for b in breaks:
            trade = b.get('trade', {})
            features.append([
                hash(trade.get('counterparty', '')) % 1000,  # Counterparty encoding
                hash(trade.get('instrument_id', '')) % 1000,  # Instrument encoding
                hash(b.get('break_type', '')) % 100,  # Break type encoding
                b.get('priority_score', 0),
                trade.get('price', 0),
                trade.get('quantity', 0)
            ])
            break_metadata.append(b)
        
        # Normalize features
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        
        # Cluster breaks
        clustering = DBSCAN(eps=0.5, min_samples=3)
        labels = clustering.fit_predict(features_scaled)
        
        # Analyze clusters
        patterns = []
        for cluster_id in set(labels):
            if cluster_id == -1:  # Noise
                continue
            
            cluster_breaks = [b for i, b in enumerate(break_metadata) if labels[i] == cluster_id]
            
            if len(cluster_breaks) >= 3:
                pattern = self._analyze_cluster(cluster_breaks)
                patterns.append(pattern)
        
        return patterns
    
    def _analyze_cluster(self, cluster_breaks: List[Dict]) -> Dict:
        """Analyze a cluster of breaks to identify common characteristics"""
        # Find common attributes
        common_counterparty = self._find_common_value(cluster_breaks, 'trade.counterparty')
        common_break_type = self._find_common_value(cluster_breaks, 'break_type')
        common_root_cause = self._find_common_value(cluster_breaks, 'root_cause_category')
        
        # Calculate impact
        total_impact = sum(
            abs(b.get('difference', 0)) * b.get('trade', {}).get('quantity', 1)
            for b in cluster_breaks
        )
        
        return {
            'pattern_id': hash(f"{common_counterparty}_{common_break_type}") % 100000,
            'break_count': len(cluster_breaks),
            'common_counterparty': common_counterparty,
            'common_break_type': common_break_type,
            'common_root_cause': common_root_cause,
            'total_impact': total_impact,
            'first_occurrence': min(b.get('created_at', datetime.utcnow()) for b in cluster_breaks),
            'last_occurrence': max(b.get('created_at', datetime.utcnow()) for b in cluster_breaks),
            'severity': 'HIGH' if len(cluster_breaks) > 10 else 'MEDIUM',
            'recommendation': self._generate_recommendation(cluster_breaks)
        }
    
    def _find_common_value(self, breaks: List[Dict], key_path: str):
        """Find most common value for a nested key"""
        values = []
        for b in breaks:
            keys = key_path.split('.')
            val = b
            for k in keys:
                val = val.get(k, {})
            if val and not isinstance(val, dict):
                values.append(val)
        
        if not values:
            return None
        
        # Return most common value
        from collections import Counter
        return Counter(values).most_common(1)[0][0]
    
    def _generate_recommendation(self, cluster_breaks: List[Dict]) -> str:
        """Generate recommendation for pattern resolution"""
        common_cause = self._find_common_value(cluster_breaks, 'root_cause_category')
        
        recommendations = {
            'BROKER_FEED_ISSUE': 'Contact broker operations team to investigate feed delays/failures',
            'DATA_ENTRY_ERROR': 'Implement additional validation checks in trade entry system',
            'PARTIAL_FILL': 'Review order execution and auto-match partial fills',
            'ROUNDING_DIFFERENCE': 'Align rounding rules between systems',
            'LATE_BOOKING': 'Escalate to trading desk for EOD booking procedures review'
        }
        
        return recommendations.get(common_cause, 'Manual investigation required')
    
    def generate_break_report(self, breaks: List[Dict], 
                             start_date: datetime, 
                             end_date: datetime) -> Dict:
        """Generate comprehensive break analysis report"""
        df = pd.DataFrame(breaks)
        
        if df.empty:
            return {
                'period': f"{start_date.date()} to {end_date.date()}",
                'total_breaks': 0,
                'by_severity': {},
                'by_type': {},
                'by_counterparty': {},
                'aging_analysis': {},
                'patterns': []
            }
        
        # Analysis by severity
        by_severity = df['severity'].value_counts().to_dict()
        
        # Analysis by type
        by_type = df['break_type'].value_counts().to_dict()
        
        # Analysis by counterparty
        by_counterparty = df.apply(
            lambda x: x['trade'].get('counterparty') if 'trade' in x else None, 
            axis=1
        ).value_counts().head(10).to_dict()
        
        # Aging analysis
        df['age_days'] = df['created_at'].apply(
            lambda x: (datetime.utcnow() - x).days if pd.notna(x) else 0
        )
        
        aging_analysis = {
            '0-1_days': len(df[df['age_days'] <= 1]),
            '1-3_days': len(df[(df['age_days'] > 1) & (df['age_days'] <= 3)]),
            '3-7_days': len(df[(df['age_days'] > 3) & (df['age_days'] <= 7)]),
            '7+_days': len(df[df['age_days'] > 7])
        }
        
        # Detect patterns
        patterns = self.detect_patterns(breaks)
        
        return {
            'period': f"{start_date.date()} to {end_date.date()}",
            'total_breaks': len(breaks),
            'by_severity': by_severity,
            'by_type': by_type,
            'by_counterparty': by_counterparty,
            'aging_analysis': aging_analysis,
            'patterns': patterns,
            'top_priority_breaks': sorted(
                breaks, 
                key=lambda x: x.get('priority_score', 0), 
                reverse=True
            )[:10]
        }


class AutoResolver:
    """Automatically resolve breaks that meet criteria"""
    
    def __init__(self, db_session):
        self.db = db_session
        self.resolution_rules = self._load_resolution_rules()
    
    def _load_resolution_rules(self) -> List[Dict]:
        """Load automated resolution rules"""
        return [
            {
                'name': 'SETTLEMENT_DATE_T_PLUS_ADJUSTMENT',
                'condition': lambda b: (
                    b['break_type'] == 'SETTLEMENT_DATE_MISMATCH' and
                    abs((b['expected_value'] - b['actual_value']).days) <= 1
                ),
                'action': 'ACCEPT_EXTERNAL',
                'reason': 'Settlement date within T+1 tolerance'
            },
            {
                'name': 'PENNY_ROUNDING',
                'condition': lambda b: (
                    b['break_type'] == 'PRICE_MISMATCH' and
                    abs(b['difference']) <= 0.01
                ),
                'action': 'ACCEPT_EXTERNAL',
                'reason': 'Price difference within rounding tolerance (1 cent)'
            },
            {
                'name': 'QUANTITY_ROUNDING',
                'condition': lambda b: (
                    b['break_type'] == 'QUANTITY_MISMATCH' and
                    abs(b['difference']) < 0.01
                ),
                'action': 'ACCEPT_INTERNAL',
                'reason': 'Quantity difference negligible (< 0.01 shares)'
            },
            {
                'name': 'KNOWN_COUNTERPARTY_MAPPING',
                'condition': lambda b: (
                    b['break_type'] == 'COUNTERPARTY_MISMATCH' and
                    self._check_counterparty_alias(b['expected_value'], b['actual_value'])
                ),
                'action': 'UPDATE_MAPPING',
                'reason': 'Counterparty names are known aliases'
            }
        ]
    
    def _check_counterparty_alias(self, name1: str, name2: str) -> bool:
        """Check if counterparty names are known aliases"""
        # In production, this would query a mapping table
        known_mappings = {
            ('JPMORGAN CHASE', 'JPM'): True,
            ('GOLDMAN SACHS', 'GS'): True,
            ('MORGAN STANLEY', 'MS'): True,
        }
        
        key = (name1.upper(), name2.upper())
        return known_mappings.get(key, False) or known_mappings.get((key[1], key[0]), False)
    
    def attempt_auto_resolve(self, break_record: Dict) -> Dict:
        """Attempt to automatically resolve a break"""
        for rule in self.resolution_rules:
            try:
                if rule['condition'](break_record):
                    return {
                        'resolved': True,
                        'rule_applied': rule['name'],
                        'action': rule['action'],
                        'reason': rule['reason'],
                        'timestamp': datetime.utcnow()
                    }
            except Exception as e:
                print(f"Error applying rule {rule['name']}: {e}")
                continue
        
        return {'resolved': False}
    
    def batch_auto_resolve(self, breaks: List[Dict]) -> Dict:
        """Attempt to auto-resolve a batch of breaks"""
        results = {
            'total_breaks': len(breaks),
            'auto_resolved': 0,
            'failed_resolution': 0,
            'resolutions': []
        }
        
        for break_record in breaks:
            if break_record.get('auto_resolvable'):
                resolution = self.attempt_auto_resolve(break_record)
                
                if resolution['resolved']:
                    # Update database
                    self._update_break_status(
                        break_record['id'],
                        'AUTO_RESOLVED',
                        resolution
                    )
                    results['auto_resolved'] += 1
                    results['resolutions'].append({
                        'break_id': break_record['id'],
                        **resolution
                    })
                else:
                    results['failed_resolution'] += 1
        
        return results
    
    def _update_break_status(self, break_id: int, status: str, resolution: Dict):
        """Update break status in database"""
        # This would update the Break table
        from database.models import Break
        
        break_obj = self.db.query(Break).filter(Break.id == break_id).first()
        if break_obj:
            break_obj.status = status
            break_obj.resolved_at = datetime.utcnow()
            break_obj.resolution_notes = f"Auto-resolved: {resolution['reason']}"
            self.db.commit()
