from typing import List, Dict, Tuple
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
from fuzzywuzzy import fuzz

@dataclass
class MatchConfig:
    """Configuration for matching tolerances"""
    price_tolerance_percent: float = 0.01  # 1% tolerance
    price_tolerance_absolute: float = 0.01  # Or $0.01
    quantity_tolerance_percent: float = 0.001  # 0.1% tolerance
    time_window_hours: int = 24  # Match trades within 24 hours
    min_match_score: float = 0.85  # Minimum fuzzy match score

class TradeMatchingEngine:
    """Core matching engine for trade reconciliation"""
    
    def __init__(self, config: MatchConfig = None):
        self.config = config or MatchConfig()
        self.match_results = []
    
    def match_trades(self, internal_trades: List[Dict], 
                    external_trades: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Match internal trades against external trades
        Returns: (matched_pairs, unmatched_trades)
        """
        internal_df = pd.DataFrame(internal_trades)
        external_df = pd.DataFrame(external_trades)
        
        matches = []
        breaks = []
        
        # Convert to indexed structure for faster lookups
        external_by_key = self._create_lookup_index(external_df)
        matched_external_ids = set()
        
        for idx, internal_trade in internal_df.iterrows():
            match_result = self._find_best_match(
                internal_trade, 
                external_by_key, 
                matched_external_ids
            )
            
            if match_result['matched']:
                matches.append({
                    'internal_trade': internal_trade.to_dict(),
                    'external_trade': match_result['external_trade'],
                    'match_score': match_result['score'],
                    'match_method': match_result['method']
                })
                matched_external_ids.add(match_result['external_trade']['id'])
            else:
                breaks.append({
                    'trade': internal_trade.to_dict(),
                    'break_type': 'MISSING_EXTERNAL_TRADE',
                    'severity': 'HIGH'
                })
        
        # Find unmatched external trades
        for idx, external_trade in external_df.iterrows():
            if external_trade['id'] not in matched_external_ids:
                breaks.append({
                    'trade': external_trade.to_dict(),
                    'break_type': 'MISSING_INTERNAL_TRADE',
                    'severity': 'HIGH'
                })
        
        return matches, breaks
    
    def _create_lookup_index(self, df: pd.DataFrame) -> Dict:
        """Create indexed structure for faster lookups"""
        index = {}
        
        for idx, row in df.iterrows():
            # Create multiple keys for flexible matching
            keys = [
                row['instrument_id'],
                f"{row['instrument_id']}_{row['counterparty']}",
                row['trade_id']
            ]
            
            for key in keys:
                if key not in index:
                    index[key] = []
                index[key].append(row.to_dict())
        
        return index
    
    def _find_best_match(self, internal_trade: pd.Series, 
                         external_index: Dict, 
                         matched_ids: set) -> Dict:
        """Find best matching external trade"""
        
        # Try exact match first
        candidates = self._get_candidates(internal_trade, external_index, matched_ids)
        
        if not candidates:
            return {'matched': False}
        
        best_match = None
        best_score = 0
        
        for candidate in candidates:
            score = self._calculate_match_score(internal_trade, candidate)
            
            if score > best_score and score >= self.config.min_match_score:
                best_score = score
                best_match = candidate
        
        if best_match:
            # Validate match meets tolerances
            if self._validate_match(internal_trade, best_match):
                return {
                    'matched': True,
                    'external_trade': best_match,
                    'score': best_score,
                    'method': 'ALGORITHMIC'
                }
        
        return {'matched': False}
    
    def _get_candidates(self, internal_trade: pd.Series, 
                       external_index: Dict, 
                       matched_ids: set) -> List[Dict]:
        """Get candidate external trades for matching"""
        candidates = []
        
        # Try different keys
        keys = [
            internal_trade['instrument_id'],
            f"{internal_trade['instrument_id']}_{internal_trade['counterparty']}",
            internal_trade['trade_id']
        ]
        
        for key in keys:
            if key in external_index:
                for trade in external_index[key]:
                    if trade['id'] not in matched_ids:
                        # Check time window
                        time_diff = abs((internal_trade['trade_date'] - 
                                       trade['trade_date']).total_seconds() / 3600)
                        if time_diff <= self.config.time_window_hours:
                            candidates.append(trade)
        
        return candidates
    
    def _calculate_match_score(self, internal: pd.Series, external: Dict) -> float:
        """Calculate similarity score between trades"""
        scores = []
        
        # Exact matches (weight: 1.0)
        if internal['instrument_id'] == external['instrument_id']:
            scores.append(1.0)
        else:
            scores.append(0.0)
        
        # Fuzzy match counterparty (weight: 0.8)
        counterparty_score = fuzz.ratio(
            str(internal['counterparty']).upper(),
            str(external['counterparty']).upper()
        ) / 100.0
        scores.append(counterparty_score * 0.8)
        
        # Price proximity (weight: 0.9)
        price_diff_pct = abs(internal['price'] - external['price']) / internal['price']
        if price_diff_pct <= self.config.price_tolerance_percent:
            price_score = 1.0 - (price_diff_pct / self.config.price_tolerance_percent)
        else:
            price_score = 0.0
        scores.append(price_score * 0.9)
        
        # Quantity proximity (weight: 0.9)
        qty_diff_pct = abs(internal['quantity'] - external['quantity']) / internal['quantity']
        if qty_diff_pct <= self.config.quantity_tolerance_percent:
            qty_score = 1.0 - (qty_diff_pct / self.config.quantity_tolerance_percent)
        else:
            qty_score = 0.0
        scores.append(qty_score * 0.9)
        
        # Time proximity (weight: 0.6)
        time_diff_hours = abs((internal['trade_date'] - 
                              external['trade_date']).total_seconds() / 3600)
        time_score = max(0, 1.0 - (time_diff_hours / self.config.time_window_hours))
        scores.append(time_score * 0.6)
        
        # Weighted average
        return np.mean(scores)
    
    def _validate_match(self, internal: pd.Series, external: Dict) -> bool:
        """Validate match meets all tolerance thresholds"""
        
        # Price check
        price_diff_pct = abs(internal['price'] - external['price']) / internal['price']
        price_diff_abs = abs(internal['price'] - external['price'])
        
        if (price_diff_pct > self.config.price_tolerance_percent and 
            price_diff_abs > self.config.price_tolerance_absolute):
            return False
        
        # Quantity check
        qty_diff_pct = abs(internal['quantity'] - external['quantity']) / internal['quantity']
        if qty_diff_pct > self.config.quantity_tolerance_percent:
            return False
        
        # Must match on key fields
        if internal['instrument_id'] != external['instrument_id']:
            return False
        
        return True
    
    def identify_breaks(self, internal: pd.Series, external: Dict) -> List[Dict]:
        """Identify specific breaks in a matched pair"""
        breaks = []
        
        # Price mismatch
        price_diff_pct = abs(internal['price'] - external['price']) / internal['price']
        if price_diff_pct > self.config.price_tolerance_percent:
            breaks.append({
                'break_type': 'PRICE_MISMATCH',
                'severity': 'HIGH' if price_diff_pct > 0.05 else 'MEDIUM',
                'expected_value': internal['price'],
                'actual_value': external['price'],
                'difference': external['price'] - internal['price']
            })
        
        # Quantity mismatch
        qty_diff_pct = abs(internal['quantity'] - external['quantity']) / internal['quantity']
        if qty_diff_pct > self.config.quantity_tolerance_percent:
            breaks.append({
                'break_type': 'QUANTITY_MISMATCH',
                'severity': 'HIGH' if qty_diff_pct > 0.01 else 'MEDIUM',
                'expected_value': internal['quantity'],
                'actual_value': external['quantity'],
                'difference': external['quantity'] - internal['quantity']
            })
        
        # Settlement date mismatch
        if internal['settlement_date'] != external['settlement_date']:
            breaks.append({
                'break_type': 'SETTLEMENT_DATE_MISMATCH',
                'severity': 'LOW',
                'expected_value': internal['settlement_date'],
                'actual_value': external['settlement_date']
            })
        
        return breaks
