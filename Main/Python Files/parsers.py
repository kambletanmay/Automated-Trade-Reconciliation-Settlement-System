import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
import json
from typing import List, Dict
import xml.etree.ElementTree as ET

class TradeParser(ABC):
    """Abstract base class for trade parsers"""
    
    @abstractmethod
    def parse(self, file_path: str) -> List[Dict]:
        """Parse trade file and return list of standardized trade dictionaries"""
        pass
    
    def normalize_trade(self, raw_trade: Dict, source: str) -> Dict:
        """Convert to standardized format"""
        return {
            'trade_id': raw_trade.get('trade_id'),
            'source': source,
            'trade_date': self._parse_date(raw_trade.get('trade_date')),
            'settlement_date': self._parse_date(raw_trade.get('settlement_date')),
            'instrument_id': raw_trade.get('instrument_id'),
            'instrument_name': raw_trade.get('instrument_name'),
            'quantity': float(raw_trade.get('quantity', 0)),
            'price': float(raw_trade.get('price', 0)),
            'currency': raw_trade.get('currency', 'USD'),
            'counterparty': raw_trade.get('counterparty'),
            'account': raw_trade.get('account'),
            'raw_data': json.dumps(raw_trade)
        }
    
    def _parse_date(self, date_str: str) -> datetime:
        """Handle multiple date formats"""
        formats = ['%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%m/%d/%Y']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except (ValueError, TypeError):
                continue
        raise ValueError(f"Unable to parse date: {date_str}")

class CSVTradeParser(TradeParser):
    """Parse CSV trade files"""
    
    def __init__(self, column_mapping: Dict[str, str] = None):
        """
        column_mapping: Dict mapping CSV columns to standard field names
        Example: {'TradeID': 'trade_id', 'Symbol': 'instrument_id'}
        """
        self.column_mapping = column_mapping or {}
    
    def parse(self, file_path: str, source: str) -> List[Dict]:
        df = pd.read_csv(file_path)
        
        # Rename columns if mapping provided
        if self.column_mapping:
            df = df.rename(columns=self.column_mapping)
        
        trades = []
        for _, row in df.iterrows():
            raw_trade = row.to_dict()
            try:
                normalized = self.normalize_trade(raw_trade, source)
                trades.append(normalized)
            except Exception as e:
                print(f"Error parsing trade {row.get('trade_id')}: {e}")
                continue
        
        return trades

class FIXMessageParser(TradeParser):
    """Parse FIX protocol messages"""
    
    FIX_TAG_MAP = {
        '11': 'trade_id',
        '55': 'instrument_id',
        '54': 'side',  # 1=Buy, 2=Sell
        '38': 'quantity',
        '44': 'price',
        '15': 'currency',
        '75': 'trade_date',
        '64': 'settlement_date',
    }
    
    def parse(self, file_path: str, source: str) -> List[Dict]:
        trades = []
        
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    raw_trade = self._parse_fix_message(line)
                    normalized = self.normalize_trade(raw_trade, source)
                    trades.append(normalized)
                except Exception as e:
                    print(f"Error parsing FIX message: {e}")
                    continue
        
        return trades
    
    def _parse_fix_message(self, message: str) -> Dict:
        """Parse FIX message (pipe-delimited tag=value pairs)"""
        fields = message.strip().split('|')
        trade_dict = {}
        
        for field in fields:
            if '=' in field:
                tag, value = field.split('=', 1)
                field_name = self.FIX_TAG_MAP.get(tag, f'tag_{tag}')
                trade_dict[field_name] = value
        
        return trade_dict

class MT541Parser(TradeParser):
    """Parse SWIFT MT541 settlement messages"""
    
    def parse(self, file_path: str, source: str) -> List[Dict]:
        # Simplified MT541 parser (real implementation would be more complex)
        trades = []
        
        with open(file_path, 'r') as f:
            content = f.read()
            # Parse SWIFT message blocks
            # This is simplified - real SWIFT parsing is more complex
            
        return trades

class DatabaseConnector:
    """Connect to internal trading system database"""
    
    def __init__(self, connection_string: str):
        from sqlalchemy import create_engine
        self.engine = create_engine(connection_string)
    
    def extract_trades(self, trade_date: datetime, source: str = 'INTERNAL') -> List[Dict]:
        """Extract trades from internal database"""
        query = f"""
        SELECT 
            trade_id,
            trade_date,
            settlement_date,
            instrument_id,
            instrument_name,
            quantity,
            price,
            currency,
            counterparty,
            account
        FROM trades
        WHERE trade_date = '{trade_date.strftime('%Y-%m-%d')}'
        """
        
        df = pd.read_sql(query, self.engine)
        
        trades = []
        for _, row in df.iterrows():
            trade = row.to_dict()
            trade['source'] = source
            trade['raw_data'] = json.dumps(trade)
            trades.append(trade)
        
        return trades
