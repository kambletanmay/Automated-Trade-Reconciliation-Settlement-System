from sqlalchemy import Column, Integer, String, Float, DateTime, Enum, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

Base = declarative_base()

class TradeSource(enum.Enum):
    INTERNAL = "internal"
    BROKER_A = "broker_a"
    BROKER_B = "broker_b"
    CUSTODIAN = "custodian"

class TradeStatus(enum.Enum):
    UNMATCHED = "unmatched"
    MATCHED = "matched"
    BREAK = "break"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"

class Trade(Base):
    __tablename__ = 'trades'
    
    id = Column(Integer, primary_key=True)
    trade_id = Column(String(50), nullable=False, index=True)
    source = Column(Enum(TradeSource), nullable=False)
    trade_date = Column(DateTime, nullable=False, index=True)
    settlement_date = Column(DateTime, nullable=False)
    
    # Trade details
    instrument_id = Column(String(50), nullable=False, index=True)  # ISIN, CUSIP, etc.
    instrument_name = Column(String(200))
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    currency = Column(String(3), nullable=False)
    counterparty = Column(String(100), nullable=False, index=True)
    account = Column(String(50), index=True)
    
    # Workflow
    status = Column(Enum(TradeStatus), default=TradeStatus.UNMATCHED, index=True)
    matched_trade_id = Column(Integer, ForeignKey('trades.id'), nullable=True)
    
    # Metadata
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    raw_data = Column(String)  # JSON blob of original data
    
    # Relationships
    matched_trade = relationship("Trade", remote_side=[id], foreign_keys=[matched_trade_id])
    breaks = relationship("Break", back_populates="trade")

class Break(Base):
    __tablename__ = 'breaks'
    
    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('trades.id'), nullable=False)
    matched_trade_id = Column(Integer, ForeignKey('trades.id'), nullable=True)
    
    break_type = Column(String(50), nullable=False)  # PRICE_MISMATCH, QUANTITY_DIFF, MISSING_TRADE
    severity = Column(String(20))  # LOW, MEDIUM, HIGH, CRITICAL
    description = Column(String(500))
    
    # Break details
    expected_value = Column(String(100))
    actual_value = Column(String(100))
    difference = Column(Float)
    
    # Workflow
    status = Column(String(50), default='OPEN')
    assigned_to = Column(String(100))
    resolution_notes = Column(String(1000))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    resolved_at = Column(DateTime)
    
    # Relationships
    trade = relationship("Trade", foreign_keys=[trade_id], back_populates="breaks")
    matched_trade = relationship("Trade", foreign_keys=[matched_trade_id])

class ReconciliationRun(Base):
    __tablename__ = 'reconciliation_runs'
    
    id = Column(Integer, primary_key=True)
    run_date = Column(DateTime, default=datetime.utcnow, index=True)
    trade_date = Column(DateTime, nullable=False)
    
    # Statistics
    total_internal_trades = Column(Integer)
    total_external_trades = Column(Integer)
    matched_trades = Column(Integer)
    new_breaks = Column(Integer)
    auto_resolved_breaks = Column(Integer)
    
    status = Column(String(20))  # RUNNING, COMPLETED, FAILED
    duration_seconds = Column(Float)
    error_message = Column(String(500))
