from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc
from typing import List, Optional
from datetime import date, datetime, timedelta
from database.models import Trade, Break, ReconciliationRun

class TradeRepository:
    """Repository for Trade operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, trade_id: int) -> Optional[Trade]:
        return self.db.query(Trade).filter(Trade.id == trade_id).first()
    
    def search(self,
               trade_date_from: Optional[date] = None,
               trade_date_to: Optional[date] = None,
               instrument_id: Optional[str] = None,
               counterparty: Optional[str] = None,
               status: Optional[str] = None,
               source: Optional[str] = None,
               skip: int = 0,
               limit: int = 100) -> List[Trade]:
        
        query = self.db.query(Trade)
        
        if trade_date_from:
            query = query.filter(Trade.trade_date >= trade_date_from)
        if trade_date_to:
            query = query.filter(Trade.trade_date <= trade_date_to)
        if instrument_id:
            query = query.filter(Trade.instrument_id.ilike(f"%{instrument_id}%"))
        if counterparty:
            query = query.filter(Trade.counterparty.ilike(f"%{counterparty}%"))
        if status:
            query = query.filter(Trade.status == status)
        if source:
            query = query.filter(Trade.source == source)
        
        return query.offset(skip).limit(limit).all()
    
    def get_unmatched_trades(self, trade_date: date) -> List[Trade]:
        return self.db.query(Trade).filter(
            and_(
                Trade.trade_date == trade_date,
                Trade.status == 'UNMATCHED'
            )
        ).all()

class BreakRepository:
    """Repository for Break operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, break_id: int) -> Optional[Break]:
        return self.db.query(Break).filter(Break.id == break_id).first()
    
    def search(self,
               status: Optional[str] = None,
               severity: Optional[str] = None,
               break_type: Optional[str] = None,
               assigned_to: Optional[str] = None,
               created_from: Optional[date] = None,
               created_to: Optional[date] = None,
               skip: int = 0,
               limit: int = 100) -> List[Break]:
        
        query = self.db.query(Break)
        
        if status:
            query = query.filter(Break.status == status)
        if severity:
            query = query.filter(Break.severity == severity)
        if break_type:
            query = query.filter(Break.break_type == break_type)
        if assigned_to:
            query = query.filter(Break.assigned_to == assigned_to)
        if created_from:
            query = query.filter(Break.created_at >= created_from)
        if created_to:
            query = query.filter(Break.created_at <= created_to)
        
        return query.order_by(desc(Break.created_at)).offset(skip).limit(limit).all()
    
    def get_by_date_range(self, date_from: date, date_to: date) -> List[Break]:
        return self.db.query(Break).filter(
            and_(
                Break.created_at >= date_from,
                Break.created_at <= date_to
            )
        ).all()
    
    def count_by_status(self, status: str) -> int:
        return self.db.query(Break).filter(Break.status == status).count()
    
    def count_by_severity(self, severity: str) -> int:
        return self.db.query(Break).filter(Break.severity == severity).count()
    
    def count_sla_breached(self) -> int:
        # Breaks older than 24 hours and still open
        threshold = datetime.utcnow() - timedelta(hours=24)
        return self.db.query(Break).filter(
            and_(
                Break.created_at < threshold,
                Break.status.in_(['OPEN', 'ASSIGNED'])
            )
        ).count()
    
    def get_aging_analysis(self) -> dict:
        now = datetime.utcnow()
        
        return {
            '0-24h': self.db.query(Break).filter(
                and_(
                    Break.created_at >= now - timedelta(hours=24),
                    Break.status != 'RESOLVED'
                )
            ).count(),
            '24-48h': self.db.query(Break).filter(
                and_(
                    Break.created_at >= now - timedelta(hours=48),
                    Break.created_at < now - timedelta(hours=24),
                    Break.status != 'RESOLVED'
                )
            ).count(),
            '48h+': self.db.query(Break).filter(
                and_(
                    Break.created_at < now - timedelta(hours=48),
                    Break.status != 'RESOLVED'
                )
            ).count()
        }
    
    def get_top_counterparties_with_breaks(self, date_from: date, date_to: date, limit: int = 10):
        from sqlalchemy import func
        
        results = self.db.query(
            Trade.counterparty,
            func.count(Break.id).label('break_count')
        ).join(
            Break, Break.trade_id == Trade.id
        ).filter(
            and_(
                Break.created_at >= date_from,
                Break.created_at <= date_to
            )
        ).group_by(
            Trade.counterparty
        ).order_by(
            desc('break_count')
        ).limit(limit).all()
        
        return [{'counterparty': r[0], 'break_count': r[1]} for r in results]

class ReconciliationRepository:
    """Repository for ReconciliationRun operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, run_id: int) -> Optional[ReconciliationRun]:
        return self.db.query(ReconciliationRun).filter(ReconciliationRun.id == run_id).first()
    
    def get_by_trade_date(self, trade_date: date) -> Optional[ReconciliationRun]:
        return self.db.query(ReconciliationRun).filter(
            ReconciliationRun.trade_date == trade_date
        ).first()
    
    def get_all(self, skip: int = 0, limit: int = 20, status: Optional[str] = None) -> List[ReconciliationRun]:
        query = self.db.query(ReconciliationRun)
        
        if status:
            query = query.filter(ReconciliationRun.status == status)
        
        return query.order_by(desc(ReconciliationRun.run_date)).offset(skip).limit(limit).all()
    
    def get_by_date_range(self, date_from: date, date_to: date) -> List[ReconciliationRun]:
        return self.db.query(ReconciliationRun).filter(
            and_(
                ReconciliationRun.trade_date >= date_from,
                ReconciliationRun.trade_date <= date_to
            )
        ).all()
    
    def get_statistics(self, date_from: date, date_to: date) -> dict:
        runs = self.get_by_date_range(date_from, date_to)
        
        if not runs:
            return {
                'total_runs': 0,
                'successful_runs': 0,
                'total_trades': 0,
                'total_matches': 0,
                'total_breaks': 0,
                'total_auto_resolved': 0,
                'auto_resolution_rate': 0,
                'avg_processing_time': 0,
                'success_rate': 0
            }
        
        total_runs = len(runs)
        successful_runs = len([r for r in runs if r.status == 'COMPLETED'])
        total_trades = sum((r.total_internal_trades or 0) + (r.tot
