from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, date, timedelta
from pydantic import BaseModel, Field
import logging

from database.models import Trade, Break, ReconciliationRun, Base
from database.repositories import TradeRepository, BreakRepository, ReconciliationRepository
from orchestrator import ReconciliationOrchestrator
from breaks.analyzer import BreakAnalyzer
from breaks.workflow import WorkflowManager

# Pydantic models for request/response
class TradeResponse(BaseModel):
    id: int
    trade_id: str
    source: str
    trade_date: datetime
    settlement_date: datetime
    instrument_id: str
    instrument_name: Optional[str]
    quantity: float
    price: float
    currency: str
    counterparty: str
    account: Optional[str]
    status: str
    
    class Config:
        from_attributes = True

class BreakResponse(BaseModel):
    id: int
    trade_id: int
    break_type: str
    severity: str
    description: Optional[str]
    expected_value: Optional[str]
    actual_value: Optional[str]
    difference: Optional[float]
    status: str
    assigned_to: Optional[str]
    created_at: datetime
    resolved_at: Optional[datetime]
    
    class Config:
        from_attributes = True

class ReconciliationRunResponse(BaseModel):
    id: int
    run_date: datetime
    trade_date: datetime
    total_internal_trades: Optional[int]
    total_external_trades: Optional[int]
    matched_trades: Optional[int]
    new_breaks: Optional[int]
    auto_resolved_breaks: Optional[int]
    status: str
    duration_seconds: Optional[float]
    
    class Config:
        from_attributes = True

class ReconciliationRequest(BaseModel):
    trade_date: date
    force_rerun: bool = False

class BreakResolutionRequest(BaseModel):
    resolution_type: str = Field(..., description="ACCEPT_EXTERNAL, ACCEPT_INTERNAL, AMEND")
    notes: str
    user: str

class BreakAssignmentRequest(BaseModel):
    assigned_to: str
    user: str

class SearchRequest(BaseModel):
    trade_date_from: Optional[date] = None
    trade_date_to: Optional[date] = None
    instrument_id: Optional[str] = None
    counterparty: Optional[str] = None
    status: Optional[str] = None
    min_amount: Optional[float] = None

# Initialize FastAPI app
app = FastAPI(
    title="Trade Reconciliation API",
    description="API for automated trade reconciliation and break management",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database dependency
def get_db():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    engine = create_engine('postgresql://user:password@localhost/reconciliation')
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Logging
logger = logging.getLogger(__name__)

# ==================== RECONCILIATION ENDPOINTS ====================

@app.post("/api/reconciliation/run", response_model=ReconciliationRunResponse)
async def run_reconciliation(
    request: ReconciliationRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger a reconciliation run for a specific trade date
    """
    try:
        # Check if reconciliation already exists
        repo = ReconciliationRepository(db)
        existing_run = repo.get_by_trade_date(request.trade_date)
        
        if existing_run and not request.force_rerun:
            raise HTTPException(
                status_code=400,
                detail=f"Reconciliation already run for {request.trade_date}. Use force_rerun=true to rerun."
            )
        
        # Load configuration
        config = {
            'internal_db_connection': 'postgresql://user:password@localhost/trading_system',
            'broker_a_csv_path': f'/data/broker_a/{request.trade_date}.csv',
            'broker_b_fix_path': f'/data/broker_b/{request.trade_date}.fix',
            'matching': {
                'price_tolerance_percent': 0.01,
                'quantity_tolerance_percent': 0.001,
                'time_window_hours': 24
            },
            'notifications': {
                'smtp_server': 'smtp.firm.com',
                'smtp_port': 587,
                'from_address': 'recon-system@firm.com',
                'username': 'recon-bot',
                'password': 'secret'
            },
            'ml_model_path': 'models/matcher.pkl'
        }
        
        # Run reconciliation in background
        orchestrator = ReconciliationOrchestrator(db, config)
        
        # For demo purposes, running synchronously
        # In production, use background_tasks.add_task()
        result = orchestrator.run_daily_reconciliation(
            datetime.combine(request.trade_date, datetime.min.time())
        )
        
        if result['status'] == 'FAILED':
            raise HTTPException(status_code=500, detail=result['error'])
        
        # Get the run record
        run = repo.get_by_id(result['run_id'])
        return run
        
    except Exception as e:
        logger.error(f"Error running reconciliation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reconciliation/runs", response_model=List[ReconciliationRunResponse])
async def get_reconciliation_runs(
    skip: int = 0,
    limit: int = 20,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get list of reconciliation runs
    """
    repo = ReconciliationRepository(db)
    runs = repo.get_all(skip=skip, limit=limit, status=status)
    return runs

@app.get("/api/reconciliation/runs/{run_id}", response_model=ReconciliationRunResponse)
async def get_reconciliation_run(run_id: int, db: Session = Depends(get_db)):
    """
    Get specific reconciliation run details
    """
    repo = ReconciliationRepository(db)
    run = repo.get_by_id(run_id)
    
    if not run:
        raise HTTPException(status_code=404, detail="Reconciliation run not found")
    
    return run

@app.get("/api/reconciliation/statistics")
async def get_reconciliation_statistics(
    date_from: date,
    date_to: date,
    db: Session = Depends(get_db)
):
    """
    Get aggregated reconciliation statistics
    """
    repo = ReconciliationRepository(db)
    stats = repo.get_statistics(date_from, date_to)
    
    return {
        'period': {
            'from': date_from,
            'to': date_to
        },
        'total_runs': stats['total_runs'],
        'total_trades_processed': stats['total_trades'],
        'total_matches': stats['total_matches'],
        'total_breaks': stats['total_breaks'],
        'auto_resolution_rate': stats['auto_resolution_rate'],
        'avg_processing_time_seconds': stats['avg_processing_time'],
        'success_rate': stats['success_rate']
    }

# ==================== TRADE ENDPOINTS ====================

@app.get("/api/trades", response_model=List[TradeResponse])
async def search_trades(
    trade_date_from: Optional[date] = None,
    trade_date_to: Optional[date] = None,
    instrument_id: Optional[str] = None,
    counterparty: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Search trades with filters
    """
    repo = TradeRepository(db)
    trades = repo.search(
        trade_date_from=trade_date_from,
        trade_date_to=trade_date_to,
        instrument_id=instrument_id,
        counterparty=counterparty,
        status=status,
        source=source,
        skip=skip,
        limit=limit
    )
    return trades

@app.get("/api/trades/{trade_id}", response_model=TradeResponse)
async def get_trade(trade_id: int, db: Session = Depends(get_db)):
    """
    Get specific trade details
    """
    repo = TradeRepository(db)
    trade = repo.get_by_id(trade_id)
    
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    
    return trade

@app.get("/api/trades/{trade_id}/matched-trade", response_model=Optional[TradeResponse])
async def get_matched_trade(trade_id: int, db: Session = Depends(get_db)):
    """
    Get the matched trade for a given trade
    """
    repo = TradeRepository(db)
    trade = repo.get_by_id(trade_id)
    
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    
    if not trade.matched_trade_id:
        return None
    
    matched_trade = repo.get_by_id(trade.matched_trade_id)
    return matched_trade

# ==================== BREAK ENDPOINTS ====================

@app.get("/api/breaks", response_model=List[BreakResponse])
async def get_breaks(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    break_type: Optional[str] = None,
    assigned_to: Optional[str] = None,
    created_from: Optional[date] = None,
    created_to: Optional[date] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get breaks with filters
    """
    repo = BreakRepository(db)
    breaks = repo.search(
        status=status,
        severity=severity,
        break_type=break_type,
        assigned_to=assigned_to,
        created_from=created_from,
        created_to=created_to,
        skip=skip,
        limit=limit
    )
    return breaks

@app.get("/api/breaks/{break_id}", response_model=BreakResponse)
async def get_break(break_id: int, db: Session = Depends(get_db)):
    """
    Get specific break details
    """
    repo = BreakRepository(db)
    break_record = repo.get_by_id(break_id)
    
    if not break_record:
        raise HTTPException(status_code=404, detail="Break not found")
    
    return break_record

@app.post("/api/breaks/{break_id}/resolve")
async def resolve_break(
    break_id: int,
    request: BreakResolutionRequest,
    db: Session = Depends(get_db)
):
    """
    Resolve a break
    """
    repo = BreakRepository(db)
    break_record = repo.get_by_id(break_id)
    
    if not break_record:
        raise HTTPException(status_code=404, detail="Break not found")
    
    if break_record.status == 'RESOLVED':
        raise HTTPException(status_code=400, detail="Break already resolved")
    
    # Update break status
    break_record.status = 'RESOLVED'
    break_record.resolved_at = datetime.utcnow()
    break_record.resolution_notes = f"{request.resolution_type}: {request.notes}"
    
    db.commit()
    
    # Create workflow resolution
    config = {'notifications': {}}  # Load from config
    workflow_manager = WorkflowManager(db, config)
    
    resolution = {
        'type': request.resolution_type,
        'notes': request.notes
    }
    
    workflow_manager.resolve_case(f"CASE-{break_id}", resolution, request.user)
    
    return {"status": "success", "message": "Break resolved successfully"}

@app.post("/api/breaks/{break_id}/assign")
async def assign_break(
    break_id: int,
    request: BreakAssignmentRequest,
    db: Session = Depends(get_db)
):
    """
    Assign break to user
    """
    repo = BreakR
