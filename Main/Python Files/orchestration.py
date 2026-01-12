from typing import Dict, List
from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
from database.models import Trade, Break, ReconciliationRun, TradeSource, TradeStatus
from ingestion.parsers import CSVTradeParser, FIXMessageParser, DatabaseConnector
from matching.engine import TradeMatchingEngine, MatchConfig
from matching.ml_matcher import MLMatchingEnhancer
from breaks.analyzer import BreakAnalyzer, AutoResolver
from breaks.workflow import WorkflowManager
import concurrent.futures

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReconciliationOrchestrator:
    """Orchestrate end-to-end reconciliation process"""
    
    def __init__(self, db_session: Session, config: Dict):
        self.db = db_session
        self.config = config
        
        # Initialize components
        self.matcher = TradeMatchingEngine(MatchConfig(**config.get('matching', {})))
        self.ml_matcher = MLMatchingEnhancer()
        self.break_analyzer = BreakAnalyzer()
        self.auto_resolver = AutoResolver(db_session)
        self.workflow_manager = WorkflowManager(
            db_session, 
            config.get('notifications', {})
        )
        
        # Load ML model if available
        try:
            self.ml_matcher.load_model(config.get('ml_model_path', 'models/matcher.pkl'))
            logger.info("ML model loaded successfully")
        except FileNotFoundError:
            logger.warning("ML model not found, using rule-based matching only")
    
    def run_daily_reconciliation(self, trade_date: datetime) -> Dict:
        """Run complete daily reconciliation workflow"""
        logger.info(f"Starting reconciliation for trade date: {trade_date.date()}")
        
        # Create reconciliation run record
        recon_run = ReconciliationRun(
            trade_date=trade_date,
            status='RUNNING'
        )
        self.db.add(recon_run)
        self.db.commit()
        
        start_time = datetime.utcnow()
        
        try:
            # Step 1: Ingest trades
            logger.info("Step 1: Ingesting trades...")
            internal_trades, external_trades = self._ingest_trades(trade_date)
            
            recon_run.total_internal_trades = len(internal_trades)
            recon_run.total_external_trades = len(external_trades)
            self.db.commit()
            
            # Step 2: Match trades
            logger.info("Step 2: Matching trades...")
            matches, unmatched = self._match_trades(internal_trades, external_trades)
            
            recon_run.matched_trades = len(matches)
            self.db.commit()
            
            # Step 3: Identify breaks
            logger.info("Step 3: Identifying breaks...")
            breaks = self._identify_breaks(matches, unmatched)
            
            recon_run.new_breaks = len(breaks)
            self.db.commit()
            
            # Step 4: Analyze and categorize breaks
            logger.info("Step 4: Analyzing breaks...")
            categorized_breaks = self._analyze_breaks(breaks)
            
            # Step 5: Auto-resolve breaks
            logger.info("Step 5: Auto-resolving breaks...")
            auto_resolution_results = self._auto_resolve_breaks(categorized_breaks)
            
            recon_run.auto_resolved_breaks = auto_resolution_results['auto_resolved']
            self.db.commit()
            
            # Step 6: Create workflow cases for unresolved breaks
            logger.info("Step 6: Creating workflow cases...")
            cases = self._create_workflow_cases(categorized_breaks, auto_resolution_results)
            
            # Step 7: Generate reports
            logger.info("Step 7: Generating reports...")
            report = self._generate_report(
                trade_date, 
                recon_run, 
                categorized_breaks,
                auto_resolution_results
            )
            
            # Complete reconciliation run
            end_time = datetime.utcnow()
            recon_run.status = 'COMPLETED'
            recon_run.duration_seconds = (end_time - start_time).total_seconds()
            self.db.commit()
            
            logger.info(f"Reconciliation completed in {recon_run.duration_seconds:.2f} seconds")
            
            return {
                'status': 'SUCCESS',
                'run_id': recon_run.id,
                'statistics': {
                    'internal_trades': len(internal_trades),
                    'external_trades': len(external_trades),
                    'matched': len(matches),
                    'breaks': len(breaks),
                    'auto_resolved': auto_resolution_results['auto_resolved'],
                    'cases_created': len(cases)
                },
                'report': report
            }
            
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}", exc_info=True)
            recon_run.status = 'FAILED'
            recon_run.error_message = str(e)
            self.db.commit()
            
            return {
                'status': 'FAILED',
                'error': str(e)
            }
    
    def _ingest_trades(self, trade_date: datetime) -> tuple:
        """Ingest trades from all sources"""
        internal_trades = []
        external_trades = []
        
        # Ingest internal trades
        internal_connector = DatabaseConnector(
            self.config['internal_db_connection']
        )
        internal_raw = internal_connector.extract_trades(trade_date, 'INTERNAL')
        
        # Save to database
        for trade_data in internal_raw:
            trade = Trade(**trade_data)
            self.db.add(trade)
            internal_trades.append(trade_data)
        
        self.db.commit()
        
        # Ingest external trades in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            
            # Broker A - CSV
            if 'broker_a_csv_path' in self.config:
                parser_a = CSVTradeParser(self.config.get('broker_a_column_mapping'))
                future_a = executor.submit(
                    parser_a.parse,
                    self.config['broker_a_csv_path'],
                    'BROKER_A'
                )
                futures[future_a] = 'BROKER_A'
            
            # Broker B - FIX
            if 'broker_b_fix_path' in self.config:
                parser_b = FIXMessageParser()
                future_b = executor.submit(
                    parser_b.parse,
                    self.config['broker_b_fix_path'],
                    'BROKER_B'
                )
                futures[future_b] = 'BROKER_B'
            
            # Process results
            for future in concurrent.futures.as_completed(futures):
                source = futures[future]
                try:
                    trades_data = future.result()
                    for trade_data in trades_data:
                        trade = Trade(**trade_data)
                        self.db.add(trade)
                        external_trades.append(trade_data)
                    logger.info(f"Ingested {len(trades_data)} trades from {source}")
                except Exception as e:
                    logger.error(f"Error ingesting from {source}: {e}")
        
        self.db.commit()
        
        return internal_trades, external_trades
    
    def _match_trades(self, internal_trades: List[Dict], 
                     external_trades: List[Dict]) -> tuple:
        """Match internal and external trades"""
        matches, unmatched = self.matcher.match_trades(internal_trades, external_trades)
        
        # Update trade statuses in database
        for match in matches:
            internal_id = match['internal_trade']['id']
            external_id = match['external_trade']['id']
            
            # Update internal trade
            internal_trade = self.db.query(Trade).filter(Trade.id == internal_id).first()
            if internal_trade:
                internal_trade.status = TradeStatus.MATCHED
                internal_trade.matched_trade_id = external_id
            
            # Update external trade
            external_trade = self.db.query(Trade).filter(Trade.id == external_id).first()
            if external_trade:
                external_trade.status = TradeStatus.MATCHED
                external_trade.matched_trade_id = internal_id
        
        self.db.commit()
        
        return matches, unmatched
    
    def _identify_breaks(self, matches: List[Dict], unmatched: List[Dict]) -> List[Dict]:
        """Identify breaks in matched and unmatched trades"""
        all_breaks = []
        
        # Breaks from matched pairs with differences
        for match in matches:
            breaks = self.matcher.identify_breaks(
                match['internal_trade'],
                match['external_trade']
            )
            
            for break_data in breaks:
                break_record = Break(
                    trade_id=match['internal_trade']['id'],
                    matched_trade_id=match['external_trade']['id'],
                    break_type=break_data['break_type'],
                    severity=break_data['severity'],
                    expected_value=str(break_data['expected_value']),
                    actual_value=str(break_data['actual_value']),
                    difference=break_data.get('difference')
                )
                self.db.add(break_record)
                all_breaks.append(break_data)
        
        # Breaks from unmatched trades
        for unmatched_break in unmatched:
            break_record = Break(
                trade_id=unmatched_break['trade']['id'],
                break_type=unmatched_break['break_type'],
                severity=unmatched_break['severity']
            )
            self.db.add(break_record)
            all_breaks.append(unmatched_break)
        
        self.db.commit()
        return all_breaks
    
    def _analyze_breaks(self, breaks: List[Dict]) -> List[Dict]:
        """Analyze and categorize breaks"""
        categorized = []
        
        for break_data in breaks:
            categorized_break = self.break_analyzer.categorize_break(break_data)
            categorized.append(categorized_break)
        
        # Detect patterns
        patterns = self.break_analyzer.detect_patterns(categorized)
        logger.info(f"Detected {len(patterns)} break patterns")
        
        return categorized
    
    def _auto_resolve_breaks(self, breaks: List[Dict]) -> Dict:
        """Attempt auto-resolution of breaks"""
        return self.auto_resolver.batch_auto_resolve(breaks)
    
    def _create_workflow_cases(self, breaks: List[Dict], 
                              auto_resolution_results: Dict) -> List[Dict]:
        """Create workflow cases for unresolved breaks"""
        auto_resolved_ids = {
            r['break_id'] for r in auto_resolution_results.get('resolutions', [])
        }
        
        cases = []
        for break_data in breaks:
            if break_data['id'] not in auto_resolved_ids:
                case = self.workflow_manager.create_break_case(break_data)
                cases.append(case)
        
        return cases
    
    def _generate_report(self, trade_date: datetime, recon_run: ReconciliationRun,
                        breaks: List[Dict], auto_resolution: Dict) -> Dict:
        """Generate comprehensive reconciliation report"""
        return self.break_analyzer.generate_break_report(
            breaks,
            trade_date,
            trade_date + timedelta(days=1)
        )
