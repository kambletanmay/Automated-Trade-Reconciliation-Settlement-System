from typing import List, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

class WorkflowStatus(Enum):
    OPEN = "OPEN"
    ASSIGNED = "ASSIGNED"
    IN_PROGRESS = "IN_PROGRESS"
    PENDING_RESPONSE = "PENDING_RESPONSE"
    RESOLVED = "RESOLVED"
    ESCALATED = "ESCALATED"
    CLOSED = "CLOSED"

class WorkflowManager:
    """Manage break investigation workflow"""
    
    def __init__(self, db_session, notification_config: Dict):
        self.db = db_session
        self.notification_config = notification_config
        self.assignment_rules = self._load_assignment_rules()
        
    def _load_assignment_rules(self) -> List[Dict]:
        """Load rules for automatic assignment"""
        return [
            {
                'condition': lambda b: b['severity'] == 'CRITICAL',
                'assign_to': 'senior-ops-team@firm.com',
                'priority': 1
            },
            {
                'condition': lambda b: 'BROKER_FEED' in b.get('root_cause_category', ''),
                'assign_to': 'broker-ops-team@firm.com',
                'priority': 2
            },
            {
                'condition': lambda b: b.get('trade', {}).get('counterparty') == 'JPMORGAN',
                'assign_to': 'jpm-specialist@firm.com',
                'priority': 3
            },
            {
                'condition': lambda b: True,  # Default
                'assign_to': 'general-ops-team@firm.com',
                'priority': 99
            }
        ]
    
    def create_break_case(self, break_data: Dict) -> Dict:
        """Create a new break investigation case"""
        # Determine assignment
        assigned_to = self._auto_assign(break_data)
        
        # Calculate SLA deadline
        sla_hours = break_data.get('sla_hours', 24)
        sla_deadline = datetime.utcnow() + timedelta(hours=sla_hours)
        
        case = {
            'break_id': break_data['id'],
            'case_id': f"CASE-{datetime.utcnow().strftime('%Y%m%d')}-{break_data['id']}",
            'status': WorkflowStatus.ASSIGNED.value,
            'assigned_to': assigned_to,
            'created_at': datetime.utcnow(),
            'sla_deadline': sla_deadline,
            'priority_score': break_data.get('priority_score', 100),
            'investigation_notes': [],
            'action_items': []
        }
        
        # Send notification
        self._send_assignment_notification(case, break_data)
        
        return case
    
    def _auto_assign(self, break_data: Dict) -> str:
        """Auto-assign break based on rules"""
        # Sort rules by priority
        sorted_rules = sorted(self.assignment_rules, key=lambda x: x['priority'])
        
        for rule in sorted_rules:
            if rule['condition'](break_data):
                return rule['assign_to']
        
        return 'general-ops-team@firm.com'
    
    def add_investigation_note(self, case_id: str, note: str, user: str):
        """Add investigation note to case"""
        note_entry = {
            'timestamp': datetime.utcnow(),
            'user': user,
            'note': note
        }
        
        # In production, update database
        # For now, return the note entry
        return note_entry
    
    def escalate_case(self, case_id: str, reason: str, escalate_to: str):
        """Escalate case to higher authority"""
        escalation = {
            'case_id': case_id,
            'escalated_at': datetime.utcnow(),
            'reason': reason,
            'escalated_to': escalate_to,
            'previous_assignee': None  # Would fetch from DB
        }
        
        # Send escalation notification
        self._send_escalation_notification(escalation)
        
        return escalation
    
    def resolve_case(self, case_id: str, resolution: Dict, user: str):
        """Resolve a break case"""
        resolution_record = {
            'case_id': case_id,
            'resolved_at': datetime.utcnow(),
            'resolved_by': user,
            'resolution_type': resolution['type'],  # ACCEPT_EXTERNAL, ACCEPT_INTERNAL, AMEND
            'resolution_notes': resolution['notes'],
            'final_status': WorkflowStatus.RESOLVED.value
        }
        
        # Send resolution notification
        self._send_resolution_notification(resolution_record)
        
        return resolution_record
    
    def check_sla_breaches(self) -> List[Dict]:
        """Check for SLA breaches and send alerts"""
        # In production, query database for cases past SLA
        # For now, return mock data
        breached_cases = []
        
        for case in breached_cases:
            self._send_sla_breach_alert(case)
        
        return breached_cases
    
    def _send_assignment_notification(self, case: Dict, break_data: Dict):
        """Send email notification for new assignment"""
        subject = f"New Break Case Assigned: {case['case_id']} - {break_data['severity']}"
        
        body = f"""
        A new break case has been assigned to you:
        
        Case ID: {case['case_id']}
        Severity: {break_data['severity']}
        Break Type: {break_data['break_type']}
        SLA Deadline: {case['sla_deadline']}
        
        Trade Details:
        - Instrument: {break_data.get('trade', {}).get('instrument_id')}
        - Quantity: {break_data.get('trade', {}).get('quantity')}
        - Price: {break_data.get('trade', {}).get('price')}
        - Counterparty: {break_data.get('trade', {}).get('counterparty')}
        
        Break Details:
        - Expected: {break_data.get('expected_value')}
        - Actual: {break_data.get('actual_value')}
        - Difference: {break_data.get('difference')}
        
        Please investigate and resolve before the SLA deadline.
        
        Access case: https://recon-system.firm.com/cases/{case['case_id']}
        """
        
        self._send_email(case['assigned_to'], subject, body)
    
    def _send_escalation_notification(self, escalation: Dict):
        """Send escalation notification"""
        subject = f"Break Case Escalated: {escalation['case_id']}"
        body = f"""
        Case {escalation['case_id']} has been escalated to you.
        
        Reason: {escalation['reason']}
        Escalated At: {escalation['escalated_at']}
        
        Please review immediately.
        """
        
        self._send_email(escalation['escalated_to'], subject, body)
    
    def _send_resolution_notification(self, resolution: Dict):
        """Send resolution notification"""
        subject = f"Break Case Resolved: {resolution['case_id']}"
        body = f"""
        Case {resolution['case_id']} has been resolved.
        
        Resolved By: {resolution['resolved_by']}
        Resolution Type: {resolution['resolution_type']}
        Notes: {resolution['resolution_notes']}
        """
        
        # Send to relevant stakeholders
        self._send_email('ops-manager@firm.com', subject, body)
    
    def _send_sla_breach_alert(self, case: Dict):
        """Send SLA breach alert"""
        subject = f"⚠️ SLA BREACH: {case['case_id']}"
        body = f"""
        ALERT: SLA has been breached for case {case['case_id']}
        
        SLA Deadline: {case['sla_deadline']}
        Current Time: {datetime.utcnow()}
        Assigned To: {case['assigned_to']}
        
        Immediate action required.
        """
        
        self._send_email('ops-manager@firm.com', subject, body, priority='high')
        self._send_email(case['assigned_to'], subject, body, priority='high')
    
    def _send_email(self, to_address: str, subject: str, body: str, priority: str = 'normal'):
        """Send email notification"""
        config = self.notification_config
        
        msg = MIMEMultipart()
        msg['From'] = config['from_address']
        msg['To'] = to_address
        msg['Subject'] = subject
        
        if priority == 'high':
            msg['X-Priority'] = '1'
            msg['Importance'] = 'high'
        
        msg.attach(MIMEText(body, 'plain'))
        
        try:
            with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
                server.starttls()
                server.login(config['username'], config['password'])
                server.send_message(msg)
                print(f"Email sent to {to_address}")
        except Exception as e:
            print(f"Failed to send email: {e}")
    
    def generate_daily_summary(self, date: datetime) -> Dict:
        """Generate daily workflow summary"""
        # In production, query database for statistics
        summary = {
            'date': date.date(),
            'cases_created': 0,
            'cases_resolved': 0,
            'cases_open': 0,
            'sla_breaches': 0,
            'avg_resolution_time_hours': 0,
            'by_severity': {},
            'by_assignee': {},
            'top_aging_cases': []
        }
        
        return summary
