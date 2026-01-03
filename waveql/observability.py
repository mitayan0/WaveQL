"""
Observability and Metrics for WaveQL
"""

from __future__ import annotations
from dataclasses import dataclass, field
import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ExecutionStep:
    """A single step in the query execution plan."""
    name: str
    type: str  # "fetch", "join", "process", "duckdb"
    start_time: datetime.datetime = field(default_factory=datetime.datetime.now)
    end_time: Optional[datetime.datetime] = None
    duration_ms: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)
    
    def finish(self):
        self.end_time = datetime.datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000


@dataclass
class QueryPlan:
    """Complete execution plan for a query."""
    sql: str
    start_time: datetime.datetime = field(default_factory=datetime.datetime.now)
    end_time: Optional[datetime.datetime] = None
    total_duration_ms: float = 0.0
    steps: List[ExecutionStep] = field(default_factory=list)
    is_explain: bool = False
    
    def add_step(self, name: str, type: str, details: Dict[str, Any] = None) -> ExecutionStep:
        step = ExecutionStep(name=name, type=type, details=details or {})
        self.steps.append(step)
        return step
    
    def finish(self):
        self.end_time = datetime.datetime.now()
        self.total_duration_ms = (self.end_time - self.start_time).total_seconds() * 1000

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sql": self.sql,
            "total_duration_ms": self.total_duration_ms,
            "steps": [
                {
                    "name": s.name,
                    "type": s.type,
                    "duration_ms": s.duration_ms,
                    "details": s.details
                } for s in self.steps
            ]
        }
    
    def format_text(self) -> str:
        """Format the plan as a human-readable string."""
        lines = [f"WaveQL Execution Plan: {self.sql}"]
        lines.append(f"Total Duration: {self.total_duration_ms:.2f}ms")
        lines.append("-" * 50)
        
        for i, step in enumerate(self.steps):
            lines.append(f"{i+1}. [{step.type.upper()}] {step.name}")
            lines.append(f"   Duration: {step.duration_ms:.2f}ms")
            for k, v in step.details.items():
                lines.append(f"   {k}: {v}")
            lines.append("")
            
        return "\n".join(lines)
