
import subprocess
import json
import logging
import io
from typing import List, Dict, Any, Optional
import pyarrow as pa
from waveql.adapters.base import BaseAdapter
from waveql.schema_cache import ColumnInfo

logger = logging.getLogger(__name__)

class SingerAdapter(BaseAdapter):
    """
    Adapter for Singer.io Taps.
    Wraps an executable tap (e.g., 'tap-github') and ingests its output.
    """
    
    adapter_name = "singer"
    
    def __init__(
        self, 
        tap_command: str, 
        config_path: str = None, 
        catalog_path: str = None,
        state_path: str = None,
        **kwargs
    ):
        """
        Initialize Singer Adapter.
        
        Args:
            tap_command: Command to run the tap (e.g. "tap-github", "python my_tap.py")
            config_path: Path to config.json file
            catalog_path: Path to catalog.json file (optional)
            state_path: Path to state.json file (optional, for incremental sync)
        """
        super().__init__(**kwargs)
        self.tap_command = tap_command
        self.config_path = config_path
        self.catalog_path = catalog_path
        self.state_path = state_path
        self._cached_catalog = None
        
    def _run_tap(self, args: List[str]) -> subprocess.Popen:
        """Run the tap command with arguments."""
        cmd = self.tap_command.split() + args
        if self.config_path:
            cmd.extend(["--config", self.config_path])
            
        logger.info(f"Running tap: {' '.join(cmd)}")
        
        env = self._config.get("env", None)
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, # Avoid deadlock by discarding stderr for now
            text=True,
            encoding='utf-8',
            env=env
        )
        return process

    def _discover_schema(self):
        """Run tap in discovery mode to get catalog."""
        if self._cached_catalog:
            return self._cached_catalog
            
        process = self._run_tap(["--discover"])
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"Tap discovery failed: {stderr}")
            raise RuntimeError(f"Tap discovery failed with code {process.returncode}: {stderr}")
            
        try:
            # Some taps might output other noise, strictly look for valid JSON at the end or proper structure
            # But standard taps output the JSON catalog as stdout
            self._cached_catalog = json.loads(stdout)
            return self._cached_catalog
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode catalog JSON: {e}")
            logger.debug(f"Output was: {stdout}")
            raise

    def list_tables(self) -> List[str]:
        """List available streams (tables)."""
        catalog = self._discover_schema()
        # Singer spec: catalog has 'streams' list
        streams = catalog.get("streams", [])
        return [s.get("stream", s.get("tap_stream_id")) for s in streams]

    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Convert Singer JSON Schema to ColumnInfo."""
        catalog = self._discover_schema()
        streams = catalog.get("streams", [])
        
        target_stream = next((s for s in streams if s.get("stream") == table), None)
        if not target_stream:
            # Fallback for taps using tap_stream_id
            target_stream = next((s for s in streams if s.get("tap_stream_id") == table), None)
            
        if not target_stream:
            raise ValueError(f"Stream '{table}' not found in tap catalog")
            
        schema = target_stream.get("schema", {})
        properties = schema.get("properties", {})
        
        columns = []
        for prop_name, prop_def in properties.items():
            # Primitive type mapping
            singer_type = prop_def.get("type", "string")
            if isinstance(singer_type, list):
                # Handle ["null", "string"]
                types = [t for t in singer_type if t != "null"]
                singer_type = types[0] if types else "string"
                
            columns.append(ColumnInfo(
                name=prop_name,
                data_type=str(singer_type),
                nullable=True # Assume everything is nullable for flexibility unless strictly defined
            ))
            
        return columns

    def fetch(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List = None,
        **kwargs
    ) -> pa.Table:
        """
        Run the tap and extract records for the given table.
        Note: This effectively streams the ENTIRE tap extraction for the specific table logic.
        """
        # If we have a catalog path, we might want to modify it to select only the requested stream?
        # For now, we will run the tap (potentially with a provided catalog) and filter in memory.
        # Ideally, we should generate a selection catalog.
        
        args = []
        if self.catalog_path:
            # TODO: We could generate a temp catalog selecting only 'table'
            args.extend(["--catalog", self.catalog_path])
        elif self.config_path:
            # Some taps imply things from config if no catalog, but usually --properties/--catalog is needed for sync
            # If no catalog passed to init, we might try to use the one from discovery?
            # But standard taps need a file.
            pass
            
        if self.state_path:
            args.extend(["--state", self.state_path])
            
        # If no specified execution args (sync mode usually just needs config + catalog), run defaults
        # But if we don't pass a catalog, many taps won't sync anything or will sync everything.
        # Let's assume the user configured the adapter with a Catalog that 'selects' what they want,
        # OR we just try simply running it.
        # Fallback: if no properties/catalog argument is standard, we assume the tap syncs default streams.
        
        process = self._run_tap(args)
        
        records = []
        
        # Stream stdout line by line
        for line in process.stdout:
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
                
            msg_type = msg.get("type")
            
            if msg_type == "RECORD":
                if msg.get("stream") == table:
                    rec = msg.get("record", {})
                    if columns:
                        # Simple column filtering
                        rec = {k: v for k, v in rec.items() if k in columns}
                    records.append(rec)
            # We ignore SCHEMA and STATE messages for the fetch result for now
            
        process.wait()
        
        if not records:
            # Return empty schema if possible
            schema = self.get_schema(table)
            schema_fields = [(c.name, pa.string()) for c in schema] # Simplifying types for empty table
            return pa.Table.from_pylist([], schema=pa.schema(schema_fields))

        # Infer schema and create table
        return pa.Table.from_pylist(records)

    def verify_connection(self) -> bool:
        """Verify we can execute the tap."""
        try:
            p = subprocess.Popen(
                self.tap_command.split() + ["--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            p.communicate()
            return p.returncode == 0
        except Exception:
            return False
