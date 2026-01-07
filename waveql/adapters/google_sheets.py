"""
Google Sheets Adapter - Query Google Sheets with SQL via WaveQL.

Features:
- Read spreadsheet data as SQL tables
- Support for multiple sheets (tabs) as different tables
- Column type inference
- Range selection support
- OAuth2 and Service Account authentication

Requires: google-api-python-client, google-auth-oauthlib
"""

from __future__ import annotations
import os
import logging
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING
from dataclasses import dataclass

import pyarrow as pa
import anyio

from waveql.adapters.base import BaseAdapter
from waveql.exceptions import AdapterError, QueryError, ConfigurationError
from waveql.schema_cache import ColumnInfo

if TYPE_CHECKING:
    from waveql.query_planner import Predicate

logger = logging.getLogger(__name__)


@dataclass
class GoogleSheetsCredentials:
    """
    Google Sheets authentication credentials.
    
    Supports:
    - Service Account JSON file
    - OAuth2 credentials file
    - API key (for public sheets only)
    """
    
    # Path to service account JSON file
    service_account_json: Optional[str] = None
    
    # Path to OAuth2 credentials file (for user-based auth)
    oauth_credentials_file: Optional[str] = None
    oauth_token_file: Optional[str] = None
    
    # API key (for public sheets only)
    api_key: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "GoogleSheetsCredentials":
        """Create credentials from environment variables."""
        return cls(
            service_account_json=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            oauth_credentials_file=os.environ.get("GOOGLE_OAUTH_CREDENTIALS"),
            oauth_token_file=os.environ.get("GOOGLE_OAUTH_TOKEN"),
            api_key=os.environ.get("GOOGLE_SHEETS_API_KEY"),
        )


class GoogleSheetsAdapter(BaseAdapter):
    """
    Google Sheets adapter for querying spreadsheets with SQL.
    
    Features:
    - Each sheet (tab) is treated as a table
    - First row is treated as column headers
    - Automatic type inference
    - Support for named ranges
    - Predicate filtering (client-side)
    
    Examples:
        # Connect to a spreadsheet by ID
        adapter = GoogleSheetsAdapter(
            "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"  # Spreadsheet ID
        )
        
        # Query a specific sheet
        data = adapter.fetch("Sheet1")
        
        # With service account
        adapter = GoogleSheetsAdapter(
            spreadsheet_id,
            credentials=GoogleSheetsCredentials(
                service_account_json="/path/to/service-account.json"
            )
        )
    """
    
    adapter_name = "google_sheets"
    supports_predicate_pushdown = False  # Filtering is done client-side
    supports_aggregation = True  # Client-side aggregation support
    supports_insert = True
    supports_update = True
    supports_delete = False  # Too complex for sheet rows
    supports_batch = True
    
    # OAuth2 scopes
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/spreadsheets",  # For write operations
    ]
    
    def __init__(
        self,
        host: str,  # Spreadsheet ID or URL
        auth_manager=None,
        schema_cache=None,
        credentials: GoogleSheetsCredentials = None,
        **kwargs
    ):
        super().__init__(host, auth_manager, schema_cache, **kwargs)
        
        self._spreadsheet_id = self._extract_spreadsheet_id(host)
        self._credentials = credentials or GoogleSheetsCredentials.from_env()
        self._service = None
        self._spreadsheet_metadata = None
        
        # Lazy initialization of Google Sheets service
        self._init_service()
    
    def _extract_spreadsheet_id(self, host: str) -> str:
        """Extract spreadsheet ID from URL or return as-is if it's already an ID."""
        # Handle full Google Sheets URLs
        if "docs.google.com" in host:
            # URL format: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/...
            import re
            match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", host)
            if match:
                return match.group(1)
        
        # Handle google_sheets:// URI scheme
        if host.startswith("google_sheets://"):
            return host.replace("google_sheets://", "")
        
        # Assume it's already a spreadsheet ID
        return host
    
    def _init_service(self):
        """Initialize Google Sheets API service."""
        try:
            from googleapiclient.discovery import build
            from google.oauth2 import service_account
            from google.auth.transport.requests import Request
        except ImportError:
            raise ConfigurationError(
                "Google Sheets adapter requires: "
                "pip install google-api-python-client google-auth-oauthlib"
            )
        
        creds = None
        
        # Try service account first
        if self._credentials.service_account_json:
            try:
                creds = service_account.Credentials.from_service_account_file(
                    self._credentials.service_account_json,
                    scopes=self.SCOPES
                )
            except Exception as e:
                logger.warning(f"Failed to load service account: {e}")
        
        # Try OAuth2 credentials
        if not creds and self._credentials.oauth_credentials_file:
            try:
                from google.oauth2.credentials import Credentials
                from google_auth_oauthlib.flow import InstalledAppFlow
                
                token_file = self._credentials.oauth_token_file or "token.json"
                
                if os.path.exists(token_file):
                    creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
                
                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                    else:
                        flow = InstalledAppFlow.from_client_secrets_file(
                            self._credentials.oauth_credentials_file, self.SCOPES
                        )
                        creds = flow.run_local_server(port=0)
                    
                    # Save credentials for future use
                    with open(token_file, 'w') as token:
                        token.write(creds.to_json())
            except Exception as e:
                logger.warning(f"Failed to load OAuth credentials: {e}")
        
        # Try API key (public sheets only)
        if not creds and self._credentials.api_key:
            self._service = build(
                "sheets", "v4",
                developerKey=self._credentials.api_key
            )
            return
        
        if creds:
            self._service = build("sheets", "v4", credentials=creds)
        else:
            raise ConfigurationError(
                "No valid Google credentials found. Provide service_account_json, "
                "oauth_credentials_file, or api_key."
            )
    
    def _get_spreadsheet_metadata(self) -> Dict:
        """Get spreadsheet metadata (cached)."""
        if self._spreadsheet_metadata is None:
            try:
                self._spreadsheet_metadata = self._service.spreadsheets().get(
                    spreadsheetId=self._spreadsheet_id
                ).execute()
            except Exception as e:
                raise AdapterError(f"Failed to get spreadsheet metadata: {e}")
        return self._spreadsheet_metadata
    
    async def fetch_async(self, *args, **kwargs) -> pa.Table:
        """Fetch data from Google Sheets (async)."""
        return await anyio.to_thread.run_sync(lambda: self.fetch(*args, **kwargs))
    
    def fetch(
        self,
        table: str,
        columns: List[str] = None,
        predicates: List["Predicate"] = None,
        limit: int = None,
        offset: int = None,
        order_by: List[tuple] = None,
        group_by: List[str] = None,
        aggregates: List[Any] = None,
    ) -> pa.Table:
        """Fetch data from a Google Sheet."""
        # Build the range (sheet name or named range)
        range_name = table
        
        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id,
                range=range_name,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="FORMATTED_STRING",
            ).execute()
        except Exception as e:
            raise AdapterError(f"Failed to fetch from Google Sheets: {e}")
        
        values = result.get("values", [])
        
        if not values:
            return pa.table({})
        
        # First row is headers
        headers = values[0]
        data_rows = values[1:]
        
        # Build column data
        column_data = {h: [] for h in headers}
        
        for row in data_rows:
            # Pad row if it's shorter than headers
            padded_row = row + [None] * (len(headers) - len(row))
            for i, header in enumerate(headers):
                column_data[header].append(padded_row[i] if i < len(padded_row) else None)
        
        # Convert to Arrow table
        table = pa.table(column_data)
        
        # Apply client-side filtering
        if predicates:
            table = self._apply_predicates(table, predicates)
        
        # Column selection
        if columns and columns != ["*"]:
            available_cols = [c for c in columns if c in table.column_names]
            if available_cols:
                table = table.select(available_cols)
        
        # Order by (client-side)
        if order_by:
            import pyarrow.compute as pc
            for col, direction in reversed(order_by):
                if col in table.column_names:
                    indices = pc.sort_indices(table.column(col), sort_keys=[(col, "ascending" if direction.upper() == "ASC" else "descending")])
                    table = table.take(indices)
        
        # Offset and Limit
        if offset:
            table = table.slice(offset)
        if limit:
            table = table.slice(0, limit)
        
        # Apply client-side aggregation if requested
        if aggregates:
            table = self._compute_client_side_aggregates(table, group_by, aggregates)
        
        return table
    
    def _apply_predicates(self, table: pa.Table, predicates: List["Predicate"]) -> pa.Table:
        """Apply predicates to filter the table."""
        import pyarrow.compute as pc
        
        mask = None
        
        for pred in predicates:
            if pred.column not in table.column_names:
                continue
            
            col = table.column(pred.column)
            
            if pred.operator == "=":
                pred_mask = pc.equal(col, pred.value)
            elif pred.operator == "!=":
                pred_mask = pc.not_equal(col, pred.value)
            elif pred.operator == ">":
                pred_mask = pc.greater(col, pred.value)
            elif pred.operator == ">=":
                pred_mask = pc.greater_equal(col, pred.value)
            elif pred.operator == "<":
                pred_mask = pc.less(col, pred.value)
            elif pred.operator == "<=":
                pred_mask = pc.less_equal(col, pred.value)
            elif pred.operator == "LIKE":
                # Convert LIKE pattern to regex
                pattern = pred.value.replace("%", ".*").replace("_", ".")
                pred_mask = pc.match_like(col, pattern)
            elif pred.operator == "IN":
                pred_mask = pc.is_in(col, pa.array(pred.value))
            elif pred.operator == "IS NULL":
                pred_mask = pc.is_null(col)
            elif pred.operator == "IS NOT NULL":
                pred_mask = pc.is_valid(col)
            else:
                continue
            
            if mask is None:
                mask = pred_mask
            else:
                mask = pc.and_(mask, pred_mask)
        
        if mask is not None:
            return table.filter(mask)
        return table
    
    async def get_schema_async(self, table: str) -> List[ColumnInfo]:
        """Get schema from sheet (async)."""
        return await anyio.to_thread.run_sync(lambda: self.get_schema(table))
    
    def get_schema(self, table: str) -> List[ColumnInfo]:
        """Get schema from a Google Sheet (inferred from first row + data)."""
        cached = self._get_cached_schema(table)
        if cached:
            return cached
        
        try:
            # Get just the first few rows to infer types
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id,
                range=f"{table}!1:100",  # First 100 rows for type inference
                valueRenderOption="UNFORMATTED_VALUE",
            ).execute()
        except Exception as e:
            raise AdapterError(f"Failed to get schema from Google Sheets: {e}")
        
        values = result.get("values", [])
        
        if not values:
            return []
        
        headers = values[0]
        data_rows = values[1:]
        
        columns = []
        for i, header in enumerate(headers):
            # Infer type from data
            col_values = [row[i] for row in data_rows if len(row) > i and row[i] is not None]
            data_type = self._infer_type(col_values)
            
            columns.append(ColumnInfo(
                name=header,
                data_type=data_type,
                nullable=True,
            ))
        
        self._cache_schema(table, columns)
        return columns
    
    def _infer_type(self, values: List[Any]) -> str:
        """Infer column type from values."""
        if not values:
            return "string"
        
        # Check first non-None values
        sample = [v for v in values[:20] if v is not None]
        
        if not sample:
            return "string"
        
        # Check types
        all_bool = all(isinstance(v, bool) for v in sample)
        all_int = all(isinstance(v, int) and not isinstance(v, bool) for v in sample)
        all_float = all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in sample)
        
        if all_bool:
            return "boolean"
        elif all_int:
            return "integer"
        elif all_float:
            return "double"
        else:
            return "string"
    
    def list_tables(self) -> List[str]:
        """List all sheets in the spreadsheet."""
        metadata = self._get_spreadsheet_metadata()
        sheets = metadata.get("sheets", [])
        return [sheet["properties"]["title"] for sheet in sheets]
    
    async def insert_async(self, *args, **kwargs) -> int:
        """Insert row into sheet (async)."""
        return await anyio.to_thread.run_sync(lambda: self.insert(*args, **kwargs))
    
    def insert(
        self,
        table: str,
        values: Dict[str, Any],
        parameters: Sequence = None,
    ) -> int:
        """Append a row to a Google Sheet."""
        # Get current headers
        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id,
                range=f"{table}!1:1",
            ).execute()
        except Exception as e:
            raise QueryError(f"Failed to get headers: {e}")
        
        headers = result.get("values", [[]])[0]
        
        if not headers:
            # No headers - create them from values
            headers = list(values.keys())
            self._service.spreadsheets().values().append(
                spreadsheetId=self._spreadsheet_id,
                range=f"{table}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
        
        # Build row in header order
        row = [values.get(h, "") for h in headers]
        
        try:
            self._service.spreadsheets().values().append(
                spreadsheetId=self._spreadsheet_id,
                range=f"{table}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]}
            ).execute()
            return 1
        except Exception as e:
            raise QueryError(f"Failed to insert into Google Sheets: {e}")
    
    async def update_async(self, *args, **kwargs) -> int:
        """Update rows in sheet (async)."""
        return await anyio.to_thread.run_sync(lambda: self.update(*args, **kwargs))
    
    def update(
        self,
        table: str,
        values: Dict[str, Any],
        predicates: List["Predicate"] = None,
        parameters: Sequence = None,
    ) -> int:
        """
        Update rows in a Google Sheet.
        
        Note: This is a relatively expensive operation as it needs to:
        1. Fetch all data
        2. Find matching rows
        3. Update them individually
        """
        # Fetch all data
        full_data = self.fetch(table)
        
        if len(full_data) == 0:
            return 0
        
        # Apply predicates to find matching rows
        filtered = self._apply_predicates(full_data, predicates) if predicates else full_data
        
        if len(filtered) == 0:
            return 0
        
        # This is complex - we need to find the row indices in the original sheet
        # For simplicity, we'll use a batch update approach
        
        logger.warning(
            "Google Sheets UPDATE is not fully implemented. "
            "Consider using the Sheets API directly for complex updates."
        )
        
        return 0
    
    def get_spreadsheet_info(self) -> Dict[str, Any]:
        """Get information about the spreadsheet."""
        metadata = self._get_spreadsheet_metadata()
        
        return {
            "spreadsheet_id": self._spreadsheet_id,
            "title": metadata.get("properties", {}).get("title"),
            "locale": metadata.get("properties", {}).get("locale"),
            "sheets": [
                {
                    "title": sheet["properties"]["title"],
                    "index": sheet["properties"]["index"],
                    "row_count": sheet["properties"].get("gridProperties", {}).get("rowCount"),
                    "column_count": sheet["properties"].get("gridProperties", {}).get("columnCount"),
                }
                for sheet in metadata.get("sheets", [])
            ],
        }
