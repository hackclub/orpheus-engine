"""
PostgreSQL connection handling with read-only enforcement and SQL validation.
"""

import os
import re
import unicodedata
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor


# SQL statements that are never allowed, even with read-only connection
DANGEROUS_PATTERNS = [
    r'\bINSERT\b',
    r'\bUPDATE\b',
    r'\bDELETE\b',
    r'\bDROP\b',
    r'\bTRUNCATE\b',
    r'\bALTER\b',
    r'\bCREATE\b',
    r'\bGRANT\b',
    r'\bREVOKE\b',
    r'\bVACUUM\b',
    r'\bREINDEX\b',
    r'\bCLUSTER\b',
    r'\bCOMMENT\b',
    r'\bLOCK\b',
    r'\bUNLOCK\b',
    r'\bSET\s+SESSION\b',
    r'\bSET\s+LOCAL\b',
    r'\bRESET\b',
    r'\bDISCARD\b',
    r'\bLISTEN\b',
    r'\bNOTIFY\b',
    r'\bLOAD\b',
    r'\bCOPY\b',
    r'\bDO\b\s*\$',  # PL/pgSQL blocks
    # Additional dangerous patterns
    r'\bEXECUTE\b',
    r'\bPREPARE\b',
    r'\bCALL\b',
    r'\bIMPORT\b',
    r'\bMERGE\b',
    r'\bREFRESH\b',
    r'\bSECURITY\b',
    r'\bOWNER\b',
    # SELECT INTO creates tables
    r'\bSELECT\b[^;]*\bINTO\b\s+(?!STRICT\b|TEMP\b|TEMPORARY\b)',
    r'\bINTO\s+(?:TEMP|TEMPORARY\s+)?TABLE\b',
    # RAISE for DoS attacks
    r'\bRAISE\b',
    # Additional safeguards
    r'\bPG_SLEEP\b',  # DoS via sleep
    r'\bLO_IMPORT\b',  # Large object operations
    r'\bLO_EXPORT\b',
    r'\bPG_READ_FILE\b',  # File system access
    r'\bPG_WRITE_FILE\b',
    r'\bPG_READ_BINARY_FILE\b',
    # Dangerous administrative functions
    r'\bPG_TERMINATE_BACKEND\b',  # Kill other connections
    r'\bPG_CANCEL_BACKEND\b',  # Cancel running queries
    r'\bSET_CONFIG\b',  # Modify session settings
    r'\bPG_ADVISORY_LOCK\b',  # Can cause deadlocks/DoS
    r'\bPG_ADVISORY_XACT_LOCK\b',
    r'\bPG_TRY_ADVISORY_LOCK\b',
    r'\bPG_TRY_ADVISORY_XACT_LOCK\b',
    # More dangerous functions
    r'\bPG_RELOAD_CONF\b',  # Reload server config
    r'\bPG_ROTATE_LOGFILE\b',  # Log rotation
    r'\bDBLINK\b',  # External database connections
    r'\bDBLINK_EXEC\b',
]


class SQLValidationError(Exception):
    """Raised when SQL contains forbidden statements."""
    pass


def _normalize_unicode(text: str) -> str:
    """
    Normalize Unicode text to ASCII to prevent homoglyph attacks.
    
    Converts full-width characters, look-alike Unicode chars, etc. to their
    ASCII equivalents.
    """
    # NFKC normalization converts full-width chars to ASCII equivalents
    # e.g., ＤＲＯＰ -> DROP
    normalized = unicodedata.normalize('NFKC', text)
    
    # Comprehensive homoglyph mappings for attack prevention
    # Covers Cyrillic, Greek, mathematical symbols, and other lookalikes
    homoglyphs = {
        # Cyrillic uppercase
        'А': 'A', 'В': 'B', 'С': 'C', 'Е': 'E', 'Н': 'H', 'І': 'I',
        'Ј': 'J', 'К': 'K', 'М': 'M', 'О': 'O', 'Р': 'P', 'Ѕ': 'S',
        'Т': 'T', 'Х': 'X', 'У': 'Y', 'Ғ': 'F',
        # Cyrillic lowercase
        'а': 'a', 'с': 'c', 'е': 'e', 'һ': 'h', 'і': 'i', 'ј': 'j',
        'о': 'o', 'р': 'p', 'ѕ': 's', 'х': 'x', 'у': 'y',
        # Greek uppercase
        'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'H', 'Ι': 'I',
        'Κ': 'K', 'Μ': 'M', 'Ν': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T',
        'Υ': 'Y', 'Χ': 'X',
        # Greek lowercase
        'α': 'a', 'β': 'B', 'ε': 'e', 'ι': 'i', 'κ': 'k', 'ν': 'v',
        'ο': 'o', 'ρ': 'p', 'τ': 't', 'υ': 'u', 'χ': 'x',
        # Various i/l/1 lookalikes
        'ı': 'i', 'ɩ': 'i', 'ǀ': 'l', 'ⅰ': 'i', 'ℓ': 'l', 'ⅼ': 'l',
        'Ɩ': 'I', 'Ⅰ': 'I', 'Ι': 'I', '١': '1', '۱': '1', 'ⅠⅠ': 'II',
        # Various o/0 lookalikes
        'ο': 'o', 'о': 'o', ' օ': 'o', '٥': '0', '۰': '0',
        # Mathematical bold/italic
        '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F',
        '𝐆': 'G', '𝐇': 'H', '𝐈': 'I', '𝐉': 'J', '𝐊': 'K', '𝐋': 'L',
        '𝐌': 'M', '𝐍': 'N', '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R',
        '𝐒': 'S', '𝐓': 'T', '𝐔': 'U', '𝐕': 'V', '𝐖': 'W', '𝐗': 'X',
        '𝐘': 'Y', '𝐙': 'Z',
        # Subscript/superscript
        'ᵃ': 'a', 'ᵇ': 'b', 'ᶜ': 'c', 'ᵈ': 'd', 'ᵉ': 'e', 'ᶠ': 'f',
        'ᵍ': 'g', 'ʰ': 'h', 'ⁱ': 'i', 'ʲ': 'j', 'ᵏ': 'k', 'ˡ': 'l',
        'ᵐ': 'm', 'ⁿ': 'n', 'ᵒ': 'o', 'ᵖ': 'p', 'ʳ': 'r', 'ˢ': 's',
        'ᵗ': 't', 'ᵘ': 'u', 'ᵛ': 'v', 'ʷ': 'w', 'ˣ': 'x', 'ʸ': 'y',
        'ᶻ': 'z',
        # Other common lookalikes
        'ƒ': 'f', 'ɡ': 'g', 'ɦ': 'h', 'ɱ': 'm', 'ɳ': 'n', 'ɾ': 'r',
        'ʋ': 'v', 'ʏ': 'Y', 'ʐ': 'z', 'ꜱ': 's', 'ꜰ': 'F',
        # Armenian
        'Տ': 'S', 'Ո': 'U', 'Ρ': 'P',
    }
    
    for homoglyph, ascii_char in homoglyphs.items():
        normalized = normalized.replace(homoglyph, ascii_char)
    
    # As a final safeguard, strip any remaining non-ASCII characters
    # that could be homoglyphs we missed, keeping only safe chars
    # But we keep common punctuation and operators needed for SQL
    safe_result = []
    for char in normalized:
        if ord(char) < 128 or char in '()[]{}.,;:!?@#$%^&*+-=<>/\\|`~"\'':
            safe_result.append(char)
        else:
            # Replace unknown non-ASCII with space to break up potential attacks
            safe_result.append(' ')
    
    return ''.join(safe_result)


def _remove_string_literals_and_identifiers(sql: str) -> str:
    """
    Remove string literals and quoted identifiers from SQL for safe pattern matching.
    
    Handles:
    - Standard strings: 'hello'
    - Escaped quotes: 'it''s' or 'it\'s'
    - PostgreSQL escape strings: E'hello\n'
    - Dollar-quoted strings: $$hello$$ or $tag$hello$tag$
    - Double-quoted identifiers: "column_name" or "DELETE" (valid column names)
    """
    result = []
    i = 0
    n = len(sql)
    
    while i < n:
        # Check for dollar-quoted strings: $$...$$ or $tag$...$tag$
        if sql[i] == '$':
            # Find the tag (empty for $$)
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == '_'):
                j += 1
            if j < n and sql[j] == '$':
                tag = sql[i:j+1]  # e.g., "$$" or "$tag$"
                end_pos = sql.find(tag, j + 1)
                if end_pos != -1:
                    result.append("''")  # Replace with empty string literal
                    i = end_pos + len(tag)
                    continue
        
        # Check for E'...' escape strings
        if sql[i] in ('E', 'e') and i + 1 < n and sql[i + 1] == "'":
            i += 1  # Skip the E, process the quote below
        
        # Check for standard string literals (single quotes)
        if sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    # Check for escaped quote ''
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2  # Skip both quotes
                        continue
                    break
                elif sql[j] == '\\' and j + 1 < n:
                    j += 2  # Skip escaped character
                    continue
                j += 1
            result.append("''")  # Replace entire string with empty
            i = j + 1
            continue
        
        # Check for double-quoted identifiers (PostgreSQL identifier quoting)
        # "DELETE" as a column name is valid and should not trigger validation
        if sql[i] == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    # Check for escaped quote ""
                    if j + 1 < n and sql[j + 1] == '"':
                        j += 2  # Skip both quotes
                        continue
                    break
                j += 1
            result.append('_ident_')  # Replace with safe placeholder
            i = j + 1
            continue
        
        result.append(sql[i])
        i += 1
    
    return ''.join(result)


def validate_sql(sql: str) -> None:
    """
    Validate that SQL doesn't contain dangerous statements.
    Raises SQLValidationError if forbidden patterns are found.
    """
    # Normalize Unicode to prevent homoglyph attacks (e.g., ＤＲＯＰ -> DROP)
    cleaned = _normalize_unicode(sql)
    
    # Remove comments
    cleaned = re.sub(r'--.*$', '', cleaned, flags=re.MULTILINE)  # Line comments
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)  # Block comments
    
    # Remove string literals and quoted identifiers
    # This prevents false positives like SELECT "DELETE" FROM table (valid column name)
    cleaned = _remove_string_literals_and_identifiers(cleaned)
    
    # Convert to uppercase for case-insensitive matching
    cleaned = cleaned.upper()
    
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            raise SQLValidationError(
                f"SQL contains forbidden statement pattern: {pattern.replace(chr(92), '')}"
            )


def get_connection_url() -> str:
    """Get the warehouse connection URL from environment."""
    url = os.environ.get('WAREHOUSE_COOLIFY_URL')
    if not url:
        raise RuntimeError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return url


def make_readonly_url(url: str) -> str:
    """Add read-only transaction option to PostgreSQL URL."""
    parsed = urlparse(url)
    
    # Check if read-only option already present
    if 'default_transaction_read_only' in url:
        return url
    
    # Add options parameter with read-only setting
    # Use %20 for space and %3D for = to avoid URL encoding issues
    readonly_option = "options=-c%20default_transaction_read_only%3Don"
    
    if parsed.query:
        new_query = f"{parsed.query}&{readonly_option}"
    else:
        new_query = readonly_option
    
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


class Database:
    """PostgreSQL database connection with read-only enforcement."""
    
    def __init__(self):
        self._conn: Optional[psycopg2.extensions.connection] = None
    
    def connect(self) -> None:
        """Establish connection to the database."""
        if self._conn is not None and not self._conn.closed:
            return
        
        url = get_connection_url()
        readonly_url = make_readonly_url(url)
        self._conn = psycopg2.connect(readonly_url)
        self._conn.set_session(readonly=True, autocommit=True)
    
    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
    
    def execute_query(self, sql: str, max_rows: int = 10000) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a read-only SQL query and return results.
        
        Args:
            sql: The SQL query to execute
            max_rows: Maximum number of rows to return (default 10000)
        
        Returns:
            Tuple of (rows as list of dicts, column names)
        
        Raises:
            SQLValidationError: If SQL contains forbidden statements
        """
        # Validate SQL before execution
        validate_sql(sql)
        
        self.connect()
        
        with self._conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql)
            
            # Fetch results
            if cursor.description is None:
                return [], []
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchmany(max_rows)
            
            # Convert to regular dicts
            rows = [dict(row) for row in rows]
            
            return rows, columns
    
    def execute_query_with_params(
        self, 
        sql: str, 
        params: tuple, 
        max_rows: int = 10000
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a parameterized read-only SQL query and return results.
        
        Args:
            sql: The SQL query with %s placeholders
            params: Tuple of parameters to substitute
            max_rows: Maximum number of rows to return (default 10000)
        
        Returns:
            Tuple of (rows as list of dicts, column names)
        """
        self.connect()
        
        with self._conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            
            # Fetch results
            if cursor.description is None:
                return [], []
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchmany(max_rows)
            
            # Convert to regular dicts
            rows = [dict(row) for row in rows]
            
            return rows, columns
    
    def list_schemas(self) -> List[str]:
        """List all non-system schemas in the database."""
        sql = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name;
        """
        rows, _ = self.execute_query(sql)
        return [row['schema_name'] for row in rows]
    
    def describe_schema(self, schema_name: str, max_cell_length: int = 100) -> str:
        """
        Get schema description using util_schema_markdown function.
        
        Args:
            schema_name: Name of the schema to describe
            max_cell_length: Maximum length for sample data values (default 100)
            
        Returns:
            Markdown description of the schema with tables, columns, and sample data
        """
        # Validate schema name format
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema_name):
            raise ValueError(f"Invalid schema name: {schema_name}")
        
        # Use parameterized query to prevent SQL injection
        rows, _ = self.execute_query_with_params(
            "SELECT util_schema_markdown(%s);",
            (schema_name,)
        )
        
        if rows and rows[0]:
            # Get the first column value (function result)
            result = list(rows[0].values())[0]
            if result:
                # Truncate long values in sample data to reduce output size
                result = self._truncate_markdown_values(result, max_cell_length)
                # Limit number of columns shown for wide tables
                result = self._limit_markdown_columns(result, max_columns=1000)
                return result
            return f"No description available for schema '{schema_name}'"
        
        return f"Schema '{schema_name}' not found or util_schema_markdown function unavailable"
    
    def _truncate_markdown_values(self, markdown: str, max_length: int) -> str:
        """
        Truncate long values in markdown table cells.
        
        Looks for table cells (content between | characters) and truncates
        values longer than max_length, showing original length.
        """
        lines = markdown.split('\n')
        result_lines = []
        
        for line in lines:
            if '|' in line and not line.strip().startswith('|--'):
                # This looks like a table row
                parts = line.split('|')
                truncated_parts = []
                for part in parts:
                    stripped = part.strip()
                    if len(stripped) > max_length:
                        # Truncate and show how much was cut
                        omitted = len(stripped) - max_length + 15  # account for suffix
                        truncated = stripped[:max_length - 15] + f'… [+{omitted} chars]'
                        # Preserve original spacing
                        if part.startswith(' '):
                            truncated = ' ' + truncated
                        if part.endswith(' ') and len(part) > 1:
                            truncated = truncated + ' '
                        truncated_parts.append(truncated)
                    else:
                        truncated_parts.append(part)
                result_lines.append('|'.join(truncated_parts))
            else:
                result_lines.append(line)
        
        return '\n'.join(result_lines)
    
    def list_columns(
        self, 
        schema_name: str, 
        table_name: str,
        offset: int = 0,
        limit: int = 100
    ) -> tuple[list[dict], int]:
        """
        List all columns for a specific table with pagination.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            offset: Starting column index
            limit: Maximum columns to return
            
        Returns:
            Tuple of (columns list, total column count)
        """
        # Validate names to prevent SQL injection
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema_name):
            raise ValueError(f"Invalid schema name: {schema_name}")
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        
        # Get total count
        count_sql = """
            SELECT COUNT(*) as total
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        count_rows, _ = self.execute_query_with_params(count_sql, (schema_name, table_name))
        total = count_rows[0]['total'] if count_rows else 0
        
        # Get columns with pagination
        columns_sql = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            LIMIT %s OFFSET %s
        """
        columns, _ = self.execute_query_with_params(
            columns_sql, 
            (schema_name, table_name, limit, offset)
        )
        
        return columns, total

    def _limit_markdown_columns(self, markdown: str, max_columns: int = 1000) -> str:
        """
        Limit the number of columns shown in markdown tables.
        
        For tables with more than max_columns, truncates and adds a note
        about omitted columns.
        """
        lines = markdown.split('\n')
        result_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Detect start of a markdown table (header row with |)
            if '|' in line and i + 1 < len(lines) and '---' in lines[i + 1]:
                # This is a table header
                header_parts = [p.strip() for p in line.split('|')]
                # Filter out empty parts from leading/trailing |
                header_parts = [p for p in header_parts if p]
                num_cols = len(header_parts)
                
                if num_cols > max_columns:
                    # Truncate the table
                    omitted = num_cols - max_columns
                    
                    # Process header
                    truncated_header = '| ' + ' | '.join(header_parts[:max_columns]) + f' | ... ({omitted} more columns) |'
                    result_lines.append(truncated_header)
                    
                    # Process separator
                    i += 1
                    sep_parts = lines[i].split('|')
                    sep_parts = [p for p in sep_parts if p.strip()]
                    truncated_sep = '|' + '|'.join(sep_parts[:max_columns]) + '|---|'
                    result_lines.append(truncated_sep)
                    
                    # Process data rows
                    i += 1
                    while i < len(lines) and '|' in lines[i] and lines[i].strip():
                        row_parts = lines[i].split('|')
                        row_parts = [p for p in row_parts if p or row_parts.index(p) in [0, len(row_parts)-1]]
                        # Keep first max_columns data cells
                        data_parts = [p.strip() for p in lines[i].split('|')]
                        data_parts = [p for p in data_parts if p][:max_columns]
                        truncated_row = '| ' + ' | '.join(data_parts) + ' | ... |'
                        result_lines.append(truncated_row)
                        i += 1
                    continue
                else:
                    result_lines.append(line)
            else:
                result_lines.append(line)
            i += 1
        
        return '\n'.join(result_lines)


# Global database instance
_db: Optional[Database] = None


def get_database() -> Database:
    """Get or create the global database instance."""
    global _db
    if _db is None:
        _db = Database()
    return _db

