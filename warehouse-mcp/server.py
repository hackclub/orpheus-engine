"""
HTTP MCP server for read-only data warehouse queries.

Provides tools for:
- query: Execute SQL and cache results
- get_rows: Paginate cached results
- grep_rows: Search within cached results
- list_schemas: List database schemas
- describe_schema: Get schema documentation with sample data
"""

import base64
import contextvars
import hashlib
import json
import logging
import os
import secrets
import time
import urllib.parse
from typing import Any

from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent

from db import get_database, SQLValidationError
from cache import get_cache

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_auth_token() -> str:
    """Get the AUTH_TOKEN from environment."""
    token = os.environ.get("AUTH_TOKEN")
    
    if not token:
        # Generate a temporary token
        token = secrets.token_urlsafe(32)
        logger.warning("=" * 60)
        logger.warning("No AUTH_TOKEN environment variable set.")
        logger.warning("Generated temporary token (will change on restart):")
        logger.warning(f"  AUTH_TOKEN: {token}")
        logger.warning("")
        logger.warning("Set AUTH_TOKEN in your environment for persistent auth.")
        logger.warning("=" * 60)
    else:
        logger.info("Using AUTH_TOKEN from environment")
    
    return token


# Auth token for authentication
AUTH_TOKEN = get_auth_token()

# OAuth state storage (in-memory for simplicity)
# Maps authorization codes to their metadata
_oauth_codes: dict[str, dict] = {}
# Maps pending auth requests (before user enters token)
_pending_auth: dict[str, dict] = {}
# Current user context (firstname, lastname) for SQL query attribution
_current_user: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar('current_user', default=None)


def generate_pending_auth(client_id: str, redirect_uri: str, code_challenge: str, state: str) -> str:
    """Store a pending auth request and return a session ID."""
    session_id = secrets.token_urlsafe(16)
    _pending_auth[session_id] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "state": state,
        "created_at": time.time(),
    }
    return session_id


def complete_auth(session_id: str, provided_token: str, firstname: str, lastname: str) -> tuple[str, str, str] | None:
    """
    Complete auth after user provides token and name.
    Returns (code, redirect_uri, state) if valid, None otherwise.
    """
    pending = _pending_auth.pop(session_id, None)
    if not pending:
        return None

    # Check session hasn't expired (10 minute validity)
    if time.time() - pending["created_at"] > 600:
        return None

    # Validate provided token matches AUTH_TOKEN
    if not secrets.compare_digest(provided_token, AUTH_TOKEN):
        # Put back in pending so user can retry
        _pending_auth[session_id] = pending
        return None

    # Generate authorization code
    code = secrets.token_urlsafe(32)
    _oauth_codes[code] = {
        "client_id": pending["client_id"],
        "redirect_uri": pending["redirect_uri"],
        "code_challenge": pending["code_challenge"],
        "created_at": time.time(),
        "firstname": firstname,
        "lastname": lastname,
    }

    return code, pending["redirect_uri"], pending["state"]


def exchange_code_for_token(code: str, code_verifier: str, client_id: str) -> str | None:
    """Exchange an authorization code for an access token."""
    code_data = _oauth_codes.pop(code, None)
    if not code_data:
        return None
    
    # Verify code hasn't expired (5 minute validity)
    if time.time() - code_data["created_at"] > 300:
        return None
    
    # Verify client_id matches
    if code_data["client_id"] != client_id:
        return None
    
    # Verify PKCE code_challenge
    # code_challenge = base64url(sha256(code_verifier))
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).decode().rstrip("=")
    
    if challenge != code_data["code_challenge"]:
        logger.warning(f"PKCE verification failed: expected {code_data['code_challenge']}, got {challenge}")
        return None

    # Encode AUTH_TOKEN with user info: base64("{AUTH_TOKEN}:{firstname}:{lastname}")
    firstname = code_data.get("firstname", "")
    lastname = code_data.get("lastname", "")
    token_data = f"{AUTH_TOKEN}:{firstname}:{lastname}"
    encoded_token = base64.b64encode(token_data.encode('utf-8')).decode('utf-8')
    return encoded_token


def decode_access_token(token: str) -> tuple[bool, str | None, str | None]:
    """
    Decode and validate an access token.

    Token format: base64("{AUTH_TOKEN}:{firstname}:{lastname}")

    Returns:
        (is_valid, firstname, lastname) or (False, None, None) if invalid
    """
    try:
        decoded = base64.b64decode(token).decode('utf-8')
        parts = decoded.split(':', 2)
        if len(parts) != 3:
            return (False, None, None)

        auth_token, firstname, lastname = parts
        if secrets.compare_digest(auth_token, AUTH_TOKEN):
            return (True, firstname, lastname)
        return (False, None, None)
    except Exception:
        return (False, None, None)


def validate_access_token(token: str) -> bool:
    """Validate an access token."""
    is_valid, _, _ = decode_access_token(token)
    return is_valid

# Create MCP server
mcp = Server("warehouse-mcp")

# HTTP transport - messages path (root for Streamable HTTP compatibility)
sse_transport = SseServerTransport("/")


def format_rows_for_display(rows: list[dict[str, Any]], columns: list[str], truncate_values: bool = False) -> str:
    """Format rows as a readable table string.

    Args:
        rows: List of row dictionaries
        columns: Column names in display order
        truncate_values: If True, truncate cell values to 100 characters (for previews)
    """
    if not rows:
        return "(no rows)"

    max_value_len = 100 if truncate_values else None

    # Convert all values to strings, handling None and optional truncation
    str_rows = []
    for row in rows:
        str_row = {}
        for k, v in row.items():
            if v is None:
                str_row[k] = "NULL"
            else:
                s = str(v)
                if max_value_len and len(s) > max_value_len:
                    s = s[:max_value_len] + "..."
                str_row[k] = s
        str_rows.append(str_row)

    # Calculate column widths based on actual content
    widths = {col: len(col) for col in columns}
    for row in str_rows:
        for col in columns:
            widths[col] = max(widths[col], len(row.get(col, "")))

    # Build table
    lines = []

    # Header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("-" * len(header))

    # Rows
    for row in str_rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col])
            for col in columns
        )
        lines.append(line)

    return "\n".join(lines)


@mcp.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="query",
            description=(
                "Execute a read-only SQL query against the data warehouse. "
                "Returns a query_id for fetching more rows, plus a preview of results. "
                "Use LIMIT in your SQL for large tables. Max 10,000 rows cached per query."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute"
                    },
                    "preview_rows": {
                        "type": "integer",
                        "description": "Number of preview rows to return (default: 50)",
                        "default": 50
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_rows",
            description=(
                "Get rows from a cached query result using pagination. "
                "Use the query_id returned from the query tool."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query_id": {
                        "type": "string",
                        "description": "Query ID from a previous query"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Starting row index (default: 0)",
                        "default": 0
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return (default: 50)",
                        "default": 50
                    }
                },
                "required": ["query_id"]
            }
        ),
        Tool(
            name="grep_rows",
            description=(
                "Search for rows matching a regex pattern in a cached query result. "
                "Searches across all columns. Case-insensitive."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query_id": {
                        "type": "string",
                        "description": "Query ID from a previous query"
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum matching rows to return (default: 100)",
                        "default": 100
                    }
                },
                "required": ["query_id", "pattern"]
            }
        ),
        Tool(
            name="list_schemas",
            description="List all available schemas in the data warehouse.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="describe_schema",
            description=(
                "Get detailed documentation for a schema including tables, columns, "
                "and sample data. Limited to 1000 columns per table. "
                "Use list_columns for tables with more columns."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema to describe"
                    }
                },
                "required": ["schema_name"]
            }
        ),
        Tool(
            name="list_columns",
            description=(
                "List all columns for a specific table with pagination. "
                "Use this for wide tables with >100 columns that get truncated in describe_schema."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Starting column index (default: 0)",
                        "default": 0
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum columns to return (default: 100)",
                        "default": 100
                    }
                },
                "required": ["schema_name", "table_name"]
            }
        )
    ]


@mcp.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    db = get_database()
    cache = get_cache()
    user_info = _current_user.get()

    try:
        if name == "query":
            sql = arguments["sql"]
            preview_rows = arguments.get("preview_rows", 50)

            # Execute query with user attribution
            rows, columns = db.execute_query(sql, user_info=user_info)
            
            # Cache results
            entry = cache.store(sql, rows, columns)
            
            # Format response
            preview = rows[:preview_rows]
            table = format_rows_for_display(preview, columns, truncate_values=True)
            
            result = (
                f"Query ID: {entry.query_id}\n"
                f"Total rows: {entry.row_count}\n"
                f"Columns: {', '.join(columns)}\n\n"
                f"Preview ({len(preview)} of {entry.row_count} rows):\n{table}"
            )
            
            if entry.row_count > preview_rows:
                result += f"\n\nUse get_rows(query_id='{entry.query_id}', offset={preview_rows}) for more rows."
            
            return [TextContent(type="text", text=result)]
        
        elif name == "get_rows":
            query_id = arguments["query_id"]
            offset = arguments.get("offset", 0)
            limit = arguments.get("limit", 50)
            
            result = cache.get_rows(query_id, offset, limit)
            if result is None:
                return [TextContent(
                    type="text",
                    text=f"Query ID '{query_id}' not found or expired. Run a new query."
                )]
            
            rows, total = result
            entry = cache.get(query_id)
            columns = entry.columns if entry else []
            
            table = format_rows_for_display(rows, columns)
            
            response = (
                f"Rows {offset + 1}-{offset + len(rows)} of {total}:\n{table}"
            )
            
            if offset + len(rows) < total:
                response += f"\n\nMore rows available. Use offset={offset + limit} to continue."
            
            return [TextContent(type="text", text=response)]
        
        elif name == "grep_rows":
            query_id = arguments["query_id"]
            pattern = arguments["pattern"]
            limit = arguments.get("limit", 100)
            
            result = cache.grep_rows(query_id, pattern, limit)
            if result is None:
                return [TextContent(
                    type="text",
                    text=f"Query ID '{query_id}' not found or expired. Run a new query."
                )]
            
            rows, total_matches = result
            entry = cache.get(query_id)
            columns = entry.columns if entry else []
            
            table = format_rows_for_display(rows, columns)
            
            response = (
                f"Found {total_matches} rows matching '{pattern}'.\n"
                f"Showing {len(rows)} matches:\n{table}"
            )
            
            return [TextContent(type="text", text=response)]
        
        elif name == "list_schemas":
            schemas = db.list_schemas()
            
            response = "Available schemas:\n"
            for schema in schemas:
                response += f"  - {schema}\n"
            response += f"\nUse describe_schema(schema_name='...') to see tables and columns."
            
            return [TextContent(type="text", text=response)]
        
        elif name == "describe_schema":
            schema_name = arguments["schema_name"]
            description = db.describe_schema(schema_name)
            return [TextContent(type="text", text=description)]
        
        elif name == "list_columns":
            schema_name = arguments["schema_name"]
            table_name = arguments["table_name"]
            offset = arguments.get("offset", 0)
            limit = arguments.get("limit", 100)
            
            columns, total = db.list_columns(schema_name, table_name, offset, limit)
            
            if not columns:
                return [TextContent(
                    type="text",
                    text=f"No columns found for {schema_name}.{table_name}"
                )]
            
            # Format as a table
            lines = [
                f"Columns for {schema_name}.{table_name} ({offset + 1}-{offset + len(columns)} of {total}):",
                "",
                "| Column | Type | Nullable | Default |",
                "|--------|------|----------|---------|"
            ]
            
            for col in columns:
                name = col['column_name']
                dtype = col['data_type']
                if col.get('character_maximum_length'):
                    dtype += f"({col['character_maximum_length']})"
                nullable = "YES" if col['is_nullable'] == 'YES' else "NO"
                default = str(col['column_default'])[:30] if col['column_default'] else ""
                lines.append(f"| {name} | {dtype} | {nullable} | {default} |")
            
            if offset + len(columns) < total:
                lines.append("")
                lines.append(f"More columns available. Use offset={offset + limit} to continue.")
            
            return [TextContent(type="text", text="\n".join(lines))]
        
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except SQLValidationError as e:
        # Safe to expose - these are intentional validation messages
        return [TextContent(type="text", text=f"SQL validation error: {e}")]
    except ValueError as e:
        # Safe to expose - argument validation errors
        return [TextContent(type="text", text=f"Invalid argument: {e}")]
    except Exception as e:
        # Log full exception for debugging, but don't expose details to client
        logger.exception("Tool execution error")
        # Return generic message to avoid leaking internal information
        return [TextContent(type="text", text="An internal error occurred. Please try again or contact support if the issue persists.")]


def validate_auth(scope) -> bool:
    """
    Validate auth from Authorization header.
    
    Supports Bearer token: "Authorization: Bearer <AUTH_TOKEN>"
    """
    headers = dict(scope.get("headers", []))
    auth = headers.get(b"authorization", b"").decode()
    
    if auth.startswith("Bearer "):
        token = auth[7:]
        return validate_access_token(token)
    
    return False


async def send_error(send, status: int, message: str):
    """Send an error response."""
    body = json.dumps({"error": message}).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [[b"content-type", b"application/json"]],
    })
    await send({
        "type": "http.response.body",
        "body": body,
    })


async def read_body(receive) -> bytes:
    """Read the full request body."""
    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break
    return body


def get_server_url(scope) -> str:
    """Get the server URL from the request scope."""
    headers = dict(scope.get("headers", []))
    host = headers.get(b"host", b"localhost:8000").decode()
    # Check for forwarded proto (behind reverse proxy)
    proto = headers.get(b"x-forwarded-proto", b"http").decode()
    return f"{proto}://{host}"


async def app(scope, receive, send):
    """Pure ASGI application with OAuth 2.1 authentication."""
    if scope["type"] != "http":
        return
    
    path = scope["path"]
    method = scope["method"]
    query_string = scope.get("query_string", b"").decode()
    query_params = dict(urllib.parse.parse_qsl(query_string))
    server_url = get_server_url(scope)
    
    # Log incoming request details for debugging (without revealing tokens)
    headers = dict(scope.get("headers", []))
    auth_header = headers.get(b"authorization", b"").decode()
    # Only log auth presence, never the token value
    if auth_header.startswith("Bearer "):
        auth_display = "Bearer <redacted>"
    elif auth_header:
        auth_display = "<redacted>"
    else:
        auth_display = "(none)"
    logger.info(f"Request: {method} {path} | Auth: {auth_display}")
    
    # Health check - no auth required
    if path == "/health" and method == "GET":
        body = json.dumps({"status": "ok", "service": "warehouse-mcp"}).encode()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })
        return
    
    # OAuth 2.1 Discovery - no auth required
    if path == "/.well-known/oauth-authorization-server" and method == "GET":
        metadata = {
            "issuer": server_url,
            "authorization_endpoint": f"{server_url}/authorize",
            "token_endpoint": f"{server_url}/token",
            "registration_endpoint": f"{server_url}/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": [],
        }
        body = json.dumps(metadata).encode()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })
        return
    
    # Dynamic Client Registration endpoint (required by some OAuth clients)
    if path == "/register" and method == "POST":
        body = await read_body(receive)
        try:
            client_data = json.loads(body.decode()) if body else {}
        except json.JSONDecodeError:
            client_data = {}
        
        # Generate a client_id for this registration
        client_id = f"client-{secrets.token_hex(8)}"
        
        response = {
            "client_id": client_id,
            "client_secret": "",  # Public client, no secret needed
            "redirect_uris": client_data.get("redirect_uris", []),
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        }
        
        response_body = json.dumps(response).encode()
        await send({
            "type": "http.response.start",
            "status": 201,
            "headers": [[b"content-type", b"application/json"]],
        })
        await send({
            "type": "http.response.body",
            "body": response_body,
        })
        return
    
    # OAuth Protected Resource Metadata - no auth required
    if path == "/.well-known/oauth-protected-resource" and method == "GET":
        metadata = {
            "resource": server_url,
            "authorization_servers": [server_url],
        }
        body = json.dumps(metadata).encode()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })
        return
    
    # OAuth Authorization endpoint - show login form
    if path == "/authorize" and method == "GET":
        client_id = query_params.get("client_id", "")
        redirect_uri = query_params.get("redirect_uri", "")
        code_challenge = query_params.get("code_challenge", "")
        state = query_params.get("state", "")
        
        logger.info(f"OAuth authorize: client_id={client_id}, redirect_uri={redirect_uri}")
        
        # Create pending auth session
        session_id = generate_pending_auth(client_id, redirect_uri, code_challenge, state)
        
        # Show login form
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Warehouse MCP - Authentication</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0;
            padding: 20px;
        }}
        .container {{
            background: rgba(255, 255, 255, 0.95);
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.4);
            max-width: 400px;
            width: 100%;
        }}
        h1 {{
            margin: 0 0 8px 0;
            color: #1a1a2e;
            font-size: 24px;
            font-weight: 600;
        }}
        .subtitle {{
            color: #666;
            margin-bottom: 32px;
            font-size: 14px;
        }}
        label {{
            display: block;
            margin-bottom: 8px;
            font-weight: 500;
            color: #333;
        }}
        .form-group {{
            margin-bottom: 16px;
        }}
        .name-row {{
            display: flex;
            gap: 12px;
        }}
        .name-row .form-group {{
            flex: 1;
        }}
        input[type="text"],
        input[type="password"] {{
            width: 100%;
            padding: 12px 16px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 16px;
            transition: border-color 0.2s;
        }}
        input[type="text"]:focus,
        input[type="password"]:focus {{
            outline: none;
            border-color: #4f46e5;
        }}
        button {{
            width: 100%;
            padding: 14px;
            background: #4f46e5;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            margin-top: 16px;
            transition: background 0.2s;
        }}
        button:hover {{
            background: #4338ca;
        }}
        .error {{
            background: #fef2f2;
            color: #dc2626;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 16px;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🏭 Warehouse MCP</h1>
        <p class="subtitle">Enter your name and API key to connect</p>
        <form method="POST" action="/authorize">
            <input type="hidden" name="session_id" value="{session_id}">
            <div class="name-row">
                <div class="form-group">
                    <label for="firstname">First Name</label>
                    <input type="text" id="firstname" name="firstname" placeholder="First name" required autofocus>
                </div>
                <div class="form-group">
                    <label for="lastname">Last Name</label>
                    <input type="text" id="lastname" name="lastname" placeholder="Last name" required>
                </div>
            </div>
            <div class="form-group">
                <label for="token">API Key</label>
                <input type="password" id="token" name="token" placeholder="Enter API key" required>
            </div>
            <button type="submit">Authenticate</button>
        </form>
    </div>
</body>
</html>"""

        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"text/html"]],
        })
        await send({
            "type": "http.response.body",
            "body": html.encode(),
        })
        return
    
    # OAuth Authorization POST - handle token submission
    if path == "/authorize" and method == "POST":
        body = await read_body(receive)
        form_data = dict(urllib.parse.parse_qsl(body.decode()))

        session_id = form_data.get("session_id", "")
        provided_token = form_data.get("token", "")
        firstname = form_data.get("firstname", "")
        lastname = form_data.get("lastname", "")

        result = complete_auth(session_id, provided_token, firstname, lastname)
        
        if not result:
            # Show error and let user retry
            pending = _pending_auth.get(session_id)
            if not pending:
                await send_error(send, 400, "Session expired. Please try again.")
                return
            
            html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Warehouse MCP - Authentication</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0;
            padding: 20px;
        }}
        .container {{
            background: rgba(255, 255, 255, 0.95);
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.4);
            max-width: 400px;
            width: 100%;
        }}
        h1 {{
            margin: 0 0 8px 0;
            color: #1a1a2e;
            font-size: 24px;
            font-weight: 600;
        }}
        .subtitle {{
            color: #666;
            margin-bottom: 32px;
            font-size: 14px;
        }}
        label {{
            display: block;
            margin-bottom: 8px;
            font-weight: 500;
            color: #333;
        }}
        .form-group {{
            margin-bottom: 16px;
        }}
        .name-row {{
            display: flex;
            gap: 12px;
        }}
        .name-row .form-group {{
            flex: 1;
        }}
        input[type="text"],
        input[type="password"] {{
            width: 100%;
            padding: 12px 16px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 16px;
            transition: border-color 0.2s;
        }}
        input[type="text"]:focus,
        input[type="password"]:focus {{
            outline: none;
            border-color: #4f46e5;
        }}
        button {{
            width: 100%;
            padding: 14px;
            background: #4f46e5;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            margin-top: 16px;
            transition: background 0.2s;
        }}
        button:hover {{
            background: #4338ca;
        }}
        .error {{
            background: #fef2f2;
            color: #dc2626;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 16px;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🏭 Warehouse MCP</h1>
        <p class="subtitle">Enter your name and API key to connect</p>
        <div class="error">Invalid API key. Please try again.</div>
        <form method="POST" action="/authorize">
            <input type="hidden" name="session_id" value="{session_id}">
            <div class="name-row">
                <div class="form-group">
                    <label for="firstname">First Name</label>
                    <input type="text" id="firstname" name="firstname" placeholder="First name" value="{firstname}" required autofocus>
                </div>
                <div class="form-group">
                    <label for="lastname">Last Name</label>
                    <input type="text" id="lastname" name="lastname" placeholder="Last name" value="{lastname}" required>
                </div>
            </div>
            <div class="form-group">
                <label for="token">API Key</label>
                <input type="password" id="token" name="token" placeholder="Enter API key" required>
            </div>
            <button type="submit">Authenticate</button>
        </form>
    </div>
</body>
</html>"""

            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [[b"content-type", b"text/html"]],
            })
            await send({
                "type": "http.response.body",
                "body": html.encode(),
            })
            return
        
        code, redirect_uri, state = result
        redirect_url = f"{redirect_uri}?code={code}&state={state}"
        
        await send({
            "type": "http.response.start",
            "status": 302,
            "headers": [
                [b"location", redirect_url.encode()],
                [b"content-type", b"text/html"],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": b"Redirecting...",
        })
        return
    
    # OAuth Token endpoint - no auth required (uses PKCE)
    if path == "/token" and method == "POST":
        body = await read_body(receive)
        token_params = dict(urllib.parse.parse_qsl(body.decode()))
        
        grant_type = token_params.get("grant_type", "")
        code = token_params.get("code", "")
        code_verifier = token_params.get("code_verifier", "")
        client_id = token_params.get("client_id", "")
        
        logger.info(f"OAuth token exchange: grant_type={grant_type}, client_id={client_id}")
        
        if grant_type != "authorization_code":
            await send_error(send, 400, "unsupported_grant_type")
            return
        
        access_token = exchange_code_for_token(code, code_verifier, client_id)
        
        if not access_token:
            await send_error(send, 400, "invalid_grant")
            return
        
        token_response = {
            "access_token": access_token,
            "token_type": "Bearer",
        }
        
        response_body = json.dumps(token_response).encode()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        })
        await send({
            "type": "http.response.body",
            "body": response_body,
        })
        return
    
    # All other endpoints require authentication
    if not validate_auth(scope):
        # Return 401 with WWW-Authenticate header to trigger OAuth flow
        body = json.dumps({"error": "Unauthorized"}).encode()
        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [
                [b"content-type", b"application/json"],
                [b"www-authenticate", f'Bearer resource_metadata="{server_url}/.well-known/oauth-protected-resource"'.encode()],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })
        return

    # Extract user context from auth token for SQL attribution
    headers = dict(scope.get("headers", []))
    auth_header = headers.get(b"authorization", b"").decode()
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        is_valid, firstname, lastname = decode_access_token(token)
        if is_valid and firstname and lastname:
            _current_user.set((firstname, lastname))

    # MCP SSE endpoint at root path (/) for Streamable HTTP transport
    if path == "/" and method == "GET":
        async with sse_transport.connect_sse(scope, receive, send) as streams:
            await mcp.run(
                streams[0],
                streams[1],
                mcp.create_initialization_options()
            )
        return

    # MCP messages endpoint at root path
    if path == "/" and method == "POST":
        await sse_transport.handle_post_message(scope, receive, send)
        return

    # 404 for unknown routes
    await send_error(send, 404, "Not found")


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logger.info(f"Starting warehouse-mcp server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
