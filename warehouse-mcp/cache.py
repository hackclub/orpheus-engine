"""
In-memory cache for query results with TTL and size limits.
"""

import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional


@dataclass
class CachedQuery:
    """A cached query result."""
    query_id: str
    sql: str
    rows: List[Dict[str, Any]]
    columns: List[str]
    created_at: float
    row_count: int
    
    def is_expired(self, ttl_seconds: int) -> bool:
        """Check if this cache entry has expired."""
        return time.time() - self.created_at > ttl_seconds


@dataclass
class QueryCache:
    """In-memory cache for query results."""
    
    ttl_seconds: int = 600   # 10 minutes default (prevents cache poisoning)
    max_entries: int = 100   # Maximum number of cached queries
    _cache: Dict[str, CachedQuery] = field(default_factory=dict)
    
    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache."""
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.is_expired(self.ttl_seconds)
        ]
        for key in expired_keys:
            del self._cache[key]
    
    def _evict_oldest(self) -> None:
        """Evict the oldest entry if cache is full."""
        if len(self._cache) >= self.max_entries:
            oldest_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at
            )
            del self._cache[oldest_key]
    
    def store(
        self,
        sql: str,
        rows: List[Dict[str, Any]],
        columns: List[str]
    ) -> CachedQuery:
        """
        Store query results in the cache.
        
        Args:
            sql: The SQL query that was executed
            rows: Query results as list of dicts
            columns: Column names
            
        Returns:
            The CachedQuery entry with its ID
        """
        self._cleanup_expired()
        self._evict_oldest()
        
        query_id = str(uuid.uuid4())[:8]  # Short ID for convenience
        entry = CachedQuery(
            query_id=query_id,
            sql=sql,
            rows=rows,
            columns=columns,
            created_at=time.time(),
            row_count=len(rows)
        )
        self._cache[query_id] = entry
        return entry
    
    def get(self, query_id: str) -> Optional[CachedQuery]:
        """
        Get a cached query by ID.
        
        Args:
            query_id: The query ID to look up
            
        Returns:
            CachedQuery if found and not expired, None otherwise
        """
        entry = self._cache.get(query_id)
        if entry is None:
            return None
        
        if entry.is_expired(self.ttl_seconds):
            del self._cache[query_id]
            return None
        
        return entry
    
    def get_rows(
        self,
        query_id: str,
        offset: int = 0,
        limit: int = 50
    ) -> Optional[tuple[List[Dict[str, Any]], int]]:
        """
        Get paginated rows from a cached query.
        
        Args:
            query_id: The query ID to look up
            offset: Starting row index
            limit: Maximum number of rows to return
            
        Returns:
            Tuple of (rows, total_count) if found, None if not found
        """
        entry = self.get(query_id)
        if entry is None:
            return None
        
        rows = entry.rows[offset:offset + limit]
        return rows, entry.row_count
    
    def grep_rows(
        self,
        query_id: str,
        pattern: str,
        limit: int = 100
    ) -> Optional[tuple[List[Dict[str, Any]], int]]:
        """
        Search for rows matching a regex pattern across all columns.
        
        Args:
            query_id: The query ID to search in
            pattern: Regex pattern to search for
            limit: Maximum number of matching rows to return
            
        Returns:
            Tuple of (matching_rows, total_matches) if found, None if query not found
        """
        entry = self.get(query_id)
        if entry is None:
            return None
        
        try:
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        
        matching_rows = []
        total_matches = 0
        
        for row in entry.rows:
            # Check if any column value matches the pattern
            row_matches = any(
                regex.search(str(value)) 
                for value in row.values() 
                if value is not None
            )
            
            if row_matches:
                total_matches += 1
                if len(matching_rows) < limit:
                    matching_rows.append(row)
        
        return matching_rows, total_matches


# Global cache instance
_cache: Optional[QueryCache] = None


def get_cache() -> QueryCache:
    """Get or create the global cache instance."""
    global _cache
    if _cache is None:
        _cache = QueryCache()
    return _cache

