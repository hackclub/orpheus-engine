import gzip
import json
import time
from datetime import date
from typing import Any, Callable, ClassVar, Dict, Generator, List, Optional

import requests
from dagster import ConfigurableResource, EnvVar, get_dagster_logger


class SlackAnalyticsApiError(Exception):
    """Custom exception for errors interacting with the Slack Analytics API."""
    pass


class SlackRateLimitError(SlackAnalyticsApiError):
    """Exception raised when Slack API returns a rate limit (429) response."""
    def __init__(self, message: str, retry_after: int = 60):
        super().__init__(message)
        self.retry_after = retry_after


class SlackAnalyticsResource(ConfigurableResource):
    """
    A Dagster resource for retrieving Slack analytics using the admin.analytics.getFile API.
    Requires a Slack user token with the admin.analytics:read scope.
    
    API Reference: https://api.slack.com/methods/admin.analytics.getFile
    """
    
    user_token: str = EnvVar("SLACK_USER_TOKEN")
    
    SLACK_API_BASE_URL: ClassVar[str] = "https://slack.com/api"
    
    def _check_rate_limit(self, response: requests.Response, context: str = "") -> None:
        """
        Check if response is rate limited and raise SlackRateLimitError if so.
        
        Args:
            response: The requests Response object
            context: Optional context string for error message (e.g. "for user X")
        """
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            raise SlackRateLimitError(
                f"Rate limited{' ' + context if context else ''}",
                retry_after=retry_after
            )
    
    def _check_json_error(self, data: Dict[str, Any], context: str = "") -> None:
        """
        Check JSON response for errors and raise appropriate exception.
        
        Args:
            data: The parsed JSON response
            context: Optional context string for error message
        """
        if not data.get("ok"):
            error_msg = data.get("error", "Unknown error")
            if error_msg == "ratelimited":
                raise SlackRateLimitError(f"Rate limited{' ' + context if context else ''}", retry_after=60)
            raise SlackAnalyticsApiError(f"Slack API error{' ' + context if context else ''}: {error_msg}")
    
    def _make_request_with_retry(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        context: str = ""
    ) -> Dict[str, Any]:
        """
        Make a Slack API request with automatic rate limit handling and retries.
        
        Args:
            method: HTTP method ('get' or 'post')
            endpoint: API endpoint (e.g. 'admin.teams.list')
            params: Query parameters (for GET)
            data: Form data (for POST)
            timeout: Request timeout in seconds
            context: Context string for error/log messages
            
        Returns:
            Parsed JSON response dict
            
        Raises:
            SlackAnalyticsApiError: If the request fails after retries
        """
        url = f"{self.SLACK_API_BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {self.user_token}"}
        log = get_dagster_logger()
        
        while True:
            try:
                if method.lower() == 'get':
                    response = requests.get(url, headers=headers, params=params, timeout=timeout)
                else:
                    response = requests.post(url, headers=headers, data=data, timeout=timeout)
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    log.warning(f"Rate limited{' ' + context if context else ''}, sleeping {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                result = response.json()
                
                # Check for rate limit in JSON response
                if not result.get("ok"):
                    error_msg = result.get("error", "Unknown error")
                    if error_msg == "ratelimited":
                        log.warning(f"Rate limited{' ' + context if context else ''}, sleeping 60s...")
                        time.sleep(60)
                        continue
                    raise SlackAnalyticsApiError(f"Slack API error{' ' + context if context else ''}: {error_msg}")
                
                return result
                
            except requests.exceptions.Timeout:
                raise SlackAnalyticsApiError(f"Request to Slack API timed out{' ' + context if context else ''}")
            except SlackAnalyticsApiError:
                raise
            except requests.exceptions.RequestException as e:
                raise SlackAnalyticsApiError(f"Request to Slack API failed{' ' + context if context else ''}: {e}") from e
    
    def _make_analytics_request(
        self, 
        analytics_type: str, 
        target_date: Optional[date] = None,
        metadata_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Makes a request to the admin.analytics.getFile API endpoint.
        
        Args:
            analytics_type: Either "member" or "public_channel"
            target_date: The date to retrieve analytics for (required unless metadata_only=True)
            metadata_only: If True, retrieves channel metadata instead of analytics data
            
        Returns:
            A list of dictionaries, each representing one line of the analytics data.
            
        Raises:
            SlackRateLimitError: If rate limited (includes retry_after seconds).
            SlackAnalyticsApiError: If the API request fails.
        """
        url = f"{self.SLACK_API_BASE_URL}/admin.analytics.getFile"
        
        headers = {
            "Authorization": f"Bearer {self.user_token}",
        }
        
        data = {
            "type": analytics_type,
        }
        
        if metadata_only:
            data["metadata_only"] = "true"
        elif target_date:
            data["date"] = target_date.strftime("%Y-%m-%d")
        else:
            raise SlackAnalyticsApiError("Either target_date or metadata_only=True must be provided")
        
        context = f"for {analytics_type}" + (f" on {target_date}" if target_date else "")
        
        try:
            response = requests.post(url, headers=headers, data=data, timeout=60)
            
            # Check for rate limiting first
            self._check_rate_limit(response, context)
            
            # Check for JSON error response (API returns JSON on error)
            content_type = response.headers.get("Content-Type", "")
            
            if "application/json" in content_type:
                # This is an error response
                try:
                    error_data = response.json()
                    self._check_json_error(error_data, context)
                except json.JSONDecodeError:
                    pass
                    
            # Check HTTP status
            response.raise_for_status()
            
            # Decompress the gzip response
            if "application/gzip" in content_type or response.content[:2] == b'\x1f\x8b':
                try:
                    decompressed_data = gzip.decompress(response.content)
                    content = decompressed_data.decode("utf-8")
                except gzip.BadGzipFile as e:
                    raise SlackAnalyticsApiError(f"Failed to decompress gzip response: {e}") from e
            else:
                content = response.text
            
            # Parse newline-delimited JSON
            records = []
            for line in content.strip().split("\n"):
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        raise SlackAnalyticsApiError(f"Failed to parse JSON line: {e}") from e
            
            return records
            
        except requests.exceptions.Timeout:
            raise SlackAnalyticsApiError(f"Request to Slack API timed out {context}")
        except (SlackRateLimitError, SlackAnalyticsApiError):
            raise  # Re-raise our custom exceptions
        except requests.exceptions.RequestException as e:
            error_detail = response.text[:500] if response else "No response"
            status_code = response.status_code if response else "N/A"
            raise SlackAnalyticsApiError(
                f"Request to Slack API failed {context} (Status: {status_code}): {e}. Response: {error_detail}"
            ) from e
    
    def get_member_analytics(self, target_date: date) -> List[Dict[str, Any]]:
        """
        Retrieves member analytics for a specific date.
        
        Args:
            target_date: The date to retrieve analytics for (in UTC).
            
        Returns:
            A list of member analytics records. Each record contains fields like:
            - enterprise_id, user_id, email_address
            - is_guest, is_billable_seat, is_active
            - is_active_ios, is_active_android, is_active_desktop
            - reactions_added_count, messages_posted_count
            - channel_messages_posted_count, files_added_count
            - total_calls_count, slack_calls_count, slack_huddles_count
            - search_count, date_claimed
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        return self._make_analytics_request("member", target_date=target_date)
    
    def get_channel_analytics(self, target_date: date) -> List[Dict[str, Any]]:
        """
        Retrieves public channel analytics for a specific date.
        
        Args:
            target_date: The date to retrieve analytics for (in UTC).
            
        Returns:
            A list of channel analytics records. Each record contains fields like:
            - enterprise_id, team_id, channel_id
            - originating_team (dict with team_id and name)
            - date_created, date_last_active
            - total_members_count, full_members_count, guest_member_count
            - messages_posted_count, messages_posted_by_members_count
            - members_who_viewed_count, members_who_posted_count
            - reactions_added_count
            - visibility, channel_type
            - is_shared_externally, shared_with, externally_shared_with_organizations
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        return self._make_analytics_request("public_channel", target_date=target_date)
    
    def get_channel_metadata(self) -> List[Dict[str, Any]]:
        """
        Retrieves metadata for all public channels (names, topics, descriptions).
        
        Returns:
            A list of channel metadata records. Each record contains:
            - channel_id, name, topic, description, date
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        return self._make_analytics_request("public_channel", metadata_only=True)

    def _get_team_ids_from_admin_users(self) -> List[str]:
        """
        Gets all unique team IDs by scanning workspaces from admin.users.list.
        
        Returns:
            A list of unique team IDs.
            
        Raises:
            SlackRateLimitError: If rate limited (includes retry_after seconds).
            SlackAnalyticsApiError: If the API request fails.
        """
        url = f"{self.SLACK_API_BASE_URL}/admin.users.list"
        headers = {"Authorization": f"Bearer {self.user_token}"}
        
        team_ids = set()
        cursor = None
        
        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                
                # Check for rate limiting first
                self._check_rate_limit(response, "for admin.users.list")
                
                response.raise_for_status()
                data = response.json()
                
                self._check_json_error(data, "for admin.users.list")
                
                for user in data.get("users", []):
                    for workspace in user.get("workspaces", []):
                        team_ids.add(workspace)
                
                cursor = data.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
                    
            except requests.exceptions.Timeout:
                raise SlackAnalyticsApiError("Request to Slack API timed out for admin.users.list")
            except (SlackRateLimitError, SlackAnalyticsApiError):
                raise  # Re-raise our custom exceptions
            except requests.exceptions.RequestException as e:
                raise SlackAnalyticsApiError(f"Request to Slack API failed for admin.users.list: {e}") from e
        
        return list(team_ids)

    def _get_users_for_team(self, team_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves all users for a specific team using users.list.
        
        Args:
            team_id: The team/workspace ID
            
        Returns:
            A list of user records with full profile information.
            
        Raises:
            SlackRateLimitError: If rate limited (includes retry_after seconds).
            SlackAnalyticsApiError: If the API request fails.
        """
        url = f"{self.SLACK_API_BASE_URL}/users.list"
        headers = {"Authorization": f"Bearer {self.user_token}"}
        context = f"for team {team_id}"
        
        all_users = []
        cursor = None
        
        while True:
            params = {"team_id": team_id, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                
                # Check for rate limiting first
                self._check_rate_limit(response, context)
                
                response.raise_for_status()
                data = response.json()
                
                self._check_json_error(data, context)
                
                users = data.get("members", [])
                all_users.extend(users)
                
                cursor = data.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
                    
            except requests.exceptions.Timeout:
                raise SlackAnalyticsApiError(f"Request to Slack API timed out {context}")
            except (SlackRateLimitError, SlackAnalyticsApiError):
                raise  # Re-raise our custom exceptions
            except requests.exceptions.RequestException as e:
                raise SlackAnalyticsApiError(f"Request to Slack API failed {context}: {e}") from e
        
        return all_users

    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Retrieves all user metadata across all teams in the Enterprise Grid.
        
        First discovers all team IDs from admin.users.list, then fetches full
        user profiles from each team using users.list (which allows limit=1000).
        
        Returns:
            A list of user records with full profile information including:
            - id, name, team_id, deleted, updated, real_name
            - tz, tz_label, tz_offset
            - is_admin, is_owner, is_primary_owner, is_restricted, is_ultra_restricted
            - is_bot, is_app_user, is_email_confirmed, has_2fa
            - profile (with email, display_name, first_name, last_name, title, phone, etc.)
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        # First, get all team IDs
        team_ids = self._get_team_ids_from_admin_users()
        
        # Fetch users from each team, deduplicating by user ID
        users_by_id = {}
        for team_id in team_ids:
            team_users = self._get_users_for_team(team_id)
            for user in team_users:
                user_id = user.get("id")
                if user_id and user_id not in users_by_id:
                    users_by_id[user_id] = user
        
        return list(users_by_id.values())

    def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Retrieves a single user's profile using users.profile.get.
        This includes custom profile fields that aren't in users.list.
        
        Args:
            user_id: The Slack user ID
            
        Returns:
            The user's profile dict including fields.
            
        Raises:
            SlackRateLimitError: If rate limited (includes retry_after seconds).
            SlackAnalyticsApiError: If the API request fails for other reasons.
        """
        url = f"{self.SLACK_API_BASE_URL}/users.profile.get"
        headers = {"Authorization": f"Bearer {self.user_token}"}
        context = f"for user {user_id}"
        
        try:
            response = requests.get(url, headers=headers, params={"user": user_id}, timeout=30)
            
            # Check for rate limiting first
            self._check_rate_limit(response, context)
            
            response.raise_for_status()
            data = response.json()
            
            self._check_json_error(data, context)
            
            return data.get("profile", {})
            
        except requests.exceptions.Timeout:
            raise SlackAnalyticsApiError(f"Request to Slack API timed out {context}")
        except (SlackRateLimitError, SlackAnalyticsApiError):
            raise  # Re-raise our custom exceptions
        except requests.exceptions.RequestException as e:
            raise SlackAnalyticsApiError(f"Request to Slack API failed {context}: {e}") from e

    def get_teams(self) -> List[Dict[str, Any]]:
        """
        Retrieves all teams via admin.teams.list.
        
        Returns:
            A list of team records with id, name, etc.
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        teams = []
        cursor = None
        
        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            
            data = self._make_request_with_retry(
                method='get',
                endpoint='admin.teams.list',
                params=params,
                context="for admin.teams.list"
            )
            
            teams.extend(data.get("teams", []))
            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        
        return teams

    def get_users_for_team_paginated(
        self, 
        team_id: str, 
        limit: int = 1000
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Retrieves users for a team, yielding pages for interruptable processing.
        
        Args:
            team_id: The team/workspace ID
            limit: Number of users per page (max 1000)
            
        Yields:
            Lists of user records (one list per page)
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        cursor = None
        
        while True:
            params = {"team_id": team_id, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            
            data = self._make_request_with_retry(
                method='get',
                endpoint='users.list',
                params=params,
                timeout=120,
                context=f"for users.list team {team_id}"
            )
            
            users = data.get("members", [])
            if users:
                yield users
            
            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

    def get_profile_field_definitions(self) -> Dict[str, str]:
        """
        Retrieves profile field definitions from team.profile.get.
        
        Returns:
            A dict mapping field ID to field label.
            
        Raises:
            SlackAnalyticsApiError: If the API request fails.
        """
        data = self._make_request_with_retry(
            method='get',
            endpoint='team.profile.get',
            context="for team.profile.get"
        )
        
        field_id_to_label = {}
        for field in data.get("profile", {}).get("fields", []):
            field_id_to_label[field["id"]] = field.get("label", field["id"])
        
        return field_id_to_label

