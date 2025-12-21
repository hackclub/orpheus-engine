import gzip
import json
from datetime import date
from typing import Any, ClassVar, Dict, List, Optional

import requests
from dagster import ConfigurableResource, EnvVar


class SlackAnalyticsApiError(Exception):
    """Custom exception for errors interacting with the Slack Analytics API."""
    pass


class SlackAnalyticsResource(ConfigurableResource):
    """
    A Dagster resource for retrieving Slack analytics using the admin.analytics.getFile API.
    Requires a Slack user token with the admin.analytics:read scope.
    
    API Reference: https://api.slack.com/methods/admin.analytics.getFile
    """
    
    user_token: str = EnvVar("SLACK_USER_TOKEN")
    
    SLACK_API_BASE_URL: ClassVar[str] = "https://slack.com/api"
    
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
        
        try:
            response = requests.post(url, headers=headers, data=data, timeout=60)
            
            # Check for JSON error response (API returns JSON on error)
            content_type = response.headers.get("Content-Type", "")
            
            if "application/json" in content_type:
                # This is an error response
                try:
                    error_data = response.json()
                    if not error_data.get("ok", False):
                        error_msg = error_data.get("error", "Unknown error")
                        raise SlackAnalyticsApiError(f"Slack API error: {error_msg}")
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
            raise SlackAnalyticsApiError("Request to Slack API timed out")
        except requests.exceptions.RequestException as e:
            error_detail = response.text[:500] if response else "No response"
            status_code = response.status_code if response else "N/A"
            raise SlackAnalyticsApiError(
                f"Request to Slack API failed (Status: {status_code}): {e}. Response: {error_detail}"
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
                response.raise_for_status()
                data = response.json()
                
                if not data.get("ok"):
                    error_msg = data.get("error", "Unknown error")
                    raise SlackAnalyticsApiError(f"Slack API error: {error_msg}")
                
                for user in data.get("users", []):
                    for workspace in user.get("workspaces", []):
                        team_ids.add(workspace)
                
                cursor = data.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
                    
            except requests.exceptions.Timeout:
                raise SlackAnalyticsApiError("Request to Slack API timed out")
            except requests.exceptions.RequestException as e:
                raise SlackAnalyticsApiError(f"Request to Slack API failed: {e}") from e
        
        return list(team_ids)

    def _get_users_for_team(self, team_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves all users for a specific team using users.list.
        
        Args:
            team_id: The team/workspace ID
            
        Returns:
            A list of user records with full profile information.
        """
        url = f"{self.SLACK_API_BASE_URL}/users.list"
        headers = {"Authorization": f"Bearer {self.user_token}"}
        
        all_users = []
        cursor = None
        
        while True:
            params = {"team_id": team_id, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if not data.get("ok"):
                    error_msg = data.get("error", "Unknown error")
                    raise SlackAnalyticsApiError(f"Slack API error for team {team_id}: {error_msg}")
                
                users = data.get("members", [])
                all_users.extend(users)
                
                cursor = data.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
                    
            except requests.exceptions.Timeout:
                raise SlackAnalyticsApiError("Request to Slack API timed out")
            except requests.exceptions.RequestException as e:
                raise SlackAnalyticsApiError(f"Request to Slack API failed: {e}") from e
        
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
            SlackAnalyticsApiError: If the API request fails.
        """
        url = f"{self.SLACK_API_BASE_URL}/users.profile.get"
        headers = {"Authorization": f"Bearer {self.user_token}"}
        
        try:
            response = requests.get(url, headers=headers, params={"user": user_id}, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("ok"):
                error_msg = data.get("error", "Unknown error")
                raise SlackAnalyticsApiError(f"Slack API error for user {user_id}: {error_msg}")
            
            return data.get("profile", {})
            
        except requests.exceptions.Timeout:
            raise SlackAnalyticsApiError(f"Request to Slack API timed out for user {user_id}")
        except requests.exceptions.RequestException as e:
            raise SlackAnalyticsApiError(f"Request to Slack API failed for user {user_id}: {e}") from e

