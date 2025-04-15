import requests
from dagster import ConfigurableResource, InitResourceContext, EnvVar
from typing import Dict, Any, Optional

LOOPS_API_V1_BASE_URL = "https://app.loops.so/api/v1"

class LoopsApiError(Exception):
    # Custom exception for errors interacting with the Loops API.
    pass

class LoopsResource(ConfigurableResource):
    # A Dagster resource for interacting with the Loops.so API (v1).
    # Requires the LOOPS_API_KEY environment variable to be set.
    # API Reference: https://loops.so/docs/api-reference/contacts/update

    api_key: str = EnvVar("LOOPS_API_KEY")

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        # Helper method to make requests to the Loops API.
        url = f"{LOOPS_API_V1_BASE_URL}/{endpoint.lstrip('/')}"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json', # Explicitly accept JSON
        }

        response = None # Initialize response to None
        try:
            response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            response_data = response.json()

            # Check for API-level errors indicated in the response body
            if isinstance(response_data, dict) and not response_data.get("success"):
                error_message = response_data.get("message", "Unknown Loops API error")
                raise LoopsApiError(f"Loops API returned an error: {error_message}")

            return response_data

        except requests.exceptions.Timeout as e:
            raise LoopsApiError(f"Request to Loops API timed out: {url}") from e
        except requests.exceptions.RequestException as e:
            # Handle potential JSONDecodeError if response isn't valid JSON on error
            error_detail = response.text[:500] if response else "No response"
            status_code = response.status_code if response else "N/A"
            raise LoopsApiError(f"Request to Loops API failed ({method} {url}, Status: {status_code}): {e}. Response: {error_detail}") from e
        except ValueError as e: # Catches JSONDecodeError
             # Ensure response exists before trying to access its text
             response_text = response.text[:500] if response else "Invalid response object"
             raise LoopsApiError(f"Failed to decode JSON response from Loops API: {url}. Response: {response_text}") from e


    def update_contact(self, email: str, **kwargs: Any) -> Dict[str, Any]:
        # Updates an existing contact or creates a new one if the email doesn't exist.
        # Args:
        #     email: The contact's email address (required).
        #     **kwargs: Additional contact properties (e.g., firstName, lastName,
        #               subscribed, userGroup, userId, mailingLists dictionary,
        #               and any custom properties). Set a property to null to clear it.
        # Returns:
        #     A dictionary containing the success status and the contact ID upon success.
        #     Example: {"success": True, "id": "contact_id"}
        # Raises:
        #     LoopsApiError: If the API request fails or the API returns an error.
        endpoint = "/contacts/update"
        payload = {"email": email, **kwargs}

        # Ensure boolean values are actual booleans, not strings
        if 'subscribed' in payload and isinstance(payload['subscribed'], str):
            payload['subscribed'] = payload['subscribed'].lower() == 'true'

        # The API doc says send null to reset, so None values in kwargs should be valid JSON null.

        return self._make_request("PUT", endpoint, json=payload)

    # Potential future methods:
    # def find_contact(self, email: str) -> Optional[Dict[str, Any]]: ...
    # def delete_contact(self, email: str) -> Dict[str, Any]: ...
    # def create_contact(self, email: str, **kwargs: Any) -> Dict[str, Any]: ...
