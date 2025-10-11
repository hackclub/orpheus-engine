import requests
import urllib.parse
from enum import Enum
from typing import Optional

from dagster import ConfigurableResource, InitResourceContext, EnvVar, resource


class GenderizeResult(str, Enum):
    MALE = "male"
    FEMALE = "female"
    GENDER_NEUTRAL = "gender-neutral" # Based on probability threshold
    ERROR = "error" # Includes API errors or null gender results


class GenderizeApiError(Exception):
    """Custom exception for errors interacting with the Genderize.io API."""
    pass


class GenderizeResource(ConfigurableResource):
    """A Dagster resource for interacting with the Genderize.io API."""

    # Read API key from environment. Optional[str] implies None if not set.
    api_key: Optional[str] = EnvVar("GENDERIZE_API_KEY")

    base_url: str = "https://api.genderize.io" # Direct assignment for default

    probability_threshold: float = 0.75 # Direct assignment with default value

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Optional: Add logging or validation after config is loaded
        if not self.api_key:
            context.log.warning(
                "GENDERIZE_API_KEY environment variable not set. API calls will be rate-limited."
            )

    def get_gender(self, name: str, country_code: Optional[str] = None) -> GenderizeResult:
        """
        Predicts the gender of a name using the Genderize.io API.

        Args:
            name: The first name to genderize.
            country_code: Optional ISO 3166-1 alpha-2 country code to improve accuracy.

        Returns:
            A GenderizeResult enum member: MALE, FEMALE, GENDER_NEUTRAL, or ERROR.
        """
        if not name:
            raise ValueError("Name cannot be empty.")

        params = {"name": name}
        if self.api_key:
            params["apikey"] = self.api_key
        if country_code:
            params["country_id"] = country_code

        if country_code == "US":
            response = requests.get(f"https://nomen.sh/api/gender?name={urllib.parse.quote(name)}", timeout=15)
            if response.status_code == 200:
                data = response.json()
                gender = data.get("sex")
                if gender == "M":
                    return GenderizeResult.MALE
                elif gender == "F":
                    return GenderizeResult.FEMALE
                elif gender == "U":
                    return GenderizeResult.GENDER_NEUTRAL
                else:
                    return GenderizeResult.ERROR
        else:
            try:
                url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
                response = requests.get(url, timeout=15)
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                data = response.json()

                if data.get("error"):
                    raise GenderizeApiError(f"Genderize.io API error: {data['error']}")

                gender = data.get("gender")
                probability = data.get("probability")

                if gender is None:
                    # Treat null gender as an error/unknown case per the JS logic intent
                    return GenderizeResult.ERROR

                if probability is None:
                    # Handle case where probability might be missing (shouldn't happen with valid gender)
                    # Treat as error or potentially make a default assumption?
                    # For now, mirroring the JS logic where only probability < threshold matters.
                    # If gender is present but probability is missing, return the gender.
                    if gender == "male":
                        return GenderizeResult.MALE
                    elif gender == "female":
                        return GenderizeResult.FEMALE
                    else:
                        # Unexpected gender value from API
                        return GenderizeResult.ERROR

                # Apply the probability threshold
                if probability < self.probability_threshold:
                    return GenderizeResult.GENDER_NEUTRAL

                # Return the direct gender if probability is sufficient
                if gender == "male":
                    return GenderizeResult.MALE
                elif gender == "female":
                    return GenderizeResult.FEMALE
                else:
                    # Should not happen if API schema is consistent
                    return GenderizeResult.ERROR

            except requests.exceptions.Timeout as e:
                raise GenderizeApiError(f"Request to Genderize API timed out: {url}") from e
            except requests.exceptions.RequestException as e:
                # Handle potential JSONDecodeError if response isn't valid JSON on error
                error_detail = response.text[:500] if 'response' in locals() and response else "No response"
                status_code = response.status_code if 'response' in locals() and response else "N/A"
                raise GenderizeApiError(f"Request to Genderize API failed ({url}, Status: {status_code}): {e}. Response: {error_detail}") from e
            except ValueError as e: # Catches JSONDecodeError
                response_text = response.text[:500] if 'response' in locals() and response else "Invalid response object"
                raise GenderizeApiError(f"Failed to decode JSON response from Genderize API: {url}. Response: {response_text}") from e
            except GenderizeApiError: # Re-raise specific API errors
                raise
            except Exception as e: # Catch any other unexpected errors
                raise GenderizeApiError(f"An unexpected error occurred while calling Genderize API: {e}") from e


@resource(description="Genderize.io API client resource")
def genderize_resource(init_context: InitResourceContext) -> GenderizeResource:
    """Factory function for the GenderizeResource."""
    # Configuration for api_key (via EnvVar) and probability_threshold is handled by Dagster
    return GenderizeResource(
        api_key=init_context.resource_config.get("api_key"),
        probability_threshold=init_context.resource_config.get("probability_threshold", 0.75)
    )
