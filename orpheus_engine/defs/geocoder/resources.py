import googlemaps
import googlemaps.exceptions as google_exceptions
from dagster import ConfigurableResource, InitResourceContext, EnvVar

class GeocodingError(Exception):
    """Custom exception for errors during geocoding operations."""
    pass

class GeocoderResource(ConfigurableResource):
    """A Dagster resource for interacting with the Google Maps Geocoding API."""

    google_maps_api_key: str = EnvVar("GOOGLE_MAPS_API_KEY")

    _client: googlemaps.Client | None = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # EnvVar handles the presence check, so we don't need to check here.
        try:
            self._client = googlemaps.Client(key=self.google_maps_api_key)
        except Exception as e:
             raise GeocodingError(f"Failed to initialize Google Maps client: {e}") from e

    @property
    def client(self) -> googlemaps.Client:
        if self._client is None:
            # This shouldn't happen in normal execution as setup_for_execution is called first,
            # but raise a specific error if it does.
            raise GeocodingError("Resource is not initialized. Call setup_for_execution first.")
        return self._client

    def geocode(self, address: str, **kwargs) -> list[dict]:
        """Geocode an address.

        Raises:
            GeocodingError: If any error occurs during the geocoding API call.
        """
        try:
            return self.client.geocode(address, **kwargs)
        except (google_exceptions.ApiError, google_exceptions.HTTPError, google_exceptions.Timeout, google_exceptions.TransportError) as e:
             # Wrap the provider-specific exception in our generic error
             raise GeocodingError(f"Google Maps API geocoding failed for address '{address}': {e}") from e
        except Exception as e:
             # Catch any other unexpected errors during the client call
             raise GeocodingError(f"An unexpected error occurred during geocoding for address '{address}': {e}") from e

    def reverse_geocode(self, latlng: tuple[float, float] | dict[str, float], **kwargs) -> list[dict]:
        """Reverse geocode a latitude/longitude coordinate.

        Raises:
            GeocodingError: If any error occurs during the reverse geocoding API call.
        """
        try:
            return self.client.reverse_geocode(latlng, **kwargs)
        except (google_exceptions.ApiError, google_exceptions.HTTPError, google_exceptions.Timeout, google_exceptions.TransportError) as e:
             # Wrap the provider-specific exception in our generic error
             raise GeocodingError(f"Google Maps API reverse geocoding failed for lat/lng '{latlng}': {e}") from e
        except Exception as e:
            # Catch any other unexpected errors during the client call
             raise GeocodingError(f"An unexpected error occurred during reverse geocoding for lat/lng '{latlng}': {e}") from e
