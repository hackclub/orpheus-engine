from dagster import ConfigurableResource, InitResourceContext, resource
from pydantic import BaseModel
from typing import Type
import litellm
import os

# Ensure necessary API keys (e.g., OPENAI_API_KEY) are set in the environment
# where Dagster processes run, or configure LiteLLM credentials directly.
# Example: os.environ["OPENAI_API_KEY"] = "your_key"


class AIResource(ConfigurableResource):
    """
    A Dagster resource for interacting with AI models via LiteLLM,
    specifically designed for generating structured responses based on a Pydantic schema.

    Uses LiteLLM's `response_model` feature to enforce output structure. Requires
    that the underlying LLM provider (e.g., OpenAI, Anthropic) supports structured output
    or function calling.
    """

    def generate_structured_response(
        self, prompt: str, response_schema: Type[BaseModel], model: str = "gpt-4o"
    ) -> BaseModel:
        """
        Generates a structured response from an AI model using LiteLLM.

        Args:
            prompt: The input prompt for the AI model.
            response_schema: The Pydantic model defining the desired structure of the response.
            model: The specific LiteLLM model identifier to use (defaults to 'gpt-4o').
                     Ensure this model supports structured output/function calling.

        Returns:
            An instance of the response_schema populated with the AI's response.

        Raises:
            ValueError: If the response from LiteLLM does not conform to the expected schema,
                        cannot be parsed, or if the API call fails in a known way.
            Exception: Propagates underlying exceptions from the LiteLLM library or API calls.
        """
        try:
            # Directly pass the Pydantic model class to response_format
            response = litellm.completion(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format=response_schema # Pass the Pydantic model class directly
            )

            # According to LiteLLM docs, when response_format is a Pydantic model,
            # the response should be an instance of that model.
            if isinstance(response, response_schema):
                return response
            # Add a check for the structure seen in the first example of the docs, just in case.
            elif (
                hasattr(response, "choices")
                and response.choices
                and hasattr(response.choices[0], "message")
                and response.choices[0].message
                and hasattr(response.choices[0].message, "content")
                and isinstance(response.choices[0].message.content, str)
            ):
                 try:
                    # Attempt to parse the string content as JSON into the Pydantic model
                    parsed_content = response_schema.model_validate_json(
                        response.choices[0].message.content
                    )
                    return parsed_content
                 except Exception as parse_error:
                    raise ValueError(
                        f"LiteLLM response was not a direct Pydantic model instance, "
                        f"and parsing message content failed. Parse error: {parse_error}. "
                        f"Response content: {response.choices[0].message.content}"
                    ) from parse_error
            else:
                # If the response is neither the direct model instance nor the expected string format
                raise ValueError(
                    f"LiteLLM did not return the expected Pydantic model instance or a known parsable structure. "
                    f"Received type: {type(response)}. Response: {response}"
                )

        except Exception as e:
            # In a real application, use context.log for better logging within Dagster
            print(f"Error during LiteLLM completion call for model '{model}': {e}")
            # Consider more specific error handling based on LiteLLM exceptions
            raise


@resource(description="LiteLLM resource for generating structured AI responses.")
def ai_resource_instance(init_context: InitResourceContext) -> AIResource:
    """
    Dagster resource factory for the AIResource.

    This resource facilitates interaction with LLMs via LiteLLM for structured data generation.
    Ensure API keys are available in the environment (e.g., OPENAI_API_KEY).
    """
    # Resource configuration can be added here if needed in the future,
    # e.g., default model, API keys, retry logic.
    # config = init_context.resource_config
    return AIResource()
