from pydantic import create_model, Field, AnyHttpUrl, StringConstraints
from pydantic_settings import BaseSettings
from typing_extensions import Literal, Annotated, Union

from exchange_calendar_service.updater.util import discover_entrypoints

LogLevel = Literal['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']

APIKey = Annotated[str, StringConstraints(max_length=256)]

AvailableProviderType = Union[tuple(discover_entrypoints("exchange_calendar_service_updater.providers")().values())]

AvailableRetryPoliciesType = Union[tuple(discover_entrypoints("exchange_calendar_service_updater.retry_policies")().values())]

AppSettings = create_model("Settings", provider=(AvailableProviderType, Field(discriminator="type")),
                           url=(AnyHttpUrl, ...), api_key=(APIKey, ...), log_level=(LogLevel, 'INFO'),
                           retry_policy=(AvailableRetryPoliciesType, Field(discriminator="type")),
                           __base__=BaseSettings)

discover_retry_policies = discover_entrypoints("exchange_calendar_service_updater.retry_policies")
