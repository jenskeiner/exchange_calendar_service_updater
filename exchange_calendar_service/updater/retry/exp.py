from typing import Literal

from pydantic import Field, model_validator
from pydantic.types import NonNegativeInt, NonNegativeFloat

from exchange_calendar_service.updater.api import RetryInfo, BaseRetryPolicy, RetryPolicyResult


class ExponentialBackOffRetryPolicy(BaseRetryPolicy):
    """
    Retry policy that retries with exponential back-off.
    """

    type: Literal["exponential_back_off"]

    # The maximum number of times to retry.
    max_tries: NonNegativeInt = Field(default=5)

    # The initial number of seconds to wait before retrying.
    initial_wait: NonNegativeFloat = Field(default=1.0)

    # The maximum number of seconds to wait before retrying.
    max_wait: NonNegativeFloat = Field(default=60.0)

    # The multiplier to apply to the previous wait time to get the next wait time.
    multiplier: NonNegativeFloat = Field(default=2.0)

    @model_validator(mode='after')
    def _validate_wait_consistency(self) -> 'ExponentialBackOffRetryPolicy':
        if self.initial_wait > self.max_wait:
            raise ValueError(f"initial_wait ({self.initial_wait}) must be less than or equal to max_wait ({self.max_wait}).")

        return self

    def __call__(self, info: RetryInfo) -> RetryPolicyResult:
        """
        Handle an error.

        Parameters
        ----------
        info : RetryInfo
            Information about the execution.

        Returns
        -------
        RetryPolicyResult
            A tuple of a boolean indicating whether to retry and an int or float indicating the number of seconds to
            wait before retrying.
        """
        if info.fails >= self.max_tries:
            return False, 0.0

        wait = min(self.max_wait, self.initial_wait * self.multiplier ** info.fails)

        return True, wait
