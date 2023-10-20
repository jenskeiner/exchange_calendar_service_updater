from typing import Literal

from pydantic import Field
from pydantic.types import NonNegativeInt, NonNegativeFloat

from exchange_calendar_service.updater.api import RetryInfo, BaseRetryPolicy, RetryPolicyResult


class FixedRetryPolicy(BaseRetryPolicy):
    """
    Retry policy that retries with exponential back-off.
    """

    type: Literal["fixed"]

    # The maximum number of times to retry.
    max_tries: NonNegativeInt = Field(default=5)

    # The number of seconds to wait before retrying.
    wait: NonNegativeFloat = Field(default=1.0)

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

        return True, self.wait
