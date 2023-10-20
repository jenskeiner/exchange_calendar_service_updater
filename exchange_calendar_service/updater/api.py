import abc
from typing import Tuple
import datetime as dt

from exchange_calendars_extensions.api.changes import ChangeSetDict
from pydantic_settings import BaseSettings
from typing_extensions import Callable, Union

# Type for callbacks that receive changes.
ChangeHandler = Callable[[ChangeSetDict], None]

# Type for callbacks that receive errors.
ErrorHandler = Callable[[Exception], None]


class ChangeProviderError(Exception):
    """
    A type of exception that may be raised by change providers while processing changes.

    The exception may or may not be critical, i.e. the change provider may continue to process changes or abort the
    process. The change handler may not be invoked. Instead, a change provider should always catch the exception and
    pass it to the error handler before returning.
    """
    pass


class BaseChangeProvider(BaseSettings, abc.ABC):
    """
    Base class for change providers.

    Provides a dictionary of change sets for exchange calendars by invoking a given callback function.
    """

    # Callback function to invoke when changes are available.
    _callback: ChangeHandler = None

    @abc.abstractmethod
    def setup(self, change_handler: ChangeHandler, error_handler: ErrorHandler) -> None:
        """
        Set up the change provider.

        May immediately invoke the given callback.

        Parameters
        ----------
        change_handler : ChangeHandler
            Callback function to invoke when changes are available.
        error_handler : ErrorHandler
            Callback function to invoke when an error occurs.

        Returns
        -------
        None
        """
        pass

    def finalize(self) -> None:
        """
        Finalize the change provider.

        Returns
        -------
        None
        """
        pass


class RetryInfo:
    __slots__ = ("fails", "exception", "since")
    # The number of times the function has been called.
    fails: int
    exception: Exception
    since: dt.datetime

    def __init__(self, fails: int, exception: Exception, since: dt.datetime) -> None:
        self.fails = fails
        self.exception = exception
        self.since = since

    def update(self, exception: Exception) -> None:
        self.fails += 1
        self.exception = exception


RetryPolicyResult = Tuple[bool, Union[int, float]]


class BaseRetryPolicy(BaseSettings, abc.ABC):

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
        pass
