import asyncio
import signal
import sys
import threading
import time
from threading import Condition
from typing import NoReturn, Literal, Union

from exchange_calendars_extensions.api.changes import ChangeSetDict
from pydantic import validate_call, BaseModel, Field, AnyHttpUrl
import logging
import requests

from exchange_calendar_service.updater.api import RetryInfo
from exchange_calendar_service.updater.settings import AppSettings, APIKey

# Get logger.
log = logging.getLogger(__name__)


Sentinel = Literal['sentinel']


class IncomingChanges(BaseModel):
    """Holds incoming changes from the provider."""

    # Incoming changes.
    changes: Union[ChangeSetDict, Sentinel] = Field(default=None)

    def pop(self) -> ChangeSetDict:
        """Pop incoming changes and return them."""
        changes: ChangeSetDict = self.changes
        self.changes = None
        return changes


# Incoming changes from the provider.
_incoming: IncomingChanges = IncomingChanges()

# Condition to synchronize access to incoming changes, and to notify main loop of new changes from the provider.
_condition: Condition = Condition()


def _on_changes0(changes: ChangeSetDict, condition: Condition, incoming: IncomingChanges):
    """Handle new changes from the provider.

    Will be called by a change provider whenever new changes are available. Takes possession of the provided changes,
    saves them to the given holder object, and notifies the given condition, all provided that the changes are actually
    different from the last changes saved to the holder.

    Parameters
    ----------
    changes : ChangeSetDict
        Changes from the provider.
    condition : Condition
        Condition to synchronize access to holder.
    incoming : IncomingChanges
        Changes holder.
    """
    with condition:
        log.info(f'_on_changes called with changes={changes}.')
        incoming_changes = incoming.changes
        if changes != incoming_changes:
            log.debug(f'Incoming changes changed from {incoming_changes} to {changes}. Notifying condition.')
            incoming.changes = changes.model_copy()
            condition.notify()


@validate_call
def _on_changes(changes: ChangeSetDict):
    """Handle new changes from the provider.

    Identical to _on_changes0(...), but with the condition and incoming arguments bound to the global variables
    _condition and _incoming.

    Also performs validation of the changes argument to ensure it is not None and a valid ChangeSetDict.
    """
    _on_changes0(changes, _condition, _incoming)


def _on_error(error: Exception):
    """Handle errors from the provider.

    Will be called by a change provider whenever an error occurs. Logs the error.

    Parameters
    ----------
    error : Exception
        The error that occurred.
    """
    log.info(f'on_error called with error={error}.', exc_info=True)


def _notify(url: AnyHttpUrl, api_key: APIKey, changes: ChangeSetDict) -> ChangeSetDict:
    """Notify target of changes.

    Parameters
    ----------
    url : AnyHttpUrl
        URL to notify.
    api_key : APIKey
        API key to use for authentication.
    changes : ChangeSetDict
        Changes to notify.

    Returns
    -------
    ChangeSetDict
        The changes that were notified.
    """
    r: requests.Response = requests.post(url=url, json=changes.model_dump_json(), headers={'Authorization': f'Bearer {api_key}'}, timeout=5.0)
    r.raise_for_status()


async def _main_loop_subtask(condition: threading.Condition, incoming: IncomingChanges, settings: AppSettings) -> None:
    # Pending changes to notify target of.
    changes_pending: ChangeSetDict = None

    # Last successfully changes the target was notified of.
    changes_last: ChangeSetDict = None

    # Main loop runs indefinitely.
    while True:
        # Block current thread and wait until condition notifies us of new incoming changes from the provider.
        with condition:
            # Get the incoming changes.
            changes: ChangeSetDict = incoming.pop()

            if changes is None:
                condition.wait_for(lambda: incoming.changes is not None)

                # Get the incoming changes.
                changes: ChangeSetDict = incoming.pop()

        if changes is None:
            # Should never happen, but just in case.
            log.debug('No incoming changes.')
            continue
        elif changes == 'sentinel':
            log.debug('Exiting main loop.')
            return

        # The provider has notified us of new incoming changes.
        log.debug(f'Handling new incoming changes {changes}.')

        # There should be no pending changes, i.e. previous changes that the target has not yet been notified of.
        assert changes_pending is None

        if changes_last == changes:
            # The changes from the provider are identical to the last changes successfully notified to the target.
            log.debug(f'Ignoring incoming changes {changes} as they are the same as last notified changes.')
            continue
        else:
            # The changes from the provider are actually different from the last changes successfully notified to the
            # target.
            log.debug(f'Setting pending changes to incoming changes {changes}.')
            # Set pending changes to incoming changes.
            changes_pending = changes

        # Notify target of changes.

        # Initialize retry info.
        retry_info: RetryInfo = RetryInfo(fails=0, exception=None, since=time.time())

        # Number of seconds to wait.
        wait = 0

        # Number of attempts.
        attempts = 0

        while changes_pending:
            try:
                if wait > 0:
                    log.debug(f'Waiting for {wait} seconds before retrying.')
                    time.sleep(wait)

                attempts += 1

                log.debug(f'Attempt #{attempts} to notify changes {changes_pending}.')

                # Notify target of pending changes. If this method returns, it means that the target has been
                # successfully notified of the changes. The return value are the changes that were notified.
                changes_last = _notify(url=settings.url, api_key=settings.api_key, changes=changes_pending)

                # If we get here, changes have been successfully notified, i.e. no changes are pending anymore.
                changes_pending = None
            except Exception as e:
                # There was an error notifying the target of changes.
                log.debug(f'Error notifying changes: {e}')

                # Update retry info.
                retry_info.update(e)

                # Let retry policy determine whether we should retry and how long to wait.
                should_retry, wait = settings.retry_policy(retry_info)

                if not should_retry:
                    # Retry policy says we should not retry.
                    log.debug(f'Retry policy says we should not retry after {retry_info.fails} fails since {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(retry_info.since))}.')
                    changes_pending = None
                else:
                    log.debug(f'Retry policy says we should retry in {wait} seconds.')
                    continue
            finally:
                if changes_pending is not None:
                    # After one unsuccessful attempt at notifying the target of changes, after which we are supposed to
                    # continue, we check if there are new incoming changes from the provider in the meantime.
                    with condition:
                        changes: ChangeSetDict = incoming.pop()

                    if changes is not None:
                        # While we were trying to notify the target of changes, the provider has notified us of new
                        # incoming changes.
                        log.debug(f'New incoming changes {changes} while notifying changes.')
                        if changes == 'sentinel':
                            log.debug('Exiting main loop.')
                            return
                        else:
                            # Re-start the notification loop with the new changes from the provider.
                            #
                            # Note that we don't check here if the new changes are different from the last changes
                            # successfully notified to the target. This is to make sure that once we try to notify the
                            # target of changes, we continue until we have succeeded, or the retry policy has stopped
                            # us. In the worst case, we would perhaps notify the target of the same changes multiple
                            # times. But that is better than abandoning an ongoing notification loop prematurely.

                            log.debug(f'Setting pending changes to incoming changes {changes}.')
                            changes_pending = changes

                            # Reset retry info.
                            retry_info = RetryInfo(fails=0, exception=None, since=time.time())

                            # Reset wait period.
                            wait = 0
                else:
                    # We are done.
                    log.info('Exiting target notification loop.')
                    break


async def _main_loop_task(condition: Condition, incoming: IncomingChanges, settings: AppSettings):
    await asyncio.gather(_main_loop_subtask(condition=condition, incoming=incoming, settings=settings))


def _main_loop(condition: Condition, incoming: IncomingChanges, settings: AppSettings):
    asyncio.run(_main_loop_task(condition=condition, incoming=incoming, settings=settings))


def main():
    def _exit() -> NoReturn:
        """Exit the program.

        Used as a signal handler.
        """
        sys.exit(0)

    # Register signal handlers.
    signal.signal(signal.SIGTERM, lambda signum, frame: _exit())
    signal.signal(signal.SIGINT, lambda signum, frame: _exit())

    # Load settings from environment variables.
    settings = AppSettings(_env_prefix='app_', _env_nested_delimiter='__')

    # Configure logging with a handler for stdout and a standard message format.
    logging.basicConfig(level=settings.log_level, format='%(asctime)s [%(levelname)-8s] - %(threadName)s - %(name)s - %(message)s')

    # Set up change provider with change and error handlers, and start it.
    # This will typically create a thread that will invoke the change handler whenever changes are detected.
    settings.provider.setup(_on_changes, _on_error)

    try:
        # Run main loop in main thread where we wait for incoming changes from the provider to notify the target.
        _main_loop(condition=_condition, incoming=_incoming, settings=settings)
    finally:
        # Run any provider teardown code.
        settings.provider.finalize()


if __name__ == "__main__":
    main()
