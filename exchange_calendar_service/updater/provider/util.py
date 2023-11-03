import hashlib
import logging
from typing import Optional

import requests
from exchange_calendars_extensions.api.changes import ChangeSetDict
from yaml import safe_load

from exchange_calendar_service.updater.api import ChangeHandler, ErrorHandler, \
    ChangeProviderError

# Set up logging.
log = logging.getLogger(__name__)


def digest_changes(changes: ChangeSetDict) -> str:
    """
    Calculate digest of serialized changes.

    Parameters
    ----------
    changes
        Changes to calculate digest of.

    Returns
    -------
    str
        Digest of serialized changes.
    """
    return hashlib.sha256(changes.model_dump_json().encode('utf-8')).hexdigest()


def fetch_url(url: str, change_handler: ChangeHandler, error_handler: ErrorHandler, digest: Optional[str]) -> str:
    try:
        log.debug(f"Fetching URL {url}.")
        try:
            r = requests.get(url)
            r.raise_for_status()
        except Exception as e:
            raise ChangeProviderError(f"Error fetching URL {url}.") from e

        try:
            changes: ChangeSetDict = ChangeSetDict(**safe_load(r.text))
        except Exception as e:
            raise ChangeProviderError("Error loading changes from response body.") from e

        # Calculate digest of serialized changes.
        digest0 = hashlib.sha256(changes.model_dump_json().encode('utf-8')).hexdigest()

        if digest0 != digest:
            log.debug(f"Invoking change handler with changes from URL {url}.")

            # Invoke change handler.
            change_handler(changes)

            # Update digest. Do this after everything else to ensure that the next round does not
            # erroneously ignore the changes should the callback or anything else before this line fail.
            digest = digest0
    except ChangeProviderError as e:
        # Invoke error handler.
        error_handler(e)

    return digest
