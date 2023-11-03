try:
    import logging
    import threading
    import time

    from typing_extensions import Literal

    from exchange_calendar_service.updater.api import BaseChangeProvider, ChangeHandler, ErrorHandler

    from exchange_calendar_service.updater.provider.util import fetch_url

    # Set up logging.
    log = logging.getLogger(__name__)

    class UrlChangeProvider(BaseChangeProvider):
        """
        A change provider that fetches a URL periodically.

        The URL should point to a YAML document with a dictionary of change sets for exchange calendars. The URL must
        be accessible when the change provider is set up. If the URL is not accessible, the change provider will not
        be set up.
        """
        type: Literal["url"]
        url: str
        seconds: int
        _t: threading.Thread = None
        _digest: str = None

        def setup(self, change_handler: ChangeHandler, error_handler: ErrorHandler):
            # Set up a thread that periodically fetches the URL.

            import threading

            def target() -> None:
                while True:
                    time.sleep(self.seconds)
                    self._digest = fetch_url(self.url, change_handler, error_handler, self._digest)

            # Start thread.
            self._t = threading.Thread(target=target, daemon=True)
            self._digest = fetch_url(self.url, change_handler, error_handler, self._digest)
            self._t.start()

        def finalize(self):
            self._t.join()

except ImportError:
    # Required dependencies not available.
    pass
