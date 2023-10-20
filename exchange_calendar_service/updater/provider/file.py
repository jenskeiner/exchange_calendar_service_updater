try:
    import logging
    import hashlib
    from pathlib import Path

    from exchange_calendars_extensions.api.changes import ChangeSetDict
    from pydantic import AfterValidator
    from typing_extensions import Literal, Annotated

    from watchdog.events import FileModifiedEvent
    from watchdog.observers import Observer
    from yaml import safe_load

    from exchange_calendar_service.updater.api import BaseChangeProvider, ChangeHandler, ErrorHandler, \
        ChangeProviderError

    # Set up logging.
    log = logging.getLogger(__name__)

    # Type alias for Path that resolves the path on initialization.
    ResolvedPath = Annotated[Path, AfterValidator(lambda p: p.resolve())]

    class FileChangeProvider(BaseChangeProvider):
        """
        A change provider that watches a file for changes.

        The file should contain a YAML document with a dictionary of change sets for exchange calendars. The file does
        not need to exist initially, but must be created before changes are made available. If the path to the file
        points to something else, e.g. a directory, it will be ignored.
        """

        # Type of change provider.
        type: Literal["file"]

        # Path to file to watch.
        file: ResolvedPath

        # Observer instance that provides the file system monitoring capability.
        _observer: Observer = None

        def setup(self, change_handler: ChangeHandler, error_handler: ErrorHandler):
            # The path to watch.
            p: Path = self.file

            # Importing watchdog here again to allow unit tests to patch the module.
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class FileChangeHandler(FileSystemEventHandler):
                """
                A handler for file changes.
                """

                # Digest of the last serialized changes.
                _digest: str = None

                def on_modified(self, event) -> None:
                    """
                    Callback when a file is modified.

                    Parameters
                    ----------
                    event : FileSystemEvent
                        The event that triggered the callback.
                    """

                    try:
                        # Ignore changes to other files and directories.
                        if Path(event.src_path).resolve() != p:
                            return

                        # Check if path exists.
                        if not p.exists():
                            log.debug(f"Path {p} does not exist. Treating as empty file.")
                            changes: ChangeSetDict = ChangeSetDict({})
                        else:
                            # Check if p0 points to file.
                            if not p.is_file():
                                raise ChangeProviderError(f"Path {p} does not point to a file.")

                            try:
                                with open(p, 'r') as f:
                                    # Load changes.
                                    changes: ChangeSetDict = ChangeSetDict(**safe_load(f))
                            except Exception as e:
                                raise ChangeProviderError(f"Error loading changes from file {p}.") from e

                        # Calculate digest of serialized changes.
                        digest0 = hashlib.sha256(changes.model_dump_json().encode('utf-8')).hexdigest()

                        if digest0 != self._digest:
                            log.debug(f"Invoking change handler with changes from file {p}.")

                            # Invoke change handler.
                            change_handler(changes)

                            # Update digest. Do this after everything else to ensure that the next round does not
                            # erroneously ignore the changes should the callback or anything else before this line fail.
                            self._digest = digest0
                    except ChangeProviderError as e:
                        # Invoke error handler.
                        error_handler(e)

            # Handler to notify about file changes.
            event_handler = FileChangeHandler()

            # Create observer and register handler.
            self._observer = Observer()
            self._observer.schedule(event_handler, path=str(self.file), recursive=True)
            self._observer.start()

            # There's a slight chance that the started observer has already notified a change to the file. This could be
            # avoided by using a lock, but that would require a more complex implementation. Instead, we simply invoke
            # the handler directly to ensure that the callback is invoked with the current state of the file.
            event_handler.on_modified(FileModifiedEvent(str(self.file)))

        def finalize(self):
            # Stop observer.
            self._observer.stop()
            self._observer.join()
except ImportError as e:
    # Required dependencies not available.
    print(e)
    pass
