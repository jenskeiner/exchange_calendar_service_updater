from unittest.mock import ANY

import pytest
import yaml
from exchange_calendars_extensions.api.changes import ChangeSetDict
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

from exchange_calendar_service.updater.provider.file import FileChangeProvider
from tests.provider.util import CHANGES, CHANGES_ALT


class TestProvider:
    """
    A collection of tests for the FileChangeProvider class.
    """
    @pytest.fixture
    def mock_observer(self, mocker):
        """
        A fixture that creates a mock Observer class.
        """
        # Create a mock Observer class.
        mock_observer_class = mocker.patch('watchdog.observers.Observer')

        # Create a mock instance.
        mock_observer_instance = mocker.Mock()

        def schedule_side_effect(event_handler: FileSystemEventHandler, *args, **kwargs):
            """
            A side effect for the schedule method that saves the event handler.
            """
            mock_observer_instance._event_handler = event_handler

        mock_observer_instance.schedule.side_effect = schedule_side_effect

        # Whenever Observer() constructor is called, the mock instance is returned.
        mock_observer_class.return_value = mock_observer_instance

        # Yield the mock instance for the tests to use if needed
        yield mock_observer_instance

        # Teardown actions can be placed here if needed

    @pytest.fixture
    def provider(self, tmp_path):
        """
        A fixture that creates an instance of FileChangeProvider with a temporary file.
        """
        file_path = tmp_path / "test.yaml"
        file_path.write_text(CHANGES)
        return FileChangeProvider(type="file", file=file_path)

    @pytest.fixture
    def setup(self, mocker, mock_observer, provider):
        """
        Fixture that sets up the provider with a mock callback and verifies the file watching setup as well as the
        initial invocation of the callback.
        """
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up provider. This should create a new observer and register a file system event handler with it.
        provider.setup(mock_change_handler, mock_error_handler)

        # Verify that watching the configured path was scheduled.
        mock_observer.schedule.assert_called_once_with(ANY, path=str(provider.file), recursive=True)

        # Verify that the observer was started.
        mock_observer.start.assert_called_once()

        # Verify that the callback was invoked with the initial changes.
        mock_change_handler.assert_called_once_with(ChangeSetDict(**yaml.safe_load(CHANGES)))

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()

        return provider, mock_change_handler, mock_error_handler

    def test_setup(self, setup):
        pass

    def test_finalize(self, mock_observer, setup):
        """
        Test that finalize stops and joins the Observer.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Finalize the provider.
        provider.finalize()

        # Verify that the observer was stopped and joined.
        mock_observer.stop.assert_called_once()
        mock_observer.join.assert_called_once()

    def test_on_modified(self, mock_observer, setup):
        """
        Test that on_modified loads the file content and invokes the callback.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Change the file content.
        with open(provider.file, 'w') as f:
            f.write(CHANGES_ALT)

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was called once with the parsed changes from the file.
        mock_change_handler.assert_called_once_with(ChangeSetDict(**yaml.safe_load(CHANGES_ALT)))

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()

    def test_on_modified_same_content(self, mock_observer, setup):
        """
        Test that on_modified does not invoke the callback if the content has not actually changed.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()

    def test_on_modified_equivalent_content(self, mock_observer, setup):
        """
        Test that on_modified does not invoke the callback if the content is equivalent to before.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Change content of file, but keep the content equivalent to the original version.
        with open(provider.file, 'w') as f:
            f.write('# Additional comment.\n')
            f.write(CHANGES)
            f.write(' ')  # Add trailing whitespace.

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()

    def test_on_modified_file_does_not_exist(self, mock_observer, setup):
        """
        Test that on_modified does invoke the callback with empty changes if the does not exist.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Delete file.
        provider.file.unlink()

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was called with empty changes.
        mock_change_handler.assert_called_once_with(ChangeSetDict({}))

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()

    def test_on_modified_not_a_file(self, mock_observer, setup):
        """
        Test that on_modified does invoke the error handler if the path points to something else than a file.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Delete file.
        provider.file.unlink()

        # Create a directory at the same path.
        provider.file.mkdir()

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was invoked.
        mock_error_handler.assert_called_once_with(ANY)

    def test_on_modified_invalid_content(self, mock_observer, setup):
        """
        Test that on_modified does invoke the error handler if the file contents cannot be parsed.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Change content of file, make the YAML invalid.
        with open(provider.file, 'w') as f:
            f.write('invalid')

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was invoked.
        mock_error_handler.assert_called_once_with(ANY)

    def test_on_modified_other_file(self, mock_observer, setup):
        """
        Test that on_modified ignores changes to other files.
        """
        # Unpack test setup.
        provider, mock_change_handler, mock_error_handler = setup

        # Reset mock change handler.
        mock_change_handler.reset_mock()

        # Filesystem event.
        event: FileModifiedEvent = FileModifiedEvent(str(provider.file.parent / (provider.file.name + "_other.yaml")))

        # Notify observer of change.
        mock_observer._event_handler.on_modified(event)

        # Assert that callback was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()
