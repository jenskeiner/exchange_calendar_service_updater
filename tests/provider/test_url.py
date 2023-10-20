from unittest.mock import ANY, call

import pytest

from exchange_calendar_service.updater.provider.url import UrlChangeProvider


class TestUrlUpdater:
    """
    A collection of tests for the UrlUpdater class.
    """

    @pytest.fixture
    def updater(self):
        """
        A fixture that creates an instance of UrlUpdater with a test URL.
        """
        return UrlChangeProvider(type="url", url="http://example.com/test.yaml", seconds=1)

    @pytest.fixture
    def mock_threading(self, mocker):
        """
        A fixture that creates a mock threading module.
        """
        mock_thread_class = mocker.patch("threading.Thread")

        # Create a mock instance.
        mock_thread_instance = mocker.Mock()

        mock_thread_class.return_value = mock_thread_instance

        mock_thread_class.side_effect = lambda target: setattr(mock_thread_instance, '_target', target) or mock_thread_instance

        # Yield the mock instance for the tests to use if needed
        yield mock_thread_class, mock_thread_instance

        # Teardown actions can be placed here if needed

    @pytest.fixture
    def mock_sleep(self, mocker):
        """
        A fixture that creates a mock sleep function.
        """
        # Create a mock sleep function to raise an InterruptedError after the configured number of invocations.
        # Otherwise, the mock method does not actually pause the current thread.

        mock_sleep = mocker.patch('time.sleep')

        def configure(loops: int):
            mock_sleep._loops = loops
            mock_sleep._i = loops

        mock_sleep.configure = configure

        def mock_sleep_side_effect(_):
            if mock_sleep._i == 0:
                raise InterruptedError()
            mock_sleep._i -= 1

        mock_sleep.side_effect = mock_sleep_side_effect

        return mock_sleep

    @pytest.fixture
    def setup(self, mocker, mock_threading, updater):
        # Unpack mock thread class and instance.
        mock_thread_class, mock_thread_instance = mock_threading

        mock_fetch_url = mocker.patch("exchange_calendar_service.updater.plugins.url.fetch_url")

        # Set up mock fetch_url to return an arbitrary but unique initial digest.
        mock_fetch_url.return_value = "digest-0"

        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up updater.
        updater.setup(mock_change_handler, mock_error_handler)

        # Assert that a new thread was created.
        mock_thread_class.assert_called_once_with(target=ANY)

        # Assert that the thread's target is a valid callable.
        assert callable(mock_thread_instance._target)

        # Assert that the thread was started.
        mock_thread_instance.start.assert_called_once()

        # Assert that fetch_url was called a single time.
        mock_fetch_url.assert_called_once_with(updater.url, mock_change_handler, mock_error_handler, None)

        # Assert that change and error handler were not actually called.
        mock_change_handler.assert_not_called()
        mock_error_handler.assert_not_called()

        return updater, mock_change_handler, mock_error_handler, mock_thread_instance, mock_fetch_url

    def test_setup(self, setup):
        """
        Test that setup calls fetch_url immediately and starts a thread.
        """
        pass

    def test_finalize(self, setup):
        """
        Test that finalize cancels the Timer.
        """
        # Unpack test setup.
        updater, mock_change_handler, mock_error_handler, mock_thread_instance, mock_fetch_url = setup

        # Reset mocks.
        mock_fetch_url.reset_mock()
        mock_change_handler.reset_mock()
        mock_error_handler.reset_mock()

        # Finalize the updater.
        updater.finalize()

        # Assert that the thread was stopped.
        mock_thread_instance.join.assert_called_once()

        # Assert that fetch_url and handlers were not called.
        mock_fetch_url.assert_not_called()
        mock_change_handler.assert_not_called()
        mock_error_handler.assert_not_called()

    def test_periodic_fetch(self, mocker, setup, mock_sleep):
        # Unpack test setup.
        updater, mock_change_handler, mock_error_handler, mock_thread_instance, mock_fetch_url = setup

        # Reset mocks.
        mock_fetch_url.reset_mock()
        mock_change_handler.reset_mock()
        mock_error_handler.reset_mock()

        # Number of loops to run.
        loops = 3

        # Set up mock fetch_url to return some arbitrary but unique digests.
        mock_fetch_url.side_effect = [f"digest-{i+1}" for i in range(loops)]

        # Configure sleep function to raise an InterruptedError after the given number of calls.
        mock_sleep.configure(loops)

        # Invoke thread's target method. This should throw InterruptedError eventually.
        with pytest.raises(InterruptedError):
            mock_thread_instance._target()

        # Assert fetch_url was called the expected number of times.
        mock_fetch_url.assert_has_calls([call(updater.url, mock_change_handler, mock_error_handler, f"digest-{i}") for i in range(loops)])

        # Assert that time.sleep was called the configured number of times with the configured number of seconds.
        mock_sleep.assert_has_calls([call(updater.seconds)] * loops)

        # Verify that handlers were not called.
        mock_change_handler.assert_not_called()
        mock_error_handler.assert_not_called()
