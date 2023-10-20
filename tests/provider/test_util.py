from unittest.mock import call

import pytest
import yaml
from exchange_calendars_extensions.api.changes import ChangeSetDict

from exchange_calendar_service.updater.api import ChangeProviderError
from exchange_calendar_service.updater.provider.util import fetch_url, digest_changes
from tests.provider.util import CHANGES, CHANGES_MODEL, CHANGES_ALT, CHANGES_ALT_MODEL

URL: str = "http://example.com/test.yaml"


class TestDigestChanges:

    def test_digest_changes_return_value(self):
        return_value = digest_changes(CHANGES_MODEL)

        assert isinstance(return_value, str)
        assert len(return_value) > 0

    def test_digest_changes_same_content(self):
        digest0 = digest_changes(CHANGES_MODEL)
        digest1 = digest_changes(CHANGES_MODEL)

        assert digest0 == digest1

    def test_digest_changes_changed_content(self):
        digest0 = digest_changes(CHANGES_MODEL)
        digest1 = digest_changes(CHANGES_ALT_MODEL)

        assert digest0 != digest1

    def test_digest_changes_equivalent_content(self):
        digest0 = digest_changes(CHANGES_MODEL)
        digest1 = digest_changes(ChangeSetDict(**yaml.safe_load('# Additional comment.\n' + CHANGES + '\n')))

        assert digest0 == digest1


class TestFetchUrl:

    @pytest.fixture
    def mock_requests_get(self, mocker):
        """
        A fixture that creates a mock requests module.
        """
        return mocker.patch("requests.get")

    def test_fetch_url_initial(self, mocker, mock_requests_get):
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up mock response to return the test changes.
        mock_response = mocker.Mock()
        mock_response.status_code = 200
        mock_response.text = CHANGES
        mock_requests_get.return_value = mock_response

        # Digest of test changes model.
        digest = digest_changes(CHANGES_MODEL)

        # Invoke fetch_url.
        return_value = fetch_url(URL, mock_change_handler, mock_error_handler, None)

        # Assert that the URL was requested.
        mock_requests_get.assert_has_calls([call(URL), call().raise_for_status()])

        # Assert that change handler was called.
        mock_change_handler.assert_called_once_with(CHANGES_MODEL)

        # Verify that the error handler was not invoked.
        mock_error_handler.assert_not_called()

        # Verify that the digest was returned.
        assert return_value == digest

    def test_fetch_url_changed_digest(self, mocker, mock_requests_get):
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up mock response to return the alt test changes.
        mock_response = mocker.Mock()
        mock_response.status_code = 200
        mock_response.text = CHANGES_ALT
        mock_requests_get.return_value = mock_response

        # Digest of fictitious previous response that had already returned test changes.
        digest = digest_changes(CHANGES_MODEL)

        # Digest of alt test changes model.
        digest_alt = digest_changes(CHANGES_ALT_MODEL)

        # Invoke fetch_url.
        return_value = fetch_url(URL, mock_change_handler, mock_error_handler, digest)

        # Assert that the URL was requested.
        mock_requests_get.assert_has_calls([call(URL), call().raise_for_status()])

        # Assert that change handler was called.
        mock_change_handler.assert_called_once_with(CHANGES_ALT_MODEL)

        # Verify that the error handler was not called.
        mock_error_handler.assert_not_called()

        # Verify that the digest was returned.
        assert return_value == digest_alt

    def test_fetch_url_same_digest(self, mocker, mock_requests_get):
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up mock response to return the test changes.
        mock_response = mocker.Mock()
        mock_response.status_code = 200
        mock_response.text = CHANGES
        mock_requests_get.return_value = mock_response

        # Digest of fictitious previous response that had already returned test changes.
        digest = digest_changes(CHANGES_MODEL)

        # Invoke fetch_url.
        return_value = fetch_url(URL, mock_change_handler, mock_error_handler, digest)

        # Assert that the URL was requested.
        mock_requests_get.assert_has_calls([call(URL), call().raise_for_status()])

        # Assert that change handler was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was not called.
        mock_error_handler.assert_not_called()

        # Verify that the digest was returned.
        assert return_value == digest

    def test_fetch_url_http_error_status(self, mocker, mock_requests_get):
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up mock response to return the test changes.
        mock_response = mocker.Mock()
        mock_response.status_code = 403
        mock_response.text = None
        mock_response.raise_for_status.side_effect = Exception("foo")
        mock_requests_get.return_value = mock_response

        # Invoke fetch_url.
        return_value = fetch_url(URL, mock_change_handler, mock_error_handler, None)

        # Assert that the URL was requested.
        mock_requests_get.assert_has_calls([call(URL), call().raise_for_status()])

        # Assert that change handler was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was called.
        mock_error_handler.assert_called_once()

        # Verify that the error handler was called with a ChangeProviderError.
        assert type(mock_error_handler.call_args_list[0][0][0]) == ChangeProviderError

        # Verify that the digest was returned.
        assert return_value is None

    def test_fetch_url_requests_get_raises_exception(self, mocker, mock_requests_get):
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up requests.get function so it raises an exception.
        mock_requests_get.side_effect = Exception("foo")

        # Invoke fetch_url.
        return_value = fetch_url(URL, mock_change_handler, mock_error_handler, None)

        # Assert that the URL was requested.
        mock_requests_get.assert_has_calls([call(URL)])

        # Assert that change handler was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was called.
        mock_error_handler.assert_called_once()

        # Verify that the error handler was called with a ChangeProviderError.
        assert type(mock_error_handler.call_args_list[0][0][0]) == ChangeProviderError

        # Verify that the digest was returned.
        assert return_value is None

    def test_fetch_url_invalid_content(self, mocker, mock_requests_get):
        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up mock response to return invalid content.
        mock_response = mocker.Mock()
        mock_response.status_code = 200
        mock_response.text = CHANGES + "\ninvalid"
        mock_requests_get.return_value = mock_response

        # Invoke fetch_url.
        return_value = fetch_url(URL, mock_change_handler, mock_error_handler, None)

        # Assert that the URL was requested.
        mock_requests_get.assert_has_calls([call(URL), call().raise_for_status()])

        # Assert that change handler was not called.
        mock_change_handler.assert_not_called()

        # Verify that the error handler was called.
        mock_error_handler.assert_called_once()

        # Verify that the error handler was called with a ChangeProviderError.
        assert type(mock_error_handler.call_args_list[0][0][0]) == ChangeProviderError

        # Verify that the digest was returned.
        assert return_value is None
