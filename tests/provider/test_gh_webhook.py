from unittest.mock import ANY

import httpx as httpx
import pytest as pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from exchange_calendar_service.updater.provider.gh_webhook import GitHubWebhookChangeProvider, GitHubWebHookContent, \
    GitHubWebHookRepository, ServerSettings, get_signature_header
from tests.provider.util import CHANGES_MODEL

SECRET: str = "secret"
BIND_HOST = '0.0.0.0'
BIND_PORT: int = 1234
PATH: str = "/webhook"
URL: str = "http://example.com/test.yaml"

GITHUB_WEBHOOK_CONTENT = GitHubWebHookContent(repository=GitHubWebHookRepository(full_name="user_name/repo_name"))


class TestGitHubWebHookProvider:
    """
    A collection of tests for the GitHubWebHookUpdater class.
    """

    @pytest.fixture
    def updater(self):
        """
        A fixture that creates an instance of UrlUpdater with a test URL.
        """
        return GitHubWebhookChangeProvider(type="gh_webhook", server=ServerSettings(bind_host=BIND_HOST,
                                                                                    bind_port=BIND_PORT, path=PATH),
                                           secret=SECRET, url=URL)

    @pytest.fixture
    def mock_uvicorn_run(self, mocker):
        """
        A fixture that creates a mock uvicorn.run function.
        """
        return mocker.patch("uvicorn.run")

    @pytest.fixture
    def setup(self, mocker, mock_uvicorn_run, updater):
        """
        Test that setup sets up the FastAPI app, starts the webserver, and fetches the URL.
        """
        mock_fetch_url = mocker.patch("exchange_calendar_service.updater.provider.gh_webhook.fetch_url")

        # Set up mock fetch_url to return an arbitrary but unique initial digest.
        mock_fetch_url.return_value = "digest-0"

        # Create a mock change handler.
        mock_change_handler = mocker.Mock()

        # Create a mock error handler.
        mock_error_handler = mocker.Mock()

        # Set up updater.
        updater.setup(mock_change_handler, mock_error_handler)

        # Assert that the app was created.
        assert isinstance(updater._app, FastAPI)

        # Assert that the webserver was started.
        mock_uvicorn_run.assert_called_once_with(updater._app, host=updater.server.bind_host, port=updater.server.bind_port)

        # Assert that fetch_url was called a single time.
        mock_fetch_url.assert_called_once_with(updater.url, mock_change_handler, mock_error_handler, None)

        # Assert that change and error handler were not actually called.
        mock_change_handler.assert_not_called()
        mock_error_handler.assert_not_called()

        # Reset mocks.
        mock_fetch_url.reset_mock()
        mock_change_handler.reset_mock()
        mock_error_handler.reset_mock()

        return updater, mock_change_handler, mock_error_handler, mock_fetch_url

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
        updater, mock_change_handler, mock_error_handler, mock_fetch_url = setup

        # Finalize the updater.
        updater.finalize()

        # Assert that fetch_url and handlers were not called.
        mock_fetch_url.assert_not_called()
        mock_change_handler.assert_not_called()
        mock_error_handler.assert_not_called()

    @pytest.mark.parametrize("secret", [None, "with secret"])
    def test_on_modified(self, setup, secret):
        # Unpack test setup.
        updater, mock_change_handler, mock_error_handler, mock_fetch_url = setup

        # Mock fetch URL should call the change handler with the test changes when invoked.
        mock_fetch_url.side_effect = lambda url, change_handler, error_handler, digest: change_handler(CHANGES_MODEL)

        # Create test HTTP client.
        client = TestClient(updater._app)

        # Inject secret into provider.
        updater.secret = secret

        # Create test request body.
        body: str = GITHUB_WEBHOOK_CONTENT.model_dump_json()

        # Create test headers.
        headers = {}

        if secret is not None:
            headers["x-hub-signature-256"] = get_signature_header(body=body.encode('utf-8'), secret=secret)

        response: httpx.Response = client.post(updater.server.path, data=body, headers=headers)

        # Assert that the response is 200.
        assert response.status_code == 200

        # Assert that the URL was requested.
        mock_fetch_url.assert_called_once_with(updater.url, mock_change_handler, mock_error_handler, 'digest-0')

        # Assert that error handler was not called.
        mock_error_handler.assert_not_called()

        # Change handler should have been called
        mock_change_handler.assert_called_once_with(CHANGES_MODEL)

    def test_on_modified_missing_signature_header(self, setup):
        # Unpack test setup.
        updater, mock_change_handler, mock_error_handler, mock_fetch_url = setup

        # Mock fetch URL should call the change handler with the test changes when invoked.
        mock_fetch_url.side_effect = lambda url, change_handler, error_handler, digest: change_handler(CHANGES_MODEL)

        # Create test HTTP client.
        client = TestClient(updater._app)

        # Inject secret into provider.
        updater.secret = "secret"

        # Create test request body.
        body: str = GITHUB_WEBHOOK_CONTENT.model_dump_json()

        # Send request w/o signature header.
        response: httpx.Response = client.post(updater.server.path, data=body)

        # Assert that the response is 403.
        assert response.status_code == 403

        # Assert that URL was not requested.
        mock_fetch_url.assert_not_called()

        # Change handler should not have been called.
        mock_change_handler.assert_not_called()

        # Assert that error handler was called.
        mock_error_handler.assert_called_once_with(ANY)

    def test_on_modified_invalid_signature_header(self, setup):
        # Unpack test setup.
        updater, mock_change_handler, mock_error_handler, mock_fetch_url = setup

        # Mock fetch URL should call the change handler with the test changes when invoked.
        mock_fetch_url.side_effect = lambda url, change_handler, error_handler, digest: change_handler(CHANGES_MODEL)

        # Create test HTTP client.
        client = TestClient(updater._app)

        # Inject secret into provider.
        updater.secret = "secret"

        # Create test request body.
        body: str = GITHUB_WEBHOOK_CONTENT.model_dump_json()

        # Create test headers.
        headers = {"x-hub-signature-256": get_signature_header(body=body.encode('utf-8'), secret="foo")}

        # Send request w/o signature header.
        response: httpx.Response = client.post(updater.server.path, data=body, headers=headers)

        # Assert that the response is 403.
        assert response.status_code == 403

        # Assert that URL was not requested.
        mock_fetch_url.assert_not_called()

        # Change handler should not have been called.
        mock_change_handler.assert_not_called()

        # Assert that error handler was called.
        mock_error_handler.assert_called_once_with(ANY)
