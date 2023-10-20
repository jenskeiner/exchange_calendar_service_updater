import itertools
import threading
import time
from unittest.mock import ANY, call, MagicMock

import pytest
import pytest_mock
from exchange_calendars_extensions.api.changes import ChangeSetDict
from pydantic import AnyHttpUrl
from pydantic_core import ValidationError
from typing_extensions import Literal, Callable, Union, Tuple

import exchange_calendar_service.updater.__main__ as main
from exchange_calendar_service.updater.api import BaseChangeProvider, ChangeHandler, ErrorHandler, BaseRetryPolicy, \
    RetryInfo, RetryPolicyResult
from exchange_calendar_service.updater.settings import AppSettings, APIKey
from tests.provider.util import CHANGES_MODEL, CHANGES_ALT_MODEL


class TestOnChanges:

    @pytest.fixture()
    def mock_condition_and_incoming(self, mocker):
        """
        A fixture that creates a mock threading.Condition, and a mock getter/setter for a ChangeSetDict object.
        """
        # Create mock condition and incoming changes holder.
        mock_condition, mock_incoming = mocker.MagicMock(spec=main.Condition), mocker.Mock(spec=main.IncomingChanges)

        # Create a mock changes property on the holder.
        mock_incoming_changes = mocker.PropertyMock()
        type(mock_incoming).changes = mock_incoming_changes

        yield mock_condition, mock_incoming, mock_incoming_changes

        # Teardown actions can be placed here if needed

    def test_on_changes0_unchanged(self, mock_condition_and_incoming):
        """
        Test that the _on_changes function does not notify the condition when the incoming changes are None.
        """
        # Extract mock condition and holder.
        mock_condition, mock_incoming, mock_incoming_changes = mock_condition_and_incoming

        # Set the getter to return CHANGES_MODEL.
        mock_incoming_changes.return_value = CHANGES_MODEL

        # Call the _on_changes function with CHANGES_MODEL.
        main._on_changes0(CHANGES_MODEL, mock_condition, mock_incoming)

        # Verify that only getter was called once.
        mock_incoming_changes.assert_called_once()

        # Verify that the condition was not notified.
        mock_condition.notify.assert_not_called()

    def test_on_changes0_changed(self, mock_condition_and_incoming):
        """
        Test that the _on_changes function does not notify the condition when the incoming changes are None.
        """
        # Extract mock condition and holder.
        mock_condition, mock_incoming, mock_incoming_changes = mock_condition_and_incoming

        # Set the getter to return CHANGES_MODEL.
        mock_incoming_changes.return_value = CHANGES_MODEL

        # Call the _on_changes function with CHANGES_ALT_MODEL.
        main._on_changes0(CHANGES_ALT_MODEL, mock_condition, mock_incoming)

        # Verify that getter was called once, and that setter was called once with the new changes.
        assert mock_incoming_changes.call_args_list == [call(), call(CHANGES_ALT_MODEL)]

        # Verify that the setter was called with a copy of CHANGES_ALT_MODEL.
        args, kwargs = mock_incoming_changes.call_args_list[1]
        assert args[0] == CHANGES_ALT_MODEL
        assert id(args[0]) != id(CHANGES_ALT_MODEL)

        # Verify that the condition was notified.
        mock_condition.notify.assert_called_once()

    def test_on_changes(self, mocker):

        mock_on_changes0 = mocker.patch('exchange_calendar_service.updater.__main__._on_changes0')

        # Should not be able to pass in None.
        with pytest.raises(ValidationError):
            main._on_changes(None)

        # Should not call _on_changes0.
        mock_on_changes0.assert_not_called()

        # Should not be able to pass in other types, such as str.
        with pytest.raises(ValidationError):
            # noinspection PyTypeChecker
            main._on_changes("foo")

        # Should not call _on_changes0.
        mock_on_changes0.assert_not_called()

        # Should accept ChangeSetDict
        changes: ChangeSetDict = ChangeSetDict(root={})
        main._on_changes(changes)

        # Should call _on_changes0.
        mock_on_changes0.assert_called_once_with(changes, main._condition, main._incoming)


class TestNotify:

    URL = "http://host.test/v1/update"
    API_KEY = "test-api-key"

    @pytest.fixture()
    def mock_requests_post(self, mocker):
        """
        A fixture that creates a mock requests.post function.
        """
        return mocker.patch("requests.post")

    def test_call_successful(self, mock_requests_post):
        """
        Test that the notify function calls requests.post with the correct arguments.
        """
        mock_requests_post.return_value.status_code = 200

        # Call the notify function with the test URL and API key.
        main._notify(self.URL, self.API_KEY, CHANGES_MODEL)

        # Verify that requests.post was called with the correct arguments.
        mock_requests_post.assert_called_once_with(
            url=self.URL,
            json=CHANGES_MODEL.model_dump_json(),
            headers={'Authorization': f'Bearer {self.API_KEY}'},
            timeout=ANY,
        )

    def test_call_error(self, mocker, mock_requests_post):
        """
        Test that the notify function raises an exception when requests.post returns an error.
        """
        mock_response = mocker.Mock()
        mock_response.status_code = 403
        mock_response.text = None
        mock_response.raise_for_status.side_effect = Exception("foo")
        mock_requests_post.return_value = mock_response

        # Call the notify function with the test URL and API key.
        with pytest.raises(Exception):
            main._notify(self.URL, self.API_KEY, CHANGES_MODEL)

        # Verify that requests.post was called with the correct arguments.
        mock_requests_post.assert_called_once_with(
            url=self.URL,
            json=CHANGES_MODEL.model_dump_json(),
            headers={'Authorization': f'Bearer {self.API_KEY}'},
            timeout=ANY,
        )


class DummyProvider(BaseChangeProvider):
    type: Literal["dummy"] = "dummy"

    def setup(self, change_handler: ChangeHandler, error_handler: ErrorHandler) -> None:
        pass


class DummyRetryPolicy(BaseRetryPolicy):
    type: Literal["dummy"] = "dummy"


class TimeoutReached(Exception):
    pass


def check_condition_with_timeout(condition: Callable[[], bool], timeout: float):
    """
    Check the condition repeatedly until it returns True, or until the timeout is reached.
    """
    start_time = time.time()
    while not condition():
        time.sleep(0.1)
        if time.time() - start_time > timeout:
            raise TimeoutReached()


class TestMainLoop:

    @staticmethod
    def get_provider_target(condition: threading.Condition, incoming: main.IncomingChanges, changes: Union[ChangeSetDict, main.Sentinel]) -> Callable[[], None]:
        """Returns a function that will notify the given condition with the given changes and return.

        The function can be used as a target for a thread to notify the main loop of changes.
        """

        def fn() -> None:
            # Acquire condition.
            with condition:
                # Set incoming changes in holder.
                incoming.changes = changes

                # Notify condition.
                condition.notify()

        return fn

    # Type alias for the context tuple.
    Context = Tuple[threading.Condition, main.IncomingChanges, AppSettings]

    @pytest.fixture
    def context(self, mocker) -> Context:
        """Returns a tuple with a threading.Condition, an IncomingChanges object, and an AppSettings object.

        The settings object contains a mock provider and retry_policy so the desired behaviour can be tested without
        having to set up a real provider and retry policy.
        """
        # Use a regular condition since we actually need to notify different threads of it.
        condition: threading.Condition = threading.Condition()

        # Spy on the condition's wait_for method so we can verify that it is called.
        condition_wait_for_spy = mocker.spy(condition, 'wait_for')

        # Use the IncomingChanges class from the main module.
        incoming: main.IncomingChanges = main.IncomingChanges()

        # Use a mock provider and retry policy.
        provider: MagicMock = MagicMock()
        retry_policy: MagicMock = MagicMock()

        # Construct a settings object, but skip validation because of the mocks used.
        settings: AppSettings = AppSettings.model_construct(provider=provider, url="http://host.test/v1/update",
                                                            api_key="test-api-key", retry_policy=retry_policy)

        return condition, condition_wait_for_spy, incoming, settings

    @pytest.fixture
    def mock_notify(self, mocker) -> MagicMock:
        """Patches the main module's _notify function and returns the corresponding mock."""
        return mocker.patch("exchange_calendar_service.updater.__main__._notify")

    def test_exits_on_sentinel_immediately(self, mock_notify: MagicMock, context: Context):
        """Test main loop exits when notified of 'sentinel' immediately when starting."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        # Create and start a thread that will notify the condition.
        t1 = threading.Thread(target=self.get_provider_target(condition, incoming, 'sentinel'), daemon=True)
        t1.start()

        # Call main loop in another thread.
        t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
        t2.start()

        # Wait for a short amount of time to allow the main loop to exit.
        time.sleep(0.1)

        # Assert t1 has finished.
        assert not t1.is_alive()

        # Assert t2 has finished.
        assert not t2.is_alive()

    def test_exits_on_sentinel_after_delay(self, mock_notify: MagicMock, context: Context):
        """Test main loop exits when notified of 'sentinel' after a delay."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        # Call main loop in another thread.
        t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
        t2.start()

        time.sleep(0.1)

        # Create and start a thread that will notify the condition.
        t1 = threading.Thread(target=self.get_provider_target(condition, incoming, 'sentinel'), daemon=True)
        t1.start()

        # Wait for a short amount of time to allow the main loop to exit.
        time.sleep(0.1)

        # Assert t1 has finished.
        assert not t1.is_alive()

        # Assert t2 has finished.
        assert not t2.is_alive()

    def test_changes_available_immediately(self, mock_notify: MagicMock, context: Context):
        """Test main loop when incoming changes are available immediately after start."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        try:
            # Set up mock notify function to return normally.
            mock_notify.return_value = CHANGES_MODEL

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # Wait until condition has been waited on once, or throw an exception if not. When the main loop starts, it
            # should immediately process the incoming changes. When target has been notified successfully, it should
            # wait on the condition because no new incoming changes are immediately available.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 1, timeout=1.0)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on twice.
            assert condition_wait_for_spy.call_count == 1

            # Assert notify was called once with the expected arguments.
            mock_notify.assert_called_once_with(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    def test_changes_available_after_delay(self, mock_notify: MagicMock, context: Context):
        """Test main loop when first incoming changes become available after some delay."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        try:
            # Set up mock notify function to return normally.
            mock_notify.return_value = CHANGES_MODEL

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # Wait for a short amount of time to allow the main loop to start and wait on the condition.
            time.sleep(0.1)

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Wait until condition has been waited on twice, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 2, timeout=1.0)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on twice.
            assert condition_wait_for_spy.call_count == 2

            # Assert notify was called once with the expected arguments.
            mock_notify.assert_called_once_with(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    def test_incoming_changes_none(self, mock_notify, context):
        """Test main loop with incoming changes set to None.

        Notifying the main loop of None changes violates the provider contract but we still want to test that the main
        loop handles this case gracefully by ignoring.
        """

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        try:
            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, None), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # The condition should only be waited on once since the main loop does so initially. When we notify the
            # condition from the other thread, it should have no effect since the incoming changes are None. We verify
            # this here, by waiting a short time and then asserting that the condition was only waited on once.
            with pytest.raises(TimeoutReached):
                check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count >= 2, timeout=0.5)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on once.
            assert condition_wait_for_spy.call_count == 1

            # Assert notify was not called since the incoming changes were None.
            mock_notify.assert_not_called()

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    def test_same_changes_sequentially(self, mock_notify, context):
        """Test main loop with two notifications of the same changes.

        In this scenario, the main loop first successfully notifies the target of a first set of changes. When done, the
        main loop is notified of the same changes again. The main loop should ignore the second notification and not
        notify the target again."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        try:
            # Set up mock notify function to return normally.
            mock_notify.return_value = CHANGES_MODEL

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # Wait until condition has been waited on once, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 1, timeout=1.0)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on once.
            assert condition_wait_for_spy.call_count == 1

            # Assert notify was called once with the expected arguments.
            mock_notify.assert_called_once_with(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)#

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None

            # Until here, this scenario is identical to the test_changes_available_immediately.

            # Create and start another thread that will notify the condition again with the same changes as before.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Wait until condition has been waited on twice, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 2, timeout=1.0)

            # Assert t3 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on twice.
            assert condition_wait_for_spy.call_count == 2

            # Assert notify was called once with the expected arguments. Second call should not have happened since the
            # passed in changes were the same as the first time.
            mock_notify.assert_called_once_with(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)

            # Assert retry policy was not called.

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    def test_different_changes_sequentially(self, mocker, mock_notify, context):
        """Test main loop with two notifications, each with different changes.

        In this scenario, the main loop first successfully notifies the target of a first set of changes. When done, the
        main loop is notified of a different set of changes. The main loop should notify the target again with the new
        changes."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        try:
            # Set up mock notify function to return normally.
            mock_notify.return_value = CHANGES_MODEL

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # Wait until condition has been waited on once, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 1, timeout=1.0)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on once.
            assert condition_wait_for_spy.call_count == 1

            # Assert notify was called once with the expected arguments.
            mock_notify.assert_called_once_with(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None

            # Set up mock notify function to return normally, now with the new changes.
            mock_notify.return_value = CHANGES_ALT_MODEL

            # Create and start another thread that will notify the condition of the new changes.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_ALT_MODEL), daemon=True)
            t1.start()

            # Wait until condition has been waited on twice, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 2, timeout=1.0)

            # Assert t3 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on twice.
            assert condition_wait_for_spy.call_count == 2

            # Assert notify was now called twice in total with the expected arguments.
            mock_notify.assert_has_calls([
                mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL),
                mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_ALT_MODEL),
            ])

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    @pytest.mark.parametrize("n_retries, delay", tuple(itertools.product([0, 1, 5, 10], [0, 0.1])))
    def test_retry(self, mocker, mock_notify, context, n_retries: int, delay: Union[float | int]):
        """Test main loop with a failing notification that succeeds after a number of retries.

        This test simulates a scenario where notifying the target fails a couple of times before it succeeds. While the
        notification is failing, no new changes come in and the notification loop eventually completes."""

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        # Exception thrown by notify function when failing.
        notify_exception = Exception("foo")

        try:

            def notify_side_effect():
                """Generator that yields the notify_exception n_retries times, and then CHANGES_MODEL once."""
                count = 0

                while count < n_retries:
                    count += 1
                    yield notify_exception

                yield CHANGES_MODEL

            # Set up mock notify function.
            mock_notify.side_effect = tuple(notify_side_effect())

            def retry_policy_side_effect(info):
                """Simulates a retry policy that always retries with the given delay."""
                # Always retry with given delay.
                return True, delay

            # Set up mock retry policy.
            settings.retry_policy.side_effect = retry_policy_side_effect

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # Wait until condition has been waited on once, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 1, timeout=1.0 + n_retries * delay)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on once.
            assert condition_wait_for_spy.call_count == 1

            # Assert notify was called n_retries + 1 times with the expected arguments.
            assert mock_notify.call_count == n_retries + 1
            mock_notify.assert_has_calls([
                mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL),
            ] * (n_retries + 1))

            # Verify expected number of calls to the retry policy.
            assert settings.retry_policy.call_count == n_retries

            # Verify each call to retry policy.
            for i in range(n_retries):
                # Verify that the retry strategy was called with the expected RetryInfo.
                args, kwargs = settings.retry_policy.call_args_list[i]
                assert type(args[0]) == RetryInfo
                # Verifying the number of fails at the time each call has happened would require a new RetryInfo object
                # for each call. We assume that the loop re-uses the same object, so the number of fails here must be
                # the total number of re-tries.
                assert args[0].fails == n_retries
                # Exception should always be the same.
                assert args[0].exception == notify_exception
                # Since should be a float.
                assert type(args[0].since) == float

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    @pytest.mark.parametrize("n_retries, delay, i_new_changes", tuple(itertools.product([0, 1, 5, 10], [0, 0.1], [0, 1, 5])))
    def test_retry_strategy_intermittent_new_changes(self, mocker: pytest_mock.MockerFixture, mock_notify: MagicMock,
                                                     context: Context, n_retries: int, delay: Union[float | int],
                                                     i_new_changes: int):
        """Test main loop with intermittent new changes coming in while target has not yet been successfully notified.

        This test simulates a scenario where new changes come in while the main loop is still trying to notify the
        target. The common case is that notifying the target fails a couple of times before it succeeds, and during
        that time, new changes come in at some point. The main loop should then just continue with the new changes until
        the target has eventually been notified successfully.

        As an edge case, new changes may come in just while a target notification attempt is ongoing that ultimately
        succeeds. In this case, the notification loop should re-enter and immediately notify the target again of the now
        updated changes.

        Eventually, the notification loop should complete successfully when the target has been notified of the new
        changes.

        The test is parametrized with the number of retries (equivalent to the number of failed notification attempts
        before success), the delay between retries, and number of notification attempts before new changes come in.

        Note that for some parameter combinations, the number of retries may be lower than the number of notification
        attempts before new changes come in. In this case, the notification loop will already have completed and the
        behaviour with respect to the new changes is not tested.

        Parameters
        ----------
        mocker : pytest_mock.MockFixture
            The pytest mock fixture.
        mock_notify : MagicMock
            The mock notify function. Used to block the main loop for a while so we can simulate new changes coming in.
        context : tuple
            The context fixture.
        """
        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        # Event used to signal from this thread to the thread running the notification function when to continue.
        event_continue: threading.Event = threading.Event()

        # Event used to signal this thread when notification function has blocked and is waiting for event_continue.
        event_blocked: threading.Event = threading.Event()

        # Exception thrown by notification function when failing.
        notify_exception = Exception("foo")

        try:
            def notify_side_effect() -> Callable[[str, str, ChangeSetDict], ChangeSetDict]:
                """Returns a function that will raise notify_exception on the first n_retries calls, and then return
                the given changes on calls after.


                Also, after i_new_changes previous calls, the returned function notifies event_blocked and then waits on
                event_continue, This effectively blocks the thread running the function until event_continue is
                notified.

                Note that i_new_changes may be larger than n_retries + 1 (the first call that will return normally), so
                the function may never actually block and wait.
                """
                # Number of times the function has been called previously.
                count = 0

                def wait_maybe():
                    if count == i_new_changes:
                        # Block and wait for event to continue. This allows us to simulate a new change set being
                        # available while this function is running.
                        event_blocked.set()
                        event_continue.wait()

                def fn(url: AnyHttpUrl, api_key: APIKey, changes: ChangeSetDict) -> ChangeSetDict:
                    nonlocal count

                    while count < n_retries:
                        # Should throw exception.

                        # Block and wait, maybe.
                        wait_maybe()

                        count += 1
                        raise notify_exception

                    # If we get here, should return normally.

                    # BLock and wait, maybe.
                    wait_maybe()

                    count += 1
                    return changes

                return fn

            # Set up mock notification function.
            mock_notify.side_effect = notify_side_effect()

            def retry_policy_side_effect(info):
                # Always retry with given delay.
                return True, delay

            # Set up mock retry policy.
            settings.retry_policy.side_effect = retry_policy_side_effect

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            if i_new_changes <= n_retries:
                # There will be new incoming changes before the notification completes successfully for the first time.
                # We know that the notification function blocks and waits on the i_new_changes + 1 call.

                # Wait for the notification function to block and wait.
                event_blocked.wait(timeout=1.0 + i_new_changes * delay)

                # Wait for a short period of time to allow the notification function to wait on event_continue itself.
                time.sleep(0.1)

                # Assert t1 has finished.
                assert not t1.is_alive()

                # Assert t2 is still alive.
                assert t2.is_alive()

                # Assert that the condition was waited on zero times so far.
                assert condition_wait_for_spy.call_count == 0

                # Assert notify was called i_new_changes + 1 times with the expected arguments.
                assert mock_notify.call_count == i_new_changes + 1
                mock_notify.assert_has_calls([
                    mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL),
                ] * (i_new_changes + 1))

                # Verify expected number of calls to the retry strategy.
                assert settings.retry_policy.call_count == i_new_changes

                # Verify each call individually.
                for i in range(i_new_changes):
                    # Verify that the retry strategy was called with the expected RetryInfo.
                    args, kwargs = settings.retry_policy.call_args_list[i]
                    assert len(args) == 1
                    assert len(kwargs) == 0
                    assert type(args[0]) == RetryInfo
                    # Verifying the number of fails at the time each call has happened would require a new RetryInfo
                    # object for each call. We assume that the loop re-uses the same object, so the number of fails here
                    # must be the total number of re-tries.
                    assert args[0].fails == i_new_changes
                    assert args[0].exception == notify_exception
                    assert type(args[0].since) == float

                # Assert incoming changes holder is re-set.
                assert incoming.changes is None

                # Create and start a thread that will notify the condition with new changes.
                t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_ALT_MODEL),
                                      daemon=True)
                t1.start()

                # Notify the event that lets the notification function unblock and continue.
                event_continue.set()

            # Wait until condition has been waited on once, or throw an exception if not. When the condition has been
            # waited on once, this indicates that the notification loop has completed.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 1,
                                         timeout=1.0 + n_retries * delay)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on once.
            assert condition_wait_for_spy.call_count == 1

            # Assert the notification function was called expected number of times with the expected arguments. There
            # are three cases to cover:
            #
            # 1) i_new_changes > n_retries: The notification function should not have blocked and waited, and should
            #    have been called with CHANGES_MODEL n_retries + 1 times in total.
            #
            # 2) i_new_changes == n_retries: The notification function should have blocked and waited on the
            #    i_new_changes + 1 call which is also the first it will have returned from normally. Since there are
            #    now new changes after the call has returned, the notification function should have been called one more
            #    time with CHANGES_ALT_MODEL.
            #
            # 3) i_new_changes < n_retries: The notification function should have blocked and waited on the
            #    i_new_changes + 1 call during which new changes should have come in. So there should be
            #    i_new_changes + 1 calls with CHANGES_MODEL, and n_retries - i_new_changes calls with CHANGES_ALT_MODEL.
            expected_calls = [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)] * (min(i_new_changes, n_retries) + 1) \
                             + [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_ALT_MODEL)] * (max(0, n_retries - i_new_changes) + (1 if i_new_changes == n_retries else 0))
            assert mock_notify.call_count == len(expected_calls)
            mock_notify.assert_has_calls(expected_calls)

            # Verify expected number of calls to the retry strategy.
            assert settings.retry_policy.call_count == n_retries

            # Verify each call individually.
            for i in range(n_retries):
                # Verify that the retry strategy was called with the expected RetryInfo.
                args, kwargs = settings.retry_policy.call_args_list[i]
                assert len(args) == 1
                assert len(kwargs) == 0
                assert type(args[0]) == RetryInfo
                # Verifying the number of fails at the time each call has happened would require a new RetryInfo object
                # for each call. We assume that the loop re-uses the same object while re-trying, so the number of fails
                # must be the total number of re-tries that have eventually happened while in the loop.
                if i_new_changes < n_retries:
                    # The first i_new_changes + 1 calls should have failed, so number of fails should be that number.
                    # After, the new incoming changes should have re-set the RetryInfo, so for the remaining
                    # (n_retries - i_new_changes - 1) calls, the number of fails should be just that.
                    assert args[0].fails == (i_new_changes + 1 if i <= i_new_changes else n_retries - i_new_changes - 1)
                else:
                    # New changes come in earliest when the notification function is already in its first call that
                    # returns normally. So all n_retries calls to the retry policy have happened before.
                    assert args[0].fails == n_retries
                assert args[0].exception == notify_exception
                assert type(args[0].since) == float

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # Unblock notification thread, maybe.
            event_continue.set()

            # start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    @pytest.mark.parametrize("n_retries, delay, i_new_changes", tuple(itertools.product([0, 1, 5, 10], [0, 0.1], [0, 1, 5])))
    def test_retry_strategy_intermittent_previous_changes(self, mocker: pytest_mock.MockerFixture, mock_notify: MagicMock,
                                                          context: Context, n_retries: int, delay: Union[float | int],
                                                          i_new_changes: int):
        """Test main loop with intermittent old changes coming in while target has not yet been successfully notified.

        This test simulates a scenario where changes that correspond to the last know successfully applied state come in
        while the main loop is still trying to notify the target. The common case is that notifying the target fails a
        couple of times before it succeeds, and during that time, changes that have already been applied before come in
        at some point. The main loop should then abandon any further attempts.

        As an edge case, the old changes may come in just while a target notification attempt is ongoing that ultimately
        succeeds. In this case, the notification loop should re-enter and immediately notify the target again of the old
        changes since they now no longer represent the last known successfully applied state.

        The test is parametrized with the number of retries (equivalent to the number of failed notification attempts
        before success), the delay between retries, and number of notification attempts before new changes come in.

        Note that for some parameter combinations, the number of retries may be lower than the number of notification
        attempts before new changes come in. In this case, the notification loop will already have completed and the
        behaviour with respect to the old changes is not tested.

        Parameters
        ----------
        mocker : pytest_mock.MockFixture
            The pytest mock fixture.
        mock_notify : MagicMock
            The mock notify function. Used to block the main loop for a while so we can simulate new changes coming in.
        context : tuple
            The context fixture.
        """
        # Event used to signal from this thread to the thread running the notification function when to continue.
        event_continue: threading.Event = threading.Event()

        # Event used to signal this thread when notification function has blocked and is waiting for event_continue.
        event_blocked: threading.Event = threading.Event()

        # Exception thrown by notification function when failing.
        notify_exception = Exception("foo")

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        try:
            # First, let the main loop notify the target of CHANGES_MODEL successfully.

            # Set up mock notify function to return normally.
            mock_notify.side_effect = lambda url, api_key, changes: changes

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
            t1.start()

            # Call main loop in another thread.
            t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
            t2.start()

            # Wait until condition has been waited on once, or throw an exception if not.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 1, timeout=1.0)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on once.
            assert condition_wait_for_spy.call_count == 1

            # Assert notify was called once with the expected arguments.
            if len(mock_notify.call_args_list) > 1:
                print(mock_notify.call_args_list)
            mock_notify.assert_called_once_with(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)

            # Assert retry policy was not called.
            settings.retry_policy.assert_not_called()

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None

            # Now, simulate the case where the main loop tries to notify the target of CHANGES_ALT_MODEL while
            # CHANGES_MODEL comes in.

            def notify_side_effect() -> Callable[[str, str, ChangeSetDict], ChangeSetDict]:
                """Returns a function that will raise notify_exception on the first n_retries calls, and then return
                the given changes on calls after.


                Also, after i_new_changes previous calls, the returned function notifies event_blocked and then waits on
                event_continue, This effectively blocks the thread running the function until event_continue is
                notified.

                Note that i_new_changes may be larger than n_retries + 1 (the first call that will return normally), so
                the function may never actually block and wait.
                """
                # Number of times the function has been called previously.
                count = 0

                def wait_maybe():
                    if count == i_new_changes:
                        # Block and wait for event to continue. This allows us to simulate a new change set being
                        # available while this function is running.
                        event_blocked.set()
                        event_continue.wait()

                def fn(url: AnyHttpUrl, api_key: APIKey, changes: ChangeSetDict) -> ChangeSetDict:
                    nonlocal count

                    while count < n_retries:
                        # Should throw exception.

                        # Block and wait, maybe.
                        wait_maybe()

                        count += 1
                        raise notify_exception

                    # If we get here, should return normally.

                    # BLock and wait, maybe.
                    wait_maybe()

                    count += 1
                    return changes

                return fn

            # Set up mock notification function.
            mock_notify.side_effect = notify_side_effect()

            def retry_policy_side_effect(info):
                # Always retry with given delay.
                return True, delay

            # Set up mock retry policy.
            settings.retry_policy.side_effect = retry_policy_side_effect

            # Create and start a thread that will notify the condition.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_ALT_MODEL), daemon=True)
            t1.start()

            if i_new_changes <= n_retries:
                # There will be incoming changes before the notification completes successfully for the first time. We
                # know that the notification function blocks and waits on the i_new_changes + 1 call.

                # Wait for the notification function to block and wait.
                event_blocked.wait(timeout=1.0 + i_new_changes * delay)

                # Wait for a short period of time to allow the notification function to wait on event_continue itself.
                time.sleep(0.1)

                # Assert t1 has finished.
                assert not t1.is_alive()

                # Assert t2 is still alive.
                assert t2.is_alive()

                # Assert that the condition was waited on once. This is the wait before the current main loop iteration
                # was entered.
                assert condition_wait_for_spy.call_count == 1

                # Assert notify was called i_new_changes + 2 times with the expected arguments. The first call is the
                # initial successful call to notify the target of CHANGES_MODEL, the remaining calls are from trying to
                # notify the target of CHANGES_ALT_MODEL.
                assert mock_notify.call_count == i_new_changes + 2
                mock_notify.assert_has_calls(
                    [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)] +
                    [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_ALT_MODEL)] * (i_new_changes + 1)
                )

                # Verify expected number of calls to the retry strategy.
                assert settings.retry_policy.call_count == i_new_changes

                # Verify each call individually.
                for i in range(i_new_changes):
                    # Verify that the retry strategy was called with the expected RetryInfo.
                    args, kwargs = settings.retry_policy.call_args_list[i]
                    assert len(args) == 1
                    assert len(kwargs) == 0
                    assert type(args[0]) == RetryInfo
                    # Verifying the number of fails at the time each call has happened would require a new RetryInfo
                    # object for each call. We assume that the loop re-uses the same object, so the number of fails here
                    # must be the total number of re-tries.
                    assert args[0].fails == i_new_changes
                    assert args[0].exception == notify_exception
                    assert type(args[0].since) == float

                # Assert incoming changes holder is re-set.
                assert incoming.changes is None

                # Create and start a thread that will notify the condition with new changes.
                t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
                t1.start()

                # Notify the event that lets the notification function unblock and continue.
                event_continue.set()

            # Wait until condition has been waited on twice, or throw an exception if not. When the condition has
            # been waited on twice, this indicates that the notification loop has completed.
            check_condition_with_timeout(condition=lambda: condition_wait_for_spy.call_count == 2,
                                         timeout=1.0 + n_retries * delay)

            # Assert t1 has finished.
            assert not t1.is_alive()

            # Assert t2 is still alive.
            assert t2.is_alive()

            # Assert that the condition was waited on twice.
            assert condition_wait_for_spy.call_count == 2

            # Assert the notification function was called the expected number of times with the expected arguments.
            # There are three cases to cover. In all three cases, the first call is the initial successful call to
            # notify the target of CHANGES_MODEL.
            #
            # 1) i_new_changes > n_retries: After the initial call, the notification function should not have blocked
            #    and waited, and should have been called with CHANGES_ALT_MODEL n_retries + 1 times until it succeeded.
            #
            # 2) i_new_changes == n_retries: After the initial call, the notification function should have been called
            #    i_new_changes + 1 times with CHANGES_ALT_MODEL. On the last of these calls, it should have blocked and
            #    waited. This call is also the first one from which it should have returned normally. When it returns,
            #    it will have notified the target successfully of CHANGES_ALT_MODEL. Since the main loop has now been
            #    notified of CHANGES_MODEL as the new incoming changes, the notification function should have been
            #    called one more time, now with CHANGES_MODEL.
            #
            # 3) i_new_changes < n_retries: After the initial call, the notification function should have been called
            #    i_new_changes + 1 times with CHANGES_ALT_MODEL. On the last of these calls, it should have blocked and
            #    waited. Eventually, it should still return from the call by raising an exception. When it returns, the
            #    main loop has now been notified of CHANGES_MODEL as the new incoming changes. Since the notification
            #    function has not yet completed normally, the loop should continue until it succeeds, but now with
            #    CHANGES_MODEL. There should be n_retries - i_new_changes calls with CHANGES_MODEL before the
            #    notification finally succeeds.
            expected_calls = [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)] \
                             + [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_ALT_MODEL)] * (min(i_new_changes, n_retries) + 1) \
                             + [mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL)] * (max(0, n_retries - i_new_changes) + (1 if i_new_changes == n_retries else 0))
            assert mock_notify.call_count == len(expected_calls)
            mock_notify.assert_has_calls(expected_calls)

            # Verify expected number of calls to the retry strategy.
            assert settings.retry_policy.call_count == n_retries

            # Verify each call individually.
            for i in range(n_retries):
                # Verify that the retry strategy was called with the expected RetryInfo.
                args, kwargs = settings.retry_policy.call_args_list[i]
                assert len(args) == 1
                assert len(kwargs) == 0
                assert type(args[0]) == RetryInfo
                # Verifying the number of fails at the time each call has happened would require a new RetryInfo object
                # for each call. We assume that the loop re-uses the same object while re-trying, so the number of fails
                # must be the total number of re-tries that have eventually happened while in the loop.
                if i_new_changes < n_retries:
                    # The first i_new_changes + 1 calls should have failed, so number of fails should be that number.
                    # After, the new incoming changes should have re-set the RetryInfo, so for the remaining
                    # (n_retries - i_new_changes - 1) calls, the number of fails should be just that.
                    assert args[0].fails == (i_new_changes + 1 if i <= i_new_changes else n_retries - i_new_changes - 1)
                else:
                    # New changes come in earliest when the notification function is already in its first call that
                    # returns normally. So all n_retries calls to the retry policy have happened before.
                    assert args[0].fails == n_retries
                assert args[0].exception == notify_exception
                assert type(args[0].since) == float

            # Assert incoming changes holder is re-set.
            assert incoming.changes is None
        finally:
            # Unblock notification thread, maybe.
            event_continue.set()

            # Start thread to cause main loop thread to stop.
            t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
            t1.start()

            t1.join(timeout=1)
            t2.join(timeout=1)

            assert not t1.is_alive()
            assert not t2.is_alive()

    def test_exists_from_notification_loop(self, mocker: pytest_mock.MockerFixture, mock_notify: MagicMock,
                                           context: Context):
        """Test if main loop exits when notified of 'sentinel' while still in the notification loop.

        This test simulates a scenario where the main loop tries to notify the target of changes, but the notification
        function repeatedly throws an exception, indicating that it was not successful. THe retry policy is set up so
        that the notification will be attempted again, after a short delay, until success.

        While the notification process is ongoing, the main loop gets notified of the special signal that indicates
        that it should exit. The main loop should then exit as early as possible.

        Parameters
        ----------
        mocker : pytest_mock.MockFixture
            The pytest mock fixture.
        mock_notify : MagicMock
            The mock notify function. Used to block the main loop for a while so we can simulate new changes coming in.
        context : tuple
            The context fixture.
        """
        # Exception thrown by notification function when failing.
        notify_exception = Exception("foo")

        # Unpack context.
        condition, condition_wait_for_spy, incoming, settings = context

        # Set up mock notify function to always throw an exception.
        mock_notify.side_effect = notify_exception

        # Set up mock retry policy.
        def retry_policy_side_effect(info: RetryInfo) -> RetryPolicyResult:
            # Always retry with given delay.
            return True, 0.1

        settings.retry_policy.side_effect = retry_policy_side_effect

        # Create and start a thread that will notify the condition.
        t1 = threading.Thread(target=self.get_provider_target(condition, incoming, CHANGES_MODEL), daemon=True)
        t1.start()

        # Call main loop in another thread.
        t2 = threading.Thread(target=main._main_loop, args=(condition, incoming, settings), daemon=True)
        t2.start()

        time.sleep(0.5)

        # Assert t1 has finished.
        assert not t1.is_alive()

        # Assert t2 is still alive.
        assert t2.is_alive()

        # Assert that the condition was waited on zero times.
        assert condition_wait_for_spy.call_count == 0

        # Start thread to cause main loop thread to stop.
        t1 = threading.Thread(target=self.get_provider_target(condition, incoming, "sentinel"), daemon=True)
        t1.start()

        t1.join(timeout=1)
        t2.join(timeout=1)

        assert not t1.is_alive()
        assert not t2.is_alive()

        # Assert notification function was called at least once with the expected arguments.
        assert mock_notify.call_count >= 1
        mock_notify.assert_has_calls([
            mocker.call(url=settings.url, api_key=settings.api_key, changes=CHANGES_MODEL),
        ] * mock_notify.call_count)

        # Assert retry policy has been called at least once with the expected RetryInfo.
        assert settings.retry_policy.call_count >= 1

        for call_args in settings.retry_policy.call_args_list:
            args, kwargs = call_args
            assert len(args) == 1
            assert len(kwargs) == 0
            assert type(args[0]) == RetryInfo
            # Number of fails should be identical to number of times notification function was called.
            assert args[0].fails == mock_notify.call_count
            assert args[0].exception == notify_exception
            assert type(args[0].since) == float
