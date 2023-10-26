try:
    import logging

    from fastapi import FastAPI, Header, HTTPException
    import uvicorn
    from typing_extensions import Literal, Annotated, Union
    from starlette.requests import Request

    from exchange_calendar_service.updater.api import BaseChangeProvider, ChangeHandler, ErrorHandler, \
        ChangeProviderError
    from exchange_calendar_service.updater.provider.util import fetch_url
    from pydantic import BaseModel, Field
    import hashlib
    import hmac

    def get_signature_header(body: bytes, secret: str) -> str:
        """Return the signature expected in the x-hub-signature-256 request header for the given request body and
        secret.

        Parameters
        ----------
            body:
                The original request body to verify
            secret:
                The secret associated with the GitHub webhook

        Returns
        -------
            bool:
                True if the signature is valid, False otherwise
        """
        return "sha256=" + hmac.new(secret.encode('utf-8'), msg=body, digestmod=hashlib.sha256).hexdigest()

    log = logging.getLogger(__name__)

    # Models for the content of the GitHub webhook. Not all fields are modeled, only what's important.

    class GitHubWebHookRepository(BaseModel):
        full_name: str

    class GitHubWebHookContent(BaseModel):
        repository: GitHubWebHookRepository

    class ServerSettings(BaseModel):
        bind_host: str = Field(default='0.0.0.0')
        bind_port: int = Field(default=80)
        path: str = Field(default='/')

    class GitHubWebhookChangeProvider(BaseChangeProvider):
        type: Literal["gh_webhook"]
        server: ServerSettings
        secret: str = Field(default=None)
        url: str
        _app: FastAPI = None
        _digest: str = None

        def setup(self, change_handler: ChangeHandler, error_handler: ErrorHandler):

            # Create FastAPI app.
            self._app = FastAPI()

            # Handler to receive webhook POST call.
            @self._app.post(self.server.path)
            async def receive_webhook(request: Request, data: GitHubWebHookContent,
                                      x_hub_signature_256: Annotated[Union[str, None], Header()] = None):
                log.debug('Received GitHub webhook request.')

                try:
                    if self.secret is not None:
                        # Check if x-hub-signature-256 header is present.
                        if x_hub_signature_256 is None:
                            raise HTTPException(status_code=403, detail="x-hub-signature-256 header is missing.")

                        # Get raw request body and decode with UTF-8 into a string.
                        try:
                            request_body = await request.body()
                        except Exception as e:
                            raise HTTPException(status_code=400, detail="Error decoding request body.") from e

                        # Verify signature.
                        if not hmac.compare_digest(get_signature_header(body=request_body, secret=self.secret),
                                                   x_hub_signature_256):
                            raise HTTPException(status_code=403, detail="x-hub-signature-256 header is invalid!")

                    # No further validation of the request body.

                    # Load changes.
                    self._digest = fetch_url(self.url, change_handler, error_handler, self._digest)
                except HTTPException as e:
                    # Invoke error handler.
                    error_handler(e)
                    raise e
                except Exception as e:
                    msg = "Error processing webhook request."
                    error_handler(ChangeProviderError(msg, cause=e))
                    return HTTPException(status_code=500, detail=msg)

                return

            # Initial fetch of URL.
            self._digest = fetch_url(self.url, change_handler, error_handler, self._digest)

            # Start webserver for app.
            uvicorn.run(self._app, host=self.server.bind_host, port=self.server.bind_port)

        def finalize(self):
            # There does not seem to be a way to stop uvicorn programmatically.
            pass

except ImportError:
    # Required dependencies not available.
    pass
