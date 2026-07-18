"""
Client to interact with a Clio backend.
"""

import os
import jwt
import json
import copy
import time
import inspect
import logging
import functools
import requests
import threading

import urllib
import urllib3
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning

import urllib.parse as urlparse
from urllib.parse import urlencode

from pathlib import Path
from functools import lru_cache

from requests import Session
from requests.adapters import HTTPAdapter

import pandas as pd

# ujson is faster than Python's builtin json module
import ujson

logger = logging.getLogger(__name__)
DEFAULT_CLIO_CLIENT = None
CLIO_CLIENTS = {}
CLIO_TOKEN_FILE = "~/clio_token.json"
CLIO_TOKEN_URL = "https://clio-store-vwzoicitea-uk.a.run.app/v2/server/token"
CLIO_TEST_STORE = "https://clio-test-7fdj77ed7q-uk.a.run.app"
CLIO_WEBSITE_URL = "https://clio.janelia.org"
# Deep-link straight to the page that shows the copyable "ClioStore Token".
CLIO_SETTINGS_URL = f"{CLIO_WEBSITE_URL}/settings"


def default_client():
    """
    Obtain the default Client object to use.
    This function returns a separate copy of the
    default client for each thread (and process).

    There's usually no need to call this function.
    It is automatically called by all query functions if
    you haven't passed in an explict `client` argument.

    """
    global DEFAULT_CLIO_CLIENT

    thread_id = threading.current_thread().ident
    pid = os.getpid()

    try:
        c = CLIO_CLIENTS[(thread_id, pid)]
    except KeyError:
        if DEFAULT_CLIO_CLIENT is None:
            raise RuntimeError(
                "No default Client has been set yet. "
                "Please create a Client object to serve as the default"
            )

        c = copy.deepcopy(DEFAULT_CLIO_CLIENT)
        CLIO_CLIENTS[(thread_id, pid)] = c

    return c


def set_default_client(client):
    """
    Set (or overwrite) the default Client.

    There's usually no need to call this function.
    It's is automatically called when your first
    ``Client`` is created, but you can call it again
    to replace the default.
    """
    global CLIO_CLIENTS
    global DEFAULT_CLIO_CLIENT

    thread_id = threading.current_thread().ident
    pid = os.getpid()

    DEFAULT_CLIO_CLIENT = client
    CLIO_CLIENTS.clear()
    CLIO_CLIENTS[(thread_id, pid)] = client


def inject_client(f):
    """
    Decorator.
    Injects the default 'client' as a keyword argument
    onto the decorated function, if the user hasn't supplied
    one herself.

    In typical usage the user will create one Client object,
    and use it with every clio-py function.
    Rather than requiring the user to pass the the client
    to every clio-py call, this decorator automatically
    passes the default (global) Client.
    """
    argspec = inspect.getfullargspec(f)
    assert "client" in argspec.kwonlyargs, (
        f"Cannot wrap {f.__name__}: clio API wrappers must accept 'client' as a keyword-only argument."
    )

    @functools.wraps(f)
    def wrapper(*args, client=None, **kwargs):
        if client is None:
            client = default_client()
        return f(*args, **kwargs, client=client)

    wrapper.__signature__ = inspect.signature(f)
    return wrapper


def set_token(token):
    f"""Save Clio API token to {CLIO_TOKEN_FILE}.

    Parameters
    ----------
    token :     str
                Your Clio API token. Get it from the "settings" menu in the
                top right corner of the Clio website.

    """
    if not isinstance(token, str):
        raise TypeError(f'Token must be string, got "{type(token)}"')

    p = Path(CLIO_TOKEN_FILE).expanduser()
    with open(p, "w") as f:
        json.dump({"token": token}, f)


def load_token():
    """Load Clio token from file."""
    p = Path(CLIO_TOKEN_FILE).expanduser()
    if not p.is_file():
        raise FileNotFoundError(
            "Clio secret file not found. Please use `clio.login` or "
            "`clio.set_token` to save a token."
        )

    with open(p, "r") as f:
        token = json.load(f)

    return token["token"]


def _unwrap_token(token):
    """Normalise a token string.

    Accepts either a raw token or the full ``{"token": "..."}`` JSON document
    (e.g. the contents of the token file) and returns the bare token string.
    """
    token = token.strip()
    # A JSON document starts with "{"; a bare token never does.
    if token.startswith("{"):
        try:
            token = ujson.loads(token)["token"]
        except Exception:
            raise RuntimeError(
                "Did not understand token. Please provide the entire JSON "
                "document or (only) the complete token string."
            )
    return token.replace('"', "")


def login(url=CLIO_SETTINGS_URL, save=True):
    """Log in to Clio by pasting a token from the website.

    This is the recommended way to authenticate and, unlike the ``gcloud``
    route, requires no extra dependencies. It opens the Clio settings page in
    your browser so you can copy your token, then prompts you to paste it here.

    Parameters
    ----------
    url :   str
            The page to open. Defaults to the settings page of the production
            instance, which shows the copyable "ClioStore Token".
    save :  bool
            If ``True`` (default), save the token to disk via `set_token` so
            future ``Client``s pick it up automatically.

    Returns
    -------
    token : str

    """
    import webbrowser

    print(
        f"Opening {url} in your browser.\n"
        'Log in if prompted, then copy your "ClioStore Token" and paste it here.'
    )
    try:
        webbrowser.open(url)
    except Exception:
        # e.g. a headless environment with no browser available
        pass

    token = _unwrap_token(input("Paste your Clio token here: "))
    if not token:
        raise ValueError("No token provided.")

    if save:
        set_token(token)

    return token


def get_token_gcloud(google_identity_token=None, save=True):
    """Fetch a Clio token via a Google identity token from ``gcloud``.

    This is an optional convenience for users who have the ``gcloud`` CLI
    installed and linked to the Google account they use for Clio; it lets
    clio-py fetch and refresh tokens without manual steps. Most users can
    instead run `clio.login` or `clio.set_token` and avoid the ``gcloud``
    dependency entirely.
    """
    # If not provided try using gcloud
    if not google_identity_token:
        google_identity_token = os.popen("gcloud auth print-identity-token").read()

    # Some clean-up
    google_identity_token = google_identity_token.strip()

    # If still no token
    if not google_identity_token:
        raise ValueError(
            "Unable to obtain a Google identity token from `gcloud`. Make "
            "sure `gcloud` is installed and configured, or use `clio.login()` "
            "/ `clio.set_token()` to provide a token manually."
        )

    # Fetch a new Clio token
    r = requests.post(
        CLIO_TOKEN_URL, headers={"Authorization": f"Bearer {google_identity_token}"}
    )
    if r.status_code != 200:
        raise ValueError(f"Unable to retrieve long-lived Clio token: {r.text}")

    token = r.text.strip('"')

    if save:
        _ = set_token(token)

    return token


# Backwards-compatible alias for the previous (misspelled) name.
get_token_glcoud = get_token_gcloud


def _get_token_or_raise():
    """Obtain a token when none was provided and none is saved.

    Falls back to ``gcloud`` only when it is actually available; otherwise
    raises with instructions pointing at the dependency-free options.
    """
    import shutil

    if shutil.which("gcloud"):
        return get_token_gcloud()

    raise RuntimeError(
        "No Clio token found. Run `clio.login()` to log in via the Clio "
        "website, or `clio.set_token('...')` with a token copied from "
        f'{CLIO_SETTINGS_URL} (the "ClioStore Token").\n'
        "Advanced: install and configure `gcloud` to have clio-py fetch and "
        "refresh tokens automatically."
    )


class Client:
    def __init__(
        self,
        server="https://clio-store-vwzoicitea-uk.a.run.app",
        dataset=None,
        token=None,
        verify=True,
    ):
        """
        Client constructor.

        The first ``Client`` you create will be stored as the default
        ``Client`` to be used with all ``clio-py`` functions
        if you don't explicitly specify one.

        Args:
            server:     str
                URL of Clio server.
            token:      str
                Clio token. If not passed explicitly, clio-py uses a token
                previously saved via `clio.login` or `clio.set_token` (and,
                if `gcloud` is installed, can fetch one automatically).
                Your token can be retrieved from the settings menu in the
                Clio web interface.

            verify:     bool
                If ``True`` (default), enforce signed credentials.

            dataset:    str
                The dataset to run all queries against, e.g. 'VNC'.
                If not provided, the server prints a list of available
                datasets and exits.
        """
        # If no token, try a saved one and otherwise guide the user
        if not token:
            try:
                # Try loading from file
                token = load_token()
            except FileNotFoundError:
                # No saved token: use gcloud if available, else raise with
                # instructions for the dependency-free options.
                token = _get_token_or_raise()

        # Accept either a raw token or the full JSON document
        token = _unwrap_token(token)

        # Setting self.token triggers validation and (once it exists) updates
        # the session header.
        self.token = token

        if "://" not in server:
            server = "https://" + server
        elif server.startswith("http://"):
            raise RuntimeError("Server must be https, not http")
        elif not server.startswith("https://"):
            protocol = server.split("://")[0]
            raise RuntimeError(f"Unknown protocol: {protocol}")

        # Remove trailing backslash
        while server.endswith("/"):
            server = server[:-1]

        self.server = server

        self.session = Session()
        self.session.headers.update(
            {
                "Authorization": "Bearer " + self.token,
                "Content-type": "application/json",
            }
        )

        # If the connection fails, retry a couple times.
        retries = Retry(connect=2, backoff_factor=0.1)
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self.verify = verify
        if not verify:
            urllib3.disable_warnings(InsecureRequestWarning)

        all_datasets = [*self.fetch_datasets().keys()]
        if len(all_datasets) == 0:
            raise RuntimeError(f"The clio server {self.server} has no datasets!")

        if len(all_datasets) == 1 and not dataset:
            self.dataset = all_datasets[0]
            logger.info(f"Initializing clio.Client with dataset: {self.dataset}")
        elif dataset in all_datasets:
            self.dataset = dataset
        elif dataset is None:
            raise RuntimeError(
                f"Please specify a dataset from the following list: {all_datasets}"
            )
        else:
            raise RuntimeError(
                f"Dataset '{dataset}' does not exist on"
                f" the clio server ({self.server}).\n"
                f"Available datasets: {all_datasets}"
            )

        # Set this as the default client if there isn't one already
        global DEFAULT_CLIO_CLIENT
        if DEFAULT_CLIO_CLIENT is None:
            set_default_client(self)

    def __repr__(self):
        s = f'Client("{self.server}", "{self.dataset}"'
        if not self.verify:
            s += ", verify=False"
        s += ")"
        return s

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, token):
        # Make sure token is principally valid
        self._validate_token(token)

        # Set token
        self._token = token

        # Update session header
        if hasattr(self, "session"):
            self.session.headers["Authorization"] = "Bearer " + self._token

    @property
    def head_version(self):
        """Head version."""
        if not getattr(self, "_head_version", None):
            self._head_version = self._fetch_json(
                f"{self.server}/v2/json-annotations/{self.dataset}/neurons/head_tag"
            )
        return self._head_version

    @property
    def head_uuid(self):
        """Head node UUID."""
        if not getattr(self, "_head_uuid", None):
            self._head_uuid = self._fetch_json(
                f"{self.server}/v2/json-annotations/{self.dataset}/neurons/head_uuid"
            )
        return self._head_uuid

    @property
    def meta(self):
        """Meta data for this dataset."""
        if not getattr(self, "_meta", None):
            self._meta = self._fetch_json(f"{self.server}/v2/datasets")[self.dataset]
        return self._meta

    def _add_identifier(self, url):
        """Add identifier to URL."""
        url_parts = list(urlparse.urlparse(url))
        query = dict(urlparse.parse_qsl(url_parts[4]))

        if "app" not in query:
            query.update({"app": "clio-py"})

        url_parts[4] = urlencode(query)

        return urlparse.urlunparse(url_parts)

    def make_url(self, *args, test=False, **GET):
        """Generates URL."""
        # Generate the URL
        if not test:
            url = self.server
        else:
            url = CLIO_TEST_STORE
        for arg in args:
            arg_str = str(arg)
            joiner = "" if url.endswith("/") else "/"
            relative = arg_str[1:] if arg_str.startswith("/") else arg_str
            url = requests.compat.urljoin(url + joiner, relative)
        if GET:
            url += "?{}".format(urllib.parse.urlencode(GET))
        return url

    def refresh_token(self, *args, **kwargs):
        """Try refreshing Clio token. Requires gcloud."""
        self.token = get_token_gcloud(*args, **kwargs)
        print("Clio token successfully refreshed.")

    def token_time_left(self):
        """Time left before token expires [s], or ``None`` if unknown.

        Returns ``None`` for opaque (non-JWT) tokens, whose expiry cannot be
        read locally; the server remains the source of truth in that case.
        """
        try:
            decoded = jwt.decode(
                self.token, algorithms=["HS256"], options={"verify_signature": False}
            )
        except jwt.PyJWTError:
            return None

        if "exp" not in decoded:
            return None

        return int(decoded["exp"]) - int(time.time())

    def _validate_token(self, token=None):
        """Basic sanity check on a token.

        Opaque (non-JWT) tokens are accepted as-is; the expected fields are
        only enforced when the token is a decodable JWT.
        """
        if token is None:
            token = self.token

        if not isinstance(token, str) or not token.strip():
            raise ValueError("Clio token is empty.")

        try:
            decoded = jwt.decode(
                token, algorithms=["HS256"], options={"verify_signature": False}
            )
        except jwt.PyJWTError:
            # Not a JWT (e.g. an opaque server-issued token) -- accept as-is.
            return

        for field in ["email", "exp"]:
            if field not in decoded:
                raise ValueError(
                    f"Clio token doesn't have {field} field and "
                    f"is therefore invalid: {decoded}"
                )

    def _fetch(self, url, json=None, ispost=False, identify=True):
        time_left = self.token_time_left()
        if time_left is not None and time_left < 0:
            print("Clio token expired. Attempting refresh...")
            self.refresh_token()

        # Make sure URL has identifier
        if identify:
            url = self._add_identifier(url)

        if ispost:
            r = self.session.post(url, json=json, verify=self.verify)
        else:
            assert json is None, "Can't provide a body via GET method"
            r = self.session.get(url, verify=self.verify)
        r.raise_for_status()
        return r

    def _fetch_raw(self, url, json=None, ispost=False, identify=True):
        return self._fetch(url, json=json, ispost=ispost, identify=identify).content

    def _fetch_json(self, url, json=None, ispost=False, identify=True):
        r = self._fetch(url, json=json, ispost=ispost, identify=identify)
        return ujson.loads(r.content)

    def _fetch_pandas(self, url, json=None, ispost=False, identify=True):
        r = self._fetch_json(url, json=json, ispost=ispost, identify=identify)
        return pd.DataFrame.from_records(r)

    ##
    ## DB-META
    ##
    @lru_cache
    def fetch_datasets(self):
        """
        Fetch basic information about the available datasets on the server.
        """
        return self._fetch_json(f"{self.server}/v2/datasets")

    ##
    ## USER
    ##
    @lru_cache
    def fetch_roles(self):
        """
        Fetch basic information about your user profile,
        including your access level and group.
        """
        return self._fetch_json(f"{self.server}/v2/roles")

    ##
    ## Dataset
    ##
    @lru_cache
    def fetch_fields(self):
        return self._fetch_json(
            f"{self.server}/v2/json-annotations/{self.dataset}/neurons/fields"
        )

    @lru_cache
    def fetch_versions(self):
        return self._fetch_json(
            f"{self.server}/v2/json-annotations/{self.dataset}/neurons/versions"
        )

    @lru_cache
    def fetch_head_tag(self):
        return self._fetch_json(
            f"{self.server}/v2/json-annotations/{self.dataset}/neurons/head_tag"
        )

    @lru_cache
    def fetch_head_uuid(self):
        return self._fetch_json(
            f"{self.server}/v2/json-annotations/{self.dataset}/neurons/head_uuid"
        )

    @lru_cache
    def tag_to_uuid(self, tag):
        return self._fetch_json(
            f"{self.server}/v2/json-annotations/{self.dataset}/neurons/tag_to_uuid/{tag}"
        )

    @lru_cache
    def uuid_to_tag(self, uuid):
        return self._fetch_json(
            f"{self.server}/v2/json-annotations/{self.dataset}/neurons/uuid_to_tag/{uuid}"
        )
