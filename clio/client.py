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

from requests import Session
from requests.adapters import HTTPAdapter

import pandas as pd

# ujson is faster than Python's builtin json module
import ujson

logger = logging.getLogger(__name__)
DEFAULT_CLIO_CLIENT = None
CLIO_CLIENTS = {}
CLIO_TOKEN_FILE = '~/clio_token.json'
CLIO_TOKEN_URL = 'https://clio-store-vwzoicitea-uk.a.run.app/v2/server/token'


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
                    "Please create a Client object to serve as the default")

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
    and use it with every neuprint function.
    Rather than requiring the user to pass the the client
    to every neuprint call, this decorator automatically
    passes the default (global) Client.
    """
    argspec = inspect.getfullargspec(f)
    assert 'client' in argspec.kwonlyargs, \
        f"Cannot wrap {f.__name__}: clio API wrappers must accept 'client' as a keyword-only argument."

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
    with open(p, 'w') as f:
        json.dump({'token': token}, f)


def load_token():
    """Load Clio token from file."""
    p = Path(CLIO_TOKEN_FILE).expanduser()
    if not p.is_file():
        raise FileNotFoundError('Clio secret file not found. Please use '
                                '`clio.set_token` to save a token.')

    with open(p, 'r') as f:
        token = json.load(f)

    return token['token']


def get_token_glcoud(google_identify_token=None, save=True):
    """Try using GCloud to automatically get a Clio token."""
    # If not provided try using gcloud
    if not google_identify_token:
        google_identify_token = os.popen("gcloud auth print-identity-token").read()

    # Some clean-up
    google_identify_token = google_identify_token.strip()

    # If still no token
    if not google_identify_token:
        raise ValueError('Unable to automatically refresh Clio token. Make '
                         'sure `gcloud` is installed and properly configured.')

    # Fetch a new Clio token
    r = requests.post(CLIO_TOKEN_URL,
                      headers={'Authorization': f'Bearer {google_identify_token}'})
    if r.status_code != 200:
        raise ValueError(f"Unable to retrieve long-lived Clio token: {r.text}")

    token = r.text.strip('"')

    if save:
        _ = set_token(token)

    return token


class Client:
    def __init__(self, server='https://clio-store-vwzoicitea-uk.a.run.app',
                 dataset=None, token=None, verify=True):
        """
        Client constructor.

        The first ``Client`` you create will be stored as the default
        ``Client`` to be used with all ``clio-py`` functions
        if you don't explicitly specify one.

        Args:
            server:
                URL of neuprintHttp server

            token:
                Clio token. Either pass explitily as an argument or set
                as ``CLIO_APPLICATION_CREDENTIALS`` environment variable.
                Your token can be retrieved by clicking on your account in
                the NeuPrint web interface.

            verify:
                If ``True`` (default), enforce signed credentials.

            dataset:
                The dataset to run all queries against, e.g. 'VNC'.
                If not provided, the server will use a default dataset for
                all queries.
        """
        # If no token
        if not token:
            try:
                # Try loading from file
                token = load_token()
            except FileNotFoundError:
                # If loading from file didn't work, try through refreshing
                token = get_token_glcoud()
            except BaseException:
                raise

        # Some token clean-up
        if ':' in token:
            try:
                token = ujson.loads(token)['token']
            except Exception:
                raise RuntimeError("Did not understand token. Please provide the entire JSON document or (only) the complete token string")

        # Don't set token directly since that triggers a validation
        self.token = token.replace('"', '')

        if '://' not in server:
            server = 'https://' + server
        elif server.startswith('http://'):
            raise RuntimeError("Server must be https, not http")
        elif not server.startswith('https://'):
            protocol = server.split('://')[0]
            raise RuntimeError(f"Unknown protocol: {protocol}")

        # Remove trailing backslash
        while server.endswith('/'):
            server = server[:-1]

        self.server = server

        self.session = Session()
        self.session.headers.update({"Authorization": "Bearer " + self.token,
                                     "Content-type": "application/json"})

        # If the connection fails, retry a couple times.
        retries = Retry(connect=2, backoff_factor=0.1)
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

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
        else:
            raise RuntimeError(f"Dataset '{dataset}' does not exist on"
                               f" the clio server ({self.server}).\n"
                               f"Available datasets: {all_datasets}")

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
        if hasattr(self, 'session'):
            self.session.headers['Authorization'] = "Bearer " + self._token

    def _add_identifier(self, url):
        """Add identifier to URL."""
        url_parts = list(urlparse.urlparse(url))
        query = dict(urlparse.parse_qsl(url_parts[4]))

        if 'app' not in query:
            query.update({'app': 'clio-py'})

        url_parts[4] = urlencode(query)

        return urlparse.urlunparse(url_parts)

    def make_url(self, *args, **GET):
        """Generates URL."""
        # Generate the URL
        url = self.server
        for arg in args:
            arg_str = str(arg)
            joiner = '' if url.endswith('/') else '/'
            relative = arg_str[1:] if arg_str.startswith('/') else arg_str
            url = requests.compat.urljoin(url + joiner, relative)
        if GET:
            url += '?{}'.format(urllib.parse.urlencode(GET))
        return url

    def refresh_token(self, *args, **kwargs):
        """Try refreshing Clio token. Requires glcoud."""
        self.token = get_token_glcoud(*args, **kwargs)
        print('Clio token successfully refreshed.')

    def token_time_left(self):
        """Time left before token expires [s]."""
        decoded = jwt.decode(self.token, algorithms=['HS256'],
                             options={"verify_signature": False})

        return int(decoded['exp']) - int(time.time())

    def _validate_token(self, token=None):
        """Check token."""
        if not token:
            token = self.token

        try:
            decoded = jwt.decode(token, algorithms=['HS256'],
                                 options={"verify_signature": False})
        except jwt.DecodeError:
            raise ValueError("Clio token not valid: unable to decode.")

        for field in ['email', 'exp']:
            if field not in decoded:
                raise ValueError(f"Clio token doesn't have {field} field and "
                                 f"is therefore invalid: {decoded}")

    def _fetch(self, url, json=None, ispost=False, identify=True):
        if self.token_time_left() < 0:
            print('Clio token expired. Attempting refresh...')
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

    def fetch_datasets(self):
        """
        Fetch basic information about the available datasets on the server.
        """
        return self._fetch_json(f"{self.server}/v2/datasets")

    ##
    ## USER
    ##

    def fetch_roles(self):
        """
        Fetch basic information about your user profile,
        including your access level and group.
        """
        return self._fetch_json(f"{self.server}/v2/roles")
