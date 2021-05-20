import os
import os.path
import requests

CLIO_STORE_URL = {
    'prod': 'https://clio-store-vwzoicitea-uk.a.run.app',
    'test': 'https://clio-test-7fdj77ed7q-uk.a.run.app'
}
CLIO_ENDPOINTS = {
    'vnc-annotations-query': 'json-annotations/VNC/neurons/query'
}

CLIO_TOKEN_URL = 'https://clio-store-vwzoicitea-uk.a.run.app/v2/server/token'
TOKEN_CACHE_FILE = 'flyem_token.json'

def get_identity_token() -> str:
    """Returns a Google identity token using installed gcloud or manual input"""
    try:
        user_token = os.popen("gcloud auth print-identity-token").read()
    except:
        print("Unable to get identity token from gcloud.  Is it installed?")
        print("Switching to manual entering of token from command line.")
        print("Copy and paste token from clio.janelia.org User Settings page.")
        print("This can be avoided by installing the Google gcloud utility.\n")
        user_token = input("Enter your user identity token: ")
    return user_token.rstrip()

def get_clio_token():
    """Returns a long-lived FlyEM token, possibly from local cached file"""
    if os.path.exists(TOKEN_CACHE_FILE):
        with open(TOKEN_CACHE_FILE, 'r') as f:
            return f.read()
    else:
        user_token = get_identity_token()
        r = requests.post(CLIO_TOKEN_URL, headers = {'Authorization': f'Bearer {user_token}'})
        if r.status_code != 200:
            print(f"unable to retrieve long-lived FlyEM token: {r.text}")
            return None
        flyem_token = r.text.strip('"')
        with open(TOKEN_CACHE_FILE, 'w') as f:
            f.write(flyem_token)
        return flyem_token

flyem_token = get_clio_token()

def clio_url(store: str, endpoint: str) -> str:
    if store not in CLIO_STORE_URL:
        raise Exception(f'"{store}" is not a valid store. Use "prod" or "test"')
    if endpoint not in CLIO_ENDPOINTS:
        raise Exception(f'No endpoint available for "{endpoint}"')
    return CLIO_STORE_URL[store] + "/v2/" + CLIO_ENDPOINTS[endpoint]

def post(store: str, endpoint: str, json_payload: dict = None, str_payload: str = None):
    if json_payload:
        r = requests.post(clio_url(store, endpoint), json = json_payload, headers = {'Authorization': f'Bearer {flyem_token}'})
    else:
        r = requests.post(clio_url(store, endpoint), data = str_payload, headers = {'Authorization': f'Bearer {flyem_token}'})
    return r.status_code, r.content
