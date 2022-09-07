import warnings

import pandas as pd

from .client import inject_client

CLIO_TEST_STORE = 'https://clio-test-7fdj77ed7q-uk.a.run.app'

@inject_client
def annotate_bodyids(annotations, test=True, *, client=None):
    """Annotate bodyIDs.

    If no limiting criteria are given will return all available annotations.

    Parameters
    ----------
    annotations : pd.DataFrame
                DataFrame with bodyid annotations, must have a `bodyid` column.

    test        : bool
                Whether to send annotations to test (True) or production (False) server.
                Set to False only if you know what you are doing.

    Examples
    --------
    Fetch annotations for a single body
    >>> dummy_annots = pd.DataFrame.from_records({'bodyid':[10000, 14132], 'b':[44,66]})
    >>> annotate_bodyids(dummy_annots)
    """

    if test:
        prev_store = client.server
        if client.server !=  CLIO_TEST_STORE:    
            client.server = CLIO_TEST_STORE
        warnings.warn("Pushing data annotations to the test store.")
    if not isinstance(annotations, pd.DataFrame):
        raise TypeError("Annotations must be a pandas.DataFrame")
    if not 'bodyid' in annotations.columns:
        raise ValueError("annotations DataFrame must contain a `bodyid` column")
    url_template = f"v2/json-annotations/{client.dataset}/neurons"
    url = client.make_url(url_template)
    status = client._fetch(url, json=annotations.to_dict(orient='records'), ispost=True)
    if test: client.server = prev_store
    if status.ok:
        print("Bodyids annotated successfully.")


