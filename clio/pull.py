import requests
import urllib.parse

import dvid as dv
import numpy as np
import pandas as pd

from functools import lru_cache

from .client import inject_client

__all__ = [
    "fetch_annotations",
    "fetch_group_annotations",
    "ids_exist",
]


@inject_client
def fetch_annotations(
    bodyid=None, *, version=None, show_extra=None, client=None, **kwargs
):
    """Fetch annotations for given body ID(s).

    If no limiting criteria are given will return all available annotations.

    Parameters
    ----------
    bodyId :    int | list thereof
                Body ID(s).
    version :   str, optional
                Version string, e.g. "v0.3.5" to fetch annotations for.
    show_extra : None | "user" | |time" | "all"
                Whether to also pull "_user" or "_time" fields or both.
    **kwargs
                Keyword arguments can be used to provide (additional) filters.
                See examples.

    Examples
    --------
    Fetch annotations for a single body
    >>> clio.fetch_annotations(154109)

    Fetch annotations for a multiple bodies
    >>> clio.fetch_annotations([154109, 24053])

    Fetch annotations by neuron status
    >>> clio.fetch_annotations(status='Soma Anchor')

    Fetch annotations by neuron status
    >>> clio.fetch_annotations(status='Soma Anchor')

    Fetch annotations by soma side
    >>> clio.fetch_annotations(soma_side='RHS')

    Fetch annotations by hemilineage
    >>> clio.fetch_annotations(hemilineage='7B')

    Fetch annotations by user
    >>> clio.fetch_annotations(user='janedoe@gmail.com')

    Fetch annotations by neuron class
    N.B. we use 'class_' to avoid clashing with Python's `class` symbol
    >>> clio.fetch_annotations(class_='Local interneuron')

    """
    assert show_extra in (None, "user", "time", "all")
    GET = {}
    if show_extra is not None:
        GET["show"] = show_extra

    # If no filters, fetch all available annotations straight from dvid
    if isinstance(bodyid, type(None)) and not kwargs:
        return _fetch_all_annotations(GET, version=version, client=client)

    if version is not None:
        GET["version"] = version

    url = client.make_url(
        "v2/json-annotations/", client.dataset, "neurons/query", **GET
    )

    query = kwargs
    # Strip leading or trailing underscores (for e.g. "_class")
    for k, v in query.items():
        if k.startswith("_"):
            query.pop(k)
            query[k[1:]] = v
        elif k.endswith("_"):
            query.pop(k)
            query[k[:-1]] = v

    if not isinstance(bodyid, type(None)):
        if isinstance(bodyid, (str, int)):
            bodyid = [bodyid]
        bodyid = np.unique(np.asarray(bodyid).astype(int)).tolist()
        query["bodyid"] = bodyid

    an = client._fetch_pandas(url, json=query, ispost=True)

    if not isinstance(bodyid, type(None)):
        miss = ~np.isin(bodyid, an.bodyid)
        if any(miss):
            # Check if any of the body IDs do not exists
            exists = ids_exist(np.array(bodyid)[miss], client=client)
            if any(~exists):
                print(
                    "The following body IDs do not appear to exist in the "
                    f"head node {client.head_uuid}: "
                )
                print(", ".join(np.array(bodyid)[miss][~exists].astype(str)))

    return an


def _fetch_all_annotations(GET, version, client):
    """Fetch all annotations going straight to the DVID server.

    Parameters
    ----------
    GET :       dict
                Dictionary of GET parameters.
    client :    clio.Client

    Returns
    -------
    annotations :   pandas.DataFrame

    """
    if version is None:
        node = ":master"
    else:
        node = client.fetch_versions()[version]

    url = client.meta["dvid"] + f"/api/node/{node}/segmentation_annotations/all"

    if GET:
        url += "?{}".format(urllib.parse.urlencode(GET))

    r = requests.get(url)
    r.raise_for_status()
    return pd.DataFrame.from_records(r.json())


@inject_client
def fetch_group_annotations(group, *, client=None):
    """Fetch all point annotations by a given group.

    Requires the user to have the appropriate role. See also Client.fetch_roles.

    Parameters
    ----------
    group :     str | None
                Name of the group to fetch annotations for. If `None` will
                fetch only annotations for own user.
    client :    clio.Client
                If not explicitly provided will use the default client.

    Returns
    -------
    annotations :   pandas.DataFrame

    """
    if group:
        url = client.make_url("v2/annotations/", client.dataset, groups=group)
    else:
        url = client.make_url("v2/annotations/", client.dataset)
    return client._fetch_pandas(url)


@inject_client
def ids_exist(bodyid, *, client=None):
    """Check if body IDs exists.

    Parameters
    ----------
    bodyid :    int | list thereof
                Body ID(s) to check.
    client :    clio.Client
                If not explicitly provided will use the default client.

    Returns
    -------
    np.array
                Array of True/False.

    """
    if isinstance(bodyid, (str, int)):
        bodyid = [bodyid]

    bodyid = np.asarray(bodyid).astype(int)

    # It is much faster to ask if a body has an annotation than it is to
    # check if a body ID exists
    exists = np.zeros(len(bodyid), dtype=bool)
    exists[np.isin(bodyid, _annotated_bodies(client=client))] = True

    if any(~exists):
        exists[
            dv.ids_exist(
                bodyid,
                progress=False,
                server=client.meta["dvid"],
                node=client.head_uuid,
            )
        ] = True

    return exists


@lru_cache
@inject_client
def _annotated_bodies(*, client=None):
    """Get IDs of currently annotated bodies."""
    url = client.meta["dvid"] + "/api/node/:master/segmentation_annotations/keys"
    r = requests.get(url)
    r.raise_for_status()
    return np.array(r.json()).astype(int)

