import numpy as np

from .client import inject_client

__all__ = ['fetch_annotations', 'fetch_group_annotations']


@inject_client
def fetch_annotations(bodyid=None, *, client=None, **kwargs):
    """Fetch annotations for given body ID(s).

    If no limiting criteria are given will return all available annotations.

    Parameters
    ----------
    bodyId :    int | list thereof
                Body ID(s).
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

    Fetch annotations by neuron class (note the '_class' parameter)
    >>> clio.fetch_annotations(_class='Local interneuron')

    """
    # If no filters, fetch all available annotations
    if isinstance(bodyid, type(None)) and not kwargs:
        url = client.make_url('v2/json-annotations/', client.dataset, 'neurons/all')
        return client._fetch_pandas(url, ispost=False)

    url = client.make_url('v2/json-annotations/', client.dataset, 'neurons/query')

    query = kwargs
    # Strip leading underscores (for e.g. "_class")
    for k, v in query.items():
        if k.startswith('_'):
            query.pop(k)
            query[k[1:]] = v

    if not isinstance(bodyid, type(None)):
        if isinstance(bodyid, (list, set, np.ndarray)):
            bodyid = list(bodyid)
        else:
            bodyid = [bodyid]
        query['bodyid'] = bodyid

    return client._fetch_pandas(url, json=query, ispost=True)


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
        url = client.make_url('v2/annotations/', client.dataset, groups=group)
    else:
        url = client.make_url('v2/annotations/', client.dataset)
    return client._fetch_pandas(url)
