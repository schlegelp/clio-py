import requests

import numpy as np
import pandas as pd

from functools import lru_cache
from tqdm.auto import tqdm

from .client import inject_client
from .pull import fetch_annotations, ids_exist

__all__ = [
    "set_annotations",
    "set_fields",
]

TYPES_MAPPING = {
    "integer": (int, np.integer, np.int64, np.int32),
    "string": (str, object),
    "array": (object, np.ndarray),
    "boolean": (bool,),
}


@inject_client
def set_fields(
    x,
    *,
    test=False,
    version=None,
    protect=("user",),
    validate=True,
    chunksize=50,
    progress=True,
    client=None,
    **fields,
):
    """Set fields for given body ID(s).

    This function is a wrapper around `set_annotations` and is intended to
    provide a more convenient interface for setting fields for a set of body
    IDs to the same value.

    Parameters
    ----------
    x :         pd.DataFrame | dict
                If DataFrame must contain at least a 'bodyid' column.
    **fields
                Fields to set. The field names are passed as keyword arguments
                and the values as the values of those arguments. For example:
                `set_fields(x, type='SMP258')`. Since `class` is a reserved
                keyword in Python, use `class_` instead.
    version :   str
                Optional clio version to associate with this annotation. The
                default NULL uses the current head version returned by the API.
    protect :   bool | list of fields
                Which fields to protect from overwriting.
                  - if `protect` is a list of names, these fields will not be
                    overwritten. By convention, the `user` field is intended
                    for the user who added that annotation in the first place
                    and should hence not be overwritten.
                  - `protect=True`: no data in Clio will be overwritten, i.e.
                    only new data will be added
                  - `protect=False`:  all fields will be overwritten with new data
                    for each non-empty value in x. If `write_empty_fields=True`
                    even empty fields in x will overwrite fields in the database.
    validate :  bool
                Whether to validate the schema of the annotations before
                (highly recommended). If set to False, the schema will not be
                checked and the annotations will be sent as is.
    chunksize : int
                Number of annotations uploaded in one go.
    progress :  bool
                Whether to show a progress bar for the upload. Defaults to True.

    Notes
    -----
    1. On the backend annotations are only written if the submitted value is
       different from the old. That also means that the {field}_user is not
       updated unless there is a new value.
    2. Empty fields (e.g. `type=None`) will overwrite fields in the database.

    """
    if isinstance(x, (list, dict, set, pd.Series)):
        x = np.array(x)
    elif isinstance(x, (str, int, np.integer)):
        x = np.array([x])
    elif not isinstance(x, np.ndarray):
        raise TypeError(f"Expected `x` to be a list, array or a scalar. Got {type(x)}")

    assert len(x) > 0, "No body IDs provided."
    assert x.ndim == 1, "Expected `x` to be a 1D array."

    # Make sure these are actually numeric IDs
    x = x.astype(int, copy=False)

    # Turn `x` and **fields into a DataFrame
    ann = pd.DataFrame()
    ann["bodyid"] = x

    for k, v in fields.items():
        # Clear underscores at the end of the field name (e.g. for `class_`)
        if k.endswith("_"):
            k = k[:-1]

        ann[k] = v

    return set_annotations(
        ann,
        test=test,
        version=version,
        protect=protect,
        validate=validate,
        chunksize=chunksize,
        client=client,
        write_empty_fields=True,
        progress=progress,
    )


@inject_client
def set_annotations(
    x,
    *,
    test=False,
    version=None,
    write_empty_fields=False,
    protect=("user",),
    validate=True,
    chunksize=50,
    progress=True,
    client=None,
):
    """Set annotations for given body ID(s).

    Parameters
    ----------
    x :         pd.DataFrame | dict
                If DataFrame must contain at least a 'bodyid' column.
    version :   str
                Optional clio version to associate with this annotation. The
                default NULL uses the current head version returned by the API.
    write_empty_fields : bool
                When `x` is a data frame, this controls whether empty fields in
                `x` (i.e. NA or "") overwrite fields in the clio-store database
                (when they are not protected by the protect argument). The
                (conservative) default `write_empty_fields=False` does not
                overwrite. If you do want to set fields to an empty value
                (usually the empty string) then you must set
                `write_empty_fields=True`.
    protect :   bool | list of fields
                Which fields to protect from overwriting.
                  - if `protect` is a list of names, these fields will not be
                    overwritten. By convention, the `user` field is intended
                    for the user who added that annotation in the first place
                    and should hence not be overwritten.
                  - `protect=True`: no data in Clio will be overwritten, i.e.
                    only new data will be added
                  - `protect=False`:  all fields will be overwritten with new data
                    for each non-empty value in x. If `write_empty_fields=True`
                    even empty fields in x will overwrite fields in the database.
    validate :  bool
                Whether to validate the schema of the annotations before
                (highly recommended). If set to False, the schema will not be
                checked and the annotations will be sent as is.
    chunksize : int
                Number of annotations uploaded in one go.
    progress :  bool
                Whether to show a progress bar for the upload. Defaults to True.

    Notes
    -----
    1. On the backend annotations are only written if the submitted value is
       different from the old. That also means that the {field}_user is not
       updated unless there is a new value.

    """
    if isinstance(protect, str):
        protect = (protect,)

    if not isinstance(protect, (bool, tuple, list)):
        raise TypeError(
            "Expected `protect` to be a boolean, a string or a list "
            f' thereof. Got "{type(protect)}"'
        )

    if isinstance(x, dict):
        if not (
            all([isinstance(k, int) for k in x.keys()])
            and all([isinstance(v, dict) for v in x.values()])
        ):
            raise ValueError(
                "If `x` is dictionary it must be `{bodyid: {field: value}}`"
            )
        x = pd.DataFrame.from_dict(x, orient="index")
        x.index.name = "bodyid"
        x.reset_index(drop=False, inplace=True)

    if validate:
        _validate_schema(x, client)

    x = x.copy()
    x["bodyid"] = x.bodyid.astype(int)

    if any(x.bodyid.duplicated()):
        raise ValueError("Table contains duplicate body IDs.")

    if any(x.bodyid.isnull()):
        raise ValueError("Table contains empty body IDs.")

    an = x.to_dict(orient="records")

    # Drop empty entries
    if not write_empty_fields:
        an = [{k: v for k, v in at.items() if not pd.isnull(v)} for at in an]

    # Check if any of the body IDs do not exists
    exists = ids_exist(x.bodyid.values.tolist(), client=client)
    if any(~exists):
        raise ValueError(
            "The following body IDs do not appear to exist in the "
            f"head node {client.head_uuid}: "
            f"{', '.join(x.bodyid.values[~exists].astype(str))}"
        )

    # See if we need to protect any fields
    if protect is not False:
        existing = fetch_annotations(x.bodyid.values.tolist(), client=client)
        existing = existing.set_index("bodyid").to_dict(orient="index")
        # Turn into this format: {bodyId: ['user', 'type', ...]} for existing
        # fields
        existing = {k: list(v) for k, v in existing.items()}

        # Remove any field that already has a value
        if protect is True:
            an = [
                {k: v for k, v in at.items() if k not in existing.get(at["bodyid"], [])}
                for at in an
            ]
        else:
            an = [
                {
                    k: v
                    for k, v in at.items()
                    if k not in existing.get(at["bodyid"], []) or k not in protect
                }
                for at in an
            ]

    # Drop any record that is only {'bodyid'}
    an = [a for a in an if len(a) > 1]

    # Replace any np.nan with None
    an = [{k: v if not pd.isnull(v) else None for k, v in a.items()} for a in an]

    if version is None:
        version = client.head_version

    url = client.make_url(
        "v2/json-annotations/", client.dataset, f"neurons?{version}", test=test
    )

    with tqdm(
        total=len(an), desc="Writing annotations", leave=False, disable=not progress
    ) as pbar:
        for i in range(0, len(an), chunksize):
            chunk = an[i : i + chunksize]
            r = client._fetch(url, json=chunk, ispost=True)
            r.raise_for_status()
            pbar.update(len(chunk))

    return


@lru_cache
def _get_schema(client):
    """Get schema for annotations."""
    ds = client.fetch_datasets()[client.dataset]

    url = f"{ds['dvid']}/api/node/:master/segmentation_annotations/json_schema"
    r = requests.get(url)
    return r.json()


def _validate_schema(x, client):
    """Validate `x` against expected schema."""
    schema = _get_schema(client)

    for val in schema["required"]:
        if val not in x.columns:
            raise ValueError(f'Missing required "{val}" column.')

    # Note that this does not seem to contain specs for all available fields
    for col, specs in schema["properties"].items():
        # Make sure not to mess with the original schema!
        specs = specs.copy()

        # Specs is a dictionary containing either:
        #  - a single "type" ({'type': 'string'}),
        #  - a list of types ({'type': ['string', 'null']})
        #  - a "oneOf" key with a list of types/items for the positional columns
        #    such as "position"

        # Skip validation of positional columns for now:
        if "type" not in specs:
            continue

        if isinstance(specs["type"], list):
            nullable = "null" in specs["type"]
            # Drop "null" from the list
            specs["type"] = [t for t in specs["type"] if t != "null"]
            # If list is only one entry then unpack it
            if len(specs["type"]) == 1:
                specs["type"] = specs["type"][0]
            else:
                raise ValueError(
                    "Unexpected list of accepted types for "
                    f"column {col}: {specs['type']}"
                )
        else:
            nullable = specs["type"] == "null"

        if col in x.columns:
            expected_types = TYPES_MAPPING[specs["type"]]

            # Note: pandas will use `object` as datatype for integer columns if they
            # also contain `None`.
            if x[col].dtype.kind == "O":
                if not nullable and x[col].isnull().any():
                    raise ValueError(
                        f'Column "{col}" is not allowed to contain null values.'
                    )

                if x[col].notnull().any():
                    # Check individual values
                    for val in x[col].dropna():
                        if not any([isinstance(val, t) for t in expected_types]):
                            raise TypeError(
                                f'Column "{col}" should be of type(s) {expected_types}, '
                                f"got {type(val)}"
                            )
            elif x[col].dtype not in expected_types:
                raise TypeError(
                    f'Column "{col}" should be of type(s) {expected_types}, '
                    f"got {x[col].dtype}"
                )

    fields = client.fetch_fields()
    wrong = x.columns[~np.isin(x.columns, fields)]
    if any(wrong):
        raise ValueError(f"The following columns appear to be invalid fields: {wrong}")
