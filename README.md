# clio-py: Python client for clio-store

## Install
```bash
$ pip3 install git+git://github.com/schlegelp/clio-py@main
```

## Token
To access Clio programmatically you need an API token. Currently, Clio tokens
expire after a week. You have three options to get and set your token
(in increasing order of convenience):

### Option 1
Get your token from the Clio website (go to settings in the top right and copy
the "ClioStore Token") and provide it whenever you instantiate a client:

```Python
>>> import clio
>>> client = clio.Client(dataset='VNC', token="eyJhb.....")
```

### Option 2
Get your token from the Clio website and save it to disk. `clio-py` will
automatically use it when available and inform you when the token has expired
and you need to get a new one:

```Python
>>> import clio
>>> clio.set_token("eyJhb.....")  # this is a one-off until token expires!
>>> client = clio.Client(dataset='VNC')
```

### Option 3
Install [gcloud](https://cloud.google.com/sdk/docs/install) and link it with
your Google account (the same you use for Clio).

From now on, `clio-py` will use `gcloud` to auto-refresh your Clio token
whenever it expires.

```Python
>>> import clio
>>> client = clio.Client(dataset='VNC')
```

## Usage

```python
>>> import clio
>>> client = clio.Client(dataset='VNC')
>>> clio.fetch_annotations([154109])
  hemilineage long_tract   description                  user soma_side  ... to_review  bodyid entry_nerve soma_neuromere               position
0          7B             7B in T3 LHS  xxxxxxxxxx@gmail.com       LHS  ...            154109        None             T3  [17429, 21568, 21811]
```

## Documentation
TODO

## References
List of API [endpoints](https://clio-store-vwzoicitea-uk.a.run.app/) for Clio.
