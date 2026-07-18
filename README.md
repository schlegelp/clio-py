# clio-py: Python client for clio-store

## Install
```bash
$ pip3 install git+https://github.com/schlegelp/clio-py@main
```

## Token
To access Clio programmatically you need an API token. You can get and set
your token in a few ways — the first is recommended and requires no extra
dependencies.

### Option 1 (recommended): `clio.login()`
Run `clio.login()`. This opens the Clio settings page in your browser so you can
copy your token (the "ClioStore Token"), then prompts you to paste it. The token
is saved to disk, so this is a one-off until it expires:

```Python
>>> import clio
>>> clio.login()  # opens the browser and prompts you to paste your token
>>> client = clio.Client(dataset='VNC')
```

### Option 2: pass or save the token yourself
Get your token from the Clio website (settings in the top right → "ClioStore
Token") and either pass it directly:

```Python
>>> import clio
>>> client = clio.Client(dataset='VNC', token="eyJhb.....")
```

...or save it to disk once so it's picked up automatically:

```Python
>>> import clio
>>> clio.set_token("eyJhb.....")  # one-off until the token expires
>>> client = clio.Client(dataset='VNC')
```

### Option 3 (advanced): auto-refresh via `gcloud`
If you have the [gcloud](https://cloud.google.com/sdk/docs/install) CLI
installed and linked to the Google account you use for Clio, `clio-py` will use
it to fetch and refresh your token automatically. This is convenient for
long-running/automated use but installing `gcloud` is a heavy dependency most
users won't need.

```Python
>>> import clio
>>> client = clio.Client(dataset='VNC')  # uses gcloud if a token isn't already saved
```

## Usage

### Setup
```python
>>> import clio
>>> # You can get a list of available datasets by just invoking the client
>>> clio.Client()
...
RuntimeError: Please specify a dataset from the following list: ['CNS', 'MANC', 'VNC', 'hemibrain', 'manc:v1.0', 'medulla7column']
```

```python
>>> # Initialize client with a specific dataset
>>> client = clio.Client(dataset='VNC')
```

### Pulling annotations
```python
>>> # Fetch annotations for given ID(s)
>>> clio.fetch_annotations([154109])
  hemilineage long_tract   description                  user soma_side  ... to_review  bodyid entry_nerve soma_neuromere               position
0          7B             7B in T3 LHS  xxxxxxxxxx@gmail.com       LHS  ...            154109        None             T3  [17429, 21568, 21811]

```

```python
>>> # Fetch annotations by a given field
>>> clio.fetch_annotations(hemilineage='07B')
          avg_location        birthtime  bodyid  ... transmission old_bodyids tosoma_position
0  18910, 35515, 55240          primary   10010  ...          NaN         NaN             NaN
1  26128, 38807, 55528          primary   10018  ...          NaN         NaN             NaN
2  15749, 33362, 38398          primary   10087  ...          NaN         NaN             NaN
3  26128, 38807, 55528          primary   10090  ...          NaN         NaN             NaN
4                  NaN  early secondary   10148  ...          NaN         NaN             NaN
...
```

```python
>>> # Fetch all annotations
>>> clio.fetch_annotations()
          avg_location  birthtime  bodyid  ... subclassabbr tosoma_position source
0  20293, 15901, 10843  secondary   26896  ...          NaN             NaN    NaN
1                  NaN        NaN  139486  ...          NaN             NaN    NaN
2  27443, 20049, 23897  secondary  158190  ...          NaN             NaN    NaN
3  16884, 19674, 35784  secondary  165660  ...          NaN             NaN    NaN
4  13227, 35591, 30152  secondary   20366  ...          NaN             NaN    NaN
...
```

Please see `help(clio.fetch_annotations)` for further details and examples.


### Pushing annotations
:warning: Clio has little in the way of guard rails when it comes to writing annotations
to the database. It's easy to accidentally overwrite existing annotations. Do not use
this functionality unless you know exactly what you are doing! Please check in with a
a senior person/postdoc before writing to _any_ field.

```python
>>> # Use a {bodyid: {field: value}} dictionary to update fields
>>> clio.set_annotations({154109: {"soma_side": "LHS"}})
```

```python
>>> # Alternatively, use a dataframe dictionary to update fields
>>> import pandas as pd
>>> new_ann = pd.DataFrame([[154109, "LHS", "ascending neuron"]],
...                         columns=["bodyid", "soma_side", "class"])
>>> new_ann.head()
   bodyid soma_side             class
0  154109       LHS  ascending neuron
>>> clio.set_annotations(new_ann)
```

Please see `help(clio.set_annotations)` for details.

## References
List of API [endpoints](https://clio-store-vwzoicitea-uk.a.run.app/) for Clio.
