# clio-py: Python client for clio-store

## Install
```bash
$ pip3 install git+git://github.com/schlegelp/clio-py@main
```

## Usage
Log into [Clio](https://clio.janelia.org) and grab your API token from via
settings (top right corner). Export credentials as
`CLIO_APPLICATION_CREDENTIALS`.

```python
>>> import clio
>>> client = clio.Client(dataset='VNC')
>>> clio.fetch_annotations([154109])
  hemilineage long_tract   description                  user soma_side  ... to_review  bodyid entry_nerve soma_neuromere               position
0          7B             7B in T3 LHS  xxxxxxxxxx@gmail.com       LHS  ...            154109        None             T3  [17429, 21568, 21811]
```
