# gordium
A tool for untangling the mysteries of large-scale graphs.

## Installation
```
git clone https://github.com/aplbrain/gordium.git
pip install -r requirements.txt
pip install .
```

## Usage
```
import pandas as pd
from gordium import Gordium

edgeframe = pd.read_csv('example.csv')
g = Gordium(edgeframe)
analytics = g.process()
```

## Graph Backends
Gordium defaults to using NetworkX for its graph
algorithms, but it also supports several additional
backends. To use a different backend, pass the
backend constructor into the Gordium constructor as
a `backend`.

Current backends include:
- NetworkXBackend
- IGraphBackend (optional; requires [igraph](https://igraph.org/python/))

```
import pandas as pd
from gordium import Gordium, IGraphBackend

edgeframe = pd.read_csv('example.csv')
g = Gordium(edgeframe, backend=IGraphBackend)
analytics = g.process()
```

