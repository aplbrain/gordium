# gordium
A tool for untangling the mysteries of large-scale graphs.

## Installation
```
git clone https://github.com/aplbrain/gordium.git
pip install -r requirements.txt
pip install .
```

## Data
Gordium expects to receive a pandas DataFrame representing an edge list. It will handle two columns containing the ids of source nodes and target nodes respectively. By default, these column names are expected to be "source" and "target", but they can be specified by passing values for `src_label` and `tgt_label` to the Gordium constructor. At present, all edges are treated as directed, multiedges are ignored, edge attributes are ignored, and selfloops are permitted. A typical CSV representing the edgeframe of a directed 3-cycle is presented below.
```
source,target
0,1
1,2
2,0
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

