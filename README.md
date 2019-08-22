# gordium
A tool for untangling the mysteries of large-scale graphs.

## Installation
```
pip install -r requirements.txt
pip install -e .
```

## Usage
```
import pandas as pd
from gordium import Gordium

edgeframe = pd.read_csv('example.csv')
g = Gordium(edgeframe)
analytics = g.process()
```

