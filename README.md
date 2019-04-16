# gordium
A tool for untangling the mysteries of large-scale graphs.

## Installation
```
pip install -r requirements.txt
pip install -e .
```

## Usage
```
from getpass import getpass
from gordium import Gordium

# Enter Neo4j Password
pswd = getpass()

# Connect to Neo4j and Run
g = Gordium('bolt://gordium:7687', password=pswd)
analytics = g.process()
```

