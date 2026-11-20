import sys
import toml

pyproject = toml.load(sys.argv[1])
base = pyproject["tool"]["cassandra-driver"]
base["libev-includes"] = [sys.argv[2]]
base["libev-libs"] = [sys.argv[3]]

print(toml.dumps(pyproject))
