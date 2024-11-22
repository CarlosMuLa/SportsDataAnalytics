import os
from dgraph_utils import main

if __name__ == "__main__":
    os.environ["DGRAPH_HOST"] = "localhost"
    os.environ["DGRAPH_PORT"] = "9080"
    main()
