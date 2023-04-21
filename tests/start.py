import sys
from airflow.__main__ import main

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.argv.append("standalone")

    main()
