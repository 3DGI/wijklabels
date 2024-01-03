set dotenv-load := true
datadir := "tests" / "data"

# Download the test data
download:
    #!/usr/bin/env bash
    set -euxo pipefail
    # Download the reconstructed features
    tiles=("9/316/552" "9/316/556")
    for tile_id in "${tiles[@]}" ;
    do
      filename="${tile_id//'/'/'-'}.city.json.gz"
      wget "https://data.3dbag.nl/cityjson/v20231008/tiles/$tile_id/$filename" -P "tests/data"
      gunzip "tests/data/$filename"
    done

# Render the report with Quarto. Requires that a Python3.12 virtualenv is set up in the 'venv_312' directory.
render:
    #!/usr/bin/env bash
    set -euxo pipefail
    export QUARTO_PYTHON="$(pwd)/venv_312/bin/python3.12"
    source "$(pwd)/venv_312/bin/activate"
    quarto render ./report --execute

# Preview the report with Quarto. Requires that a Python3.12 virtualenv is set up in the 'venv_312' directory.
preview $QUARTO_PYTHON="../venv_312/bin/python3.12":
    #!/usr/bin/env bash
    set -euxo pipefail
    export QUARTO_PYTHON="$(pwd)/venv_312/bin/python3.12"
    source "$(pwd)/venv_312/bin/activate"
    quarto preview ./report/report.qmd