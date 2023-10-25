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
