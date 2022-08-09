# Get all files named 'Cargo.toml' in the `graph-node` directory, excluding the `integration-tests` folder.
get_all_toml_files () {
  echo "$(find . -name Cargo.toml | grep -v integration-tests)"
}

get_all_toml_versions () {
  local FILE_NAMES=$@
  echo $(
    echo $FILE_NAMES | \
    # Read all 'Cargo.toml' file contents.
    xargs cat | \
    # Get the 'version' key of the TOML, eg: version = "0.25.2"
    grep '^version = ' | \
    # Remove the '"' enclosing the version, eg: "0.25.2"
    tr -d '"' | \
    # Get only the version number, eg: 0.25.2
    awk '{print $3}' \
  )
}
