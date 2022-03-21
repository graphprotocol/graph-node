#!/bin/bash

source 'scripts/abort.sh'
source 'scripts/toml-utils.sh'

ALL_TOML_FILE_NAMES=$(get_all_toml_files)
ALL_TOML_VERSIONS=$(get_all_toml_versions $ALL_TOML_FILE_NAMES)

# Asserts all .toml versions are currently the same, otherwise abort.
if ./scripts/lines-unique.sh $ALL_TOML_VERSIONS; then
  CURRENT_VERSION=$(echo $ALL_TOML_VERSIONS | awk '{print $1}')
  echo "Current version: \"$CURRENT_VERSION\""
else
  abort "Some Cargo.toml files have different versions than others, make sure they're all the same before creating a new release"
fi


# Increment by CLI argument (major, minor, patch)
MAJOR=$(echo $CURRENT_VERSION | cut -d. -f1)
MINOR=$(echo $CURRENT_VERSION | cut -d. -f2)
PATCH=$(echo $CURRENT_VERSION | cut -d. -f3)

case $1 in
  "major")
    let "MAJOR++"
    MINOR=0
    PATCH=0
    ;;
  "minor")
    let "MINOR++"
    PATCH=0
    ;;
  "patch")
    let "PATCH++"
    ;;
  *)
    abort "Version argument should be one of: major, minor or patch"
    ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
echo "New version: \"$NEW_VERSION\""


# Replace on all .toml files
echo "Changing $(echo $ALL_TOML_VERSIONS | tr " " "\n" | wc -l | awk '{$1=$1;print}') toml files"

# MacOS/OSX unfortunately doesn't have the same API for `sed`, so we're
# using `perl` instead since it's installed by default in all Mac machines.
#
# For mor info: https://stackoverflow.com/questions/4247068/sed-command-with-i-option-failing-on-mac-but-works-on-linux
if [[ "$OSTYPE" == "darwin"* ]]; then
    perl -i -pe"s/^version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" $ALL_TOML_FILE_NAMES
# Default, for decent OSs (eg: GNU-Linux)
else
    sed -i "s/^version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" $ALL_TOML_FILE_NAMES
fi


# Assert all the new .toml versions are the same, otherwise abort
UPDATED_TOML_VERSIONS=$(get_all_toml_versions $ALL_TOML_FILE_NAMES)
if ./scripts/lines-unique.sh $UPDATED_TOML_VERSIONS; then
  echo "Toml files are still consistent in their version after the update"
else
  abort "Something went wrong with the version replacement and the new version isn't the same across the Cargo.toml files"
fi


# Assert there was a git diff in the changed files, otherwise abort
if [[ $(git diff $ALL_TOML_FILE_NAMES) ]]; then
  :
else
  abort "Somehow the toml files didn't get changed"
fi


echo "Updating Cargo.lock file"
cargo check --tests


# Assert .lock file changed the versions, otherwise abort
if [[ $(git diff Cargo.lock) ]]; then
    echo "Cargo.lock file updated"
else
    abort "There was no change in the Cargo.lock file, something went wrong with updating the crates versions"
fi


echo "Updating version of the Cargo.{lock, toml} files succeded!"

git add Cargo.lock $ALL_TOML_FILE_NAMES
git commit -m "Release ${NEW_VERSION}"
