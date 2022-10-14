#!/usr/bin/env bash
set -eo pipefail

# TODO: Maybe we should revert all changes if the script fails halfway through?

abort () {
  local FAIL_MSG=$@
  echo "$FAIL_MSG"
  exit 1
}

abort_failed_to_update () {
  local FILE_NAME=$@
  abort "💀 Failed to update $FILE_NAME. Aborting."
}

assert_all_cargo_tomls_inherit_version () {
  ERROR=0
  # Get all files named Cargo.toml excluding the `integration-tests` folder and
  # the root Cargo.toml.
  CARGO_TOMLS=$(
    find . -name Cargo.toml | \
    grep -v integration-tests | \
    grep -v '\./Cargo.toml'
  )
  for CARGO_TOML in $CARGO_TOMLS
  do
    # Good files have a line that looks like `version.workspace = true`. Bad
    # files don't.
    VERSION_LINE=$(grep '^version' $CARGO_TOML)
    if [[ $VERSION_LINE != "version.workspace = true" ]]; then
      echo "⚠️  $CARGO_TOML does not inherit the crate version from the root workspace."
      ERROR=1
    fi
  done

  if [[ $ERROR == 1 ]]; then
    echo "💀 All crates must inherit the workspace's crate version."
    echo "   <https://doc.rust-lang.org/cargo/reference/workspaces.html#the-package-table>"
    abort "   Aborting."
  fi
}

get_toml_version () {
  echo $(grep '^version =' Cargo.toml | cut -d '"' -f2)
}

main () {
  CURRENT_VERSION=$(get_toml_version)
  assert_all_cargo_tomls_inherit_version

  # Increment by CLI argument (major, minor, patch)
  MAJOR=$(echo $CURRENT_VERSION | cut -d. -f1)
  MINOR=$(echo $CURRENT_VERSION | cut -d. -f2)
  PATCH=$(echo $CURRENT_VERSION | cut -d. -f3)

  case $1 in
    "major")
      let "++MAJOR"
      MINOR=0
      PATCH=0
      ;;
    "minor")
      # Preincrement to avoid early exit with set -e:
      # https://stackoverflow.com/questions/7247279/bash-set-e-and-i-0let-i-do-not-agree
      let "++MINOR"
      PATCH=0
      ;;
    "patch")
      let "++PATCH"
      ;;
    *)
      abort "💀 Bad CLI usage! Version argument should be one of: major, minor or patch"
      ;;
  esac

  echo " - Current version: \"$CURRENT_VERSION\""
  NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
  echo " - New version: \"$NEW_VERSION\""

  echo "⏳ Updating Cargo.toml..."

  # Works both on GNU and BSD sed (for macOS users)
  # See:
  # - https://unix.stackexchange.com/questions/401905/bsd-sed-vs-gnu-sed-and-i
  # - https://stackoverflow.com/a/22084103/5148606
  sed -i.backup "s/^version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/g" Cargo.toml
  rm Cargo.toml.backup

  if [[ $(git diff Cargo.toml) ]]; then
    echo "✅ Cargo.toml successfully updated."
  else
    abort_failed_to_update Cargo.toml
  fi

  echo "⏳ Updating Cargo.lock..."
  cargo check --tests
  if [[ $(git diff Cargo.lock) ]]; then
    echo "✅ Cargo.lock successfully updated."
  else
    abort_failed_to_update Cargo.lock
  fi

  echo "⏳ Committing changes..."
  git add Cargo.lock Cargo.toml
  git commit -m "Release v${NEW_VERSION}"

  echo "🎉 Done!"
}

main "$@"
