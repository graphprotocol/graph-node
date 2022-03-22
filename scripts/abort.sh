abort () {
  local ERROR_MESSAGE=$1
  echo "Release failed, error message:"
  echo $ERROR_MESSAGE
  exit 1
}
