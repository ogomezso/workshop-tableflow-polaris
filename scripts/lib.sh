# Helpers shared by the scripts (bash 3.2 compatible). Usage: source scripts/lib.sh

# setvar FILE NAME VALUE: set the value of the `export NAME=...` line, keeping any trailing "  # comment"
setvar() {
  local file=$1 name=$2 value=$3
  awk -v n="$name" -v v="$value" '
    index($0, "export " n "=") == 1 { c = ""; i = index($0, "  #"); if (i) c = substr($0, i); print "export " n "=" v c; next }
    { print }' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
}

# retry N SECONDS cmd...: IAM is eventually consistent (new users/roles are not valid principals for a few seconds)
retry() {
  local n=$1 wait=$2 i; shift 2
  for i in $(seq 1 "$n"); do
    "$@" && return 0
    echo "  (attempt $i/$n failed, usually IAM propagation; retrying in ${wait}s)" >&2; sleep "$wait"
  done
  return 1
}
