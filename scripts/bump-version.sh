#!/usr/bin/env sh

set -eu

if [ $# -ne 1 ]; then
	printf 'usage: %s <version>\n' "$0" >&2
	exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
version=${1#v}

cd "$repo_root"

python3 - "$version" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]
path = Path("internal/version/version.go")
text = path.read_text(encoding="utf-8")
new_text, count = re.subn(r'var Current = "v[^"]+"', f'var Current = "v{version}"', text, count=1)
if count != 1:
    raise SystemExit("failed to update internal/version/version.go")
path.write_text(new_text, encoding="utf-8")
print(f"Updated internal/version/version.go to v{version}")
PY

if git rev-parse --verify HEAD >/dev/null 2>&1; then
	git tag -f "v${version}"
	printf 'Created/updated git tag v%s\n' "$version"
else
	printf 'Skipped git tag creation (no commit yet to tag)\n'
fi