#!/usr/bin/env python3
"""Check stable idea IDs and Markdown table structure, not adoption claims."""
from pathlib import Path
import re
import sys

path = Path(__file__).resolve().parent.parent / 'PRODUCT_IDEA_GAPS.md'
seen = set()
errors = []
section = ''
prefixes = {'ClickHouse': 'CH', 'Materialize': 'M', 'Tarantool': 'T'}
for number, line in enumerate(path.read_text().splitlines(), 1):
    if line.startswith('## '):
        section = line[3:]
    identifiers = re.findall(r'\|\s*((?:CH|M|T)-U\d{2})\s*\|', line)
    if not identifiers:
        continue
    match = re.match(r'^\| ((?:CH|M|T)-U\d{2}) \|', line)
    if not match or len(identifiers) != 1 or len(line.split('|')) != 6:
        errors.append(f'{number}: malformed idea row')
        continue
    identifier = match[1]
    if identifier in seen:
        errors.append(f'{number}: duplicate {identifier}')
    seen.add(identifier)
    prefix, index = identifier.split('-U')
    if prefix != prefixes.get(section):
        errors.append(f'{number}: {identifier} is outside its product section')
    if int(index) == 0:
        errors.append(f'{number}: invalid zero idea number')
for prefix in prefixes.values():
    missing = sorted({f'{prefix}-U{i:02}' for i in range(1, 51)} - seen)
    if missing:
        errors.append('missing original ideas: ' + ', '.join(missing))
if errors:
    print('\n'.join(errors), file=sys.stderr)
    raise SystemExit(1)
print(f'Catalog structure valid: 150 original ideas, {len(seen) - 150} additional ideas')
