#!/usr/bin/env python3
"""QA-05 bounded read-only profile of pinned historical Gold. Never start/build services."""
import argparse
from pathlib import Path
import re
import sys
from qa05_profile import Runtime, run

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, default=ROOT/'config/qa/resource-profile-v1.json')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    try:
        result = run(ROOT, args.config, args.output, Runtime())
        print('event=qa05_profile_accepted queries=' + str(len(result['queries']))
              + ' seconds=' + str(round(result['whole_suite_seconds'], 3)))
        return 0
    except Exception as exc:
        reason = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r'QA05_[A-Z0-9_]+', str(exc)) else 'QA05_FAILED_NO_RETRY'
        print('event=qa05_profile_failed reason=' + reason + ' no_auto_retry=true', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
