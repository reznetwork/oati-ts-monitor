#!/usr/bin/env python3
from daemon_runtime import build_daemon_parser, run_daemon


def main() -> int:
    parser = build_daemon_parser()
    args = parser.parse_args()
    return run_daemon(args)


if __name__ == "__main__":
    raise SystemExit(main())
