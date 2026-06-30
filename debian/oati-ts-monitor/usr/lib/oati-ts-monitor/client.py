#!/usr/bin/env python3
from client_runtime import build_client_parser, run_client


def main() -> int:
    parser = build_client_parser()
    args = parser.parse_args()
    return run_client(args)


if __name__ == "__main__":
    raise SystemExit(main())
