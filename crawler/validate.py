#!/usr/bin/env python3
import sys
import json


def main():
    d = []
    for line in sys.stdin:
        d.append(json.loads(line))

    m = {e["id"]: e for e in d}

    mrc = 0
    for e in d:
        if subs := e.get("sub_resources"):
            nostat = False
            for sub in subs:
                if sub["id"] not in m:
                    # possibly the resource page or its reader page is broken
                    print(f"missing sub: {e['id']} -> {sub['id']}", file=sys.stderr)
                    nostat = True
                elif "__READER__" not in m[sub["id"]]:
                    # the resource has no read link
                    print(f"missing reader: {e['id']} -> {sub['id']}", file=sys.stderr)
                    mrc += 1
                    nostat = True
            if not nostat:
                print(
                    e["id"],
                    len(subs),
                    len(
                        set(
                            r if isinstance(r, str) else r["imgUrl"]
                            for sub in subs
                            for r in m[sub["id"]]["__READER__"]
                        )
                    ),
                )
    print("missing reader count:", mrc, file=sys.stderr)


if __name__ == "__main__":
    main()
