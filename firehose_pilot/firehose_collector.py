#!/usr/bin/env python3
"""
Bluesky AT Protocol Firehose Collector
Collects posts, reposts, replies, likes, and follow events from the public
Bluesky firehose and writes them to CSVs for network analysis.

Dependencies:
    conda env create -f firehose.yml 
    conda activate firehose_env

Usage:
    python firehose_collector.py --max-events 50000
    python firehose_collector.py --max-seconds 600

Output (in --output-dir, default: data/):
    nodes_<timestamp>.csv   — one row per unique account (DID)
    edges_<timestamp>.csv   — one row per interaction (reply/repost/like/follow)
    posts_<timestamp>.csv   — one row per post with text and reply metadata
"""

import argparse
import shutil
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from atproto import CAR, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from atproto_client.models import get_or_create
from atproto.exceptions import FirehoseError

#===========================================================================#
# Global state (accumulated across all messages)
#===========================================================================#
nodes: dict[str, dict] = {}   # did to attribute dict
edges: list[dict] = []        # list of edge records
posts: list[dict] = []        # list of post records

event_count = 0
start_time: float = 0.0
running = True


#===========================================================================#
# Record-field helpers (atproto records can be dicts or dataclass-like objects)
#===========================================================================#

def _get(record, key, default=None):
    if hasattr(record, key):
        return getattr(record, key)
    if isinstance(record, dict):
        return record.get(key, default)
    return default


def _ensure_node(did: str, ts: str):
    """Add a node with zero counters if not yet seen."""
    if did not in nodes:
        nodes[did] = {
            "did": did,
            "first_seen": ts,
            "post_count": 0,
            "reply_count": 0,
            "repost_count": 0,
            "like_count": 0,
            "follow_count": 0,
        }


def _author_from_uri(uri: str) -> str | None:
    """Extract DID from an AT-URI like at://did:plc:xxx/app.bsky.feed.post/yyy."""
    if not uri:
        return None
    parts = uri.split("/")
    return parts[2] if len(parts) >= 3 else None


#===========================================================================#
# Per-record-type handlers
#===========================================================================#

def _handle_post(op, record, author_did: str, ts: str):
    text = (_get(record, "text", "") or "").replace("\n", " ").replace("\r", " ")
    created_at = _get(record, "createdAt", ts)
    reply_block = _get(record, "reply", None)

    reply_parent_uri = None
    reply_parent_author = None
    reply_root_uri = None

    if reply_block is not None:
        parent = _get(reply_block, "parent", None)
        root = _get(reply_block, "root", None)

        if parent:
            reply_parent_uri = _get(parent, "uri", "")
            reply_parent_author = _author_from_uri(reply_parent_uri)

        if root:
            reply_root_uri = _get(root, "uri", "")

        nodes[author_did]["reply_count"] += 1

        if reply_parent_author and reply_parent_author != author_did:
            _ensure_node(reply_parent_author, ts)
            edges.append({
                "src": author_did,
                "dst": reply_parent_author,
                "type": "reply",
                "timestamp": created_at,
            })
    else:
        nodes[author_did]["post_count"] += 1

    uri = f"at://{author_did}/{op.path}"
    posts.append({
        "uri": uri,
        "author_did": author_did,
        "text": text[:500],
        "created_at": created_at,
        "reply_parent_uri": reply_parent_uri,
        "reply_parent_author": reply_parent_author,
        "reply_root_uri": reply_root_uri,
    })


def _handle_repost(op, record, author_did: str, ts: str):
    subject = _get(record, "subject", None)
    created_at = _get(record, "createdAt", ts)

    nodes[author_did]["repost_count"] += 1

    if subject:
        subject_uri = _get(subject, "uri", "")
        orig_author = _author_from_uri(subject_uri)
        if orig_author and orig_author != author_did:
            _ensure_node(orig_author, ts)
            edges.append({
                "src": author_did,
                "dst": orig_author,
                "type": "repost",
                "timestamp": created_at,
            })


def _handle_like(op, record, author_did: str, ts: str):
    subject = _get(record, "subject", None)
    created_at = _get(record, "createdAt", ts)

    nodes[author_did]["like_count"] += 1

    if subject:
        subject_uri = _get(subject, "uri", "")
        orig_author = _author_from_uri(subject_uri)
        if orig_author and orig_author != author_did:
            _ensure_node(orig_author, ts)
            edges.append({
                "src": author_did,
                "dst": orig_author,
                "type": "like",
                "timestamp": created_at,
            })


def _handle_follow(op, record, author_did: str, ts: str):
    subject_did = _get(record, "subject", None)
    created_at = _get(record, "createdAt", ts)

    nodes[author_did]["follow_count"] += 1

    if subject_did and subject_did != author_did:
        _ensure_node(subject_did, ts)
        edges.append({
            "src": author_did,
            "dst": subject_did,
            "type": "follow",
            "timestamp": created_at,
        })


#===========================================================================#
# Main message handler
#===========================================================================#

HANDLERS = {
    "app.bsky.feed.post": _handle_post,
    "app.bsky.feed.repost": _handle_repost,
    "app.bsky.feed.like": _handle_like,
    "app.bsky.graph.follow": _handle_follow,
}


def handle_message(message):
    global event_count

    try:
        commit = parse_subscribe_repos_message(message)
    except Exception:
        return

    if not hasattr(commit, "ops") or commit.ops is None:
        return

    author_did = getattr(commit, "repo", None)
    if not author_did:
        return

    # decode records from CAR blocks instead of relying on op.record
    if not hasattr(commit, "blocks") or not commit.blocks:
        event_count += 1
        return

    try:
        car = CAR.from_bytes(commit.blocks)
    except Exception:
        event_count += 1
        return

    ts = datetime.now(timezone.utc).isoformat()
    _ensure_node(author_did, ts)

    for op in commit.ops:
        if op.action != "create":
            continue

        cid = getattr(op, "cid", None)
        if cid is None:
            continue

        raw = car.blocks.get(cid)
        if raw is None:
            continue

        try:
            record = models.get_or_create(raw, strict=False)
            # alternatively
            # record = get_or_create(raw, strict=False)
        except Exception:
            continue

        record_type = _get(record, "$type", None)
        if record_type is None:
            record_type = _get(record, "py_type", None)
        if record_type is None:
            record_type = _get(record, "_type", None)

        handler = HANDLERS.get(record_type)
        if handler:
            try:
                handler(op, record, author_did, ts)
            except Exception:
                pass

    event_count += 1

    if event_count % 1000 == 0:
        elapsed = time.time() - start_time
        rate = event_count / elapsed if elapsed > 0 else 0
        print(
            f"  [{elapsed:>6.0f}s] events={event_count:>7,} "
            f"nodes={len(nodes):>6,} edges={len(edges):>7,} "
            f"posts={len(posts):>7,}  ({rate:.0f} ev/s)"
        )


#===========================================================================#
# Save
#===========================================================================#

def save_data(output_dir: Path) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    nodes_path = output_dir / f"nodes_{ts}.csv"
    edges_path = output_dir / f"edges_{ts}.csv"
    posts_path = output_dir / f"posts_{ts}.csv"

    pd.DataFrame(nodes.values()).to_csv(nodes_path, index=False, lineterminator="\n")
    pd.DataFrame(edges).to_csv(edges_path, index=False, lineterminator="\n")
    pd.DataFrame(posts).to_csv(posts_path, index=False, lineterminator="\n")

    # shutil.copy2 instead of symlinks
    for stem, path in [("nodes", nodes_path), ("edges", edges_path), ("posts", posts_path)]:
        latest = output_dir / f"{stem}_latest.csv"
        shutil.copy2(path, latest)

    print(f"\n{'='*52}")
    print(f"  Nodes saved : {len(nodes):>7,}  →  {nodes_path}")
    print(f"  Edges saved : {len(edges):>7,}  →  {edges_path}")
    print(f"  Posts saved : {len(posts):>7,}  →  {posts_path}")
    print(f"\n{'='*52}")

    return nodes_path, edges_path, posts_path


#===========================================================================#
# CLI
#===========================================================================#

def main():
    global start_time, running

    parser = argparse.ArgumentParser(
        description="Collect Bluesky firehose data for network analysis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--max-events", type=int, default=50_000,
                        help="Stop after this many commit events")
    parser.add_argument("--max-seconds", type=int, default=3600,
                        help="Stop after this many seconds")
    parser.add_argument("--output-dir", type=str, default="data",
                        help="Directory to write output CSVs")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    # Handle shutdown on Ctrl-C or SIGTERM
    def _shutdown(sig, frame):
        global running
        print("\n[signal] Stopping collection and saving data …")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print("Bluesky Firehose Collector")
    print(f"  Max events  : {args.max_events:,}")
    print(f"  Max seconds : {args.max_seconds:,}")
    print(f"  Output dir  : {output_dir}")
    print("  Press Ctrl-C to stop early.\n")

    client = FirehoseSubscribeReposClient()
    start_time = time.time()

    def on_message(message):
        global running

        if not running:
            client.stop()
            return

        elapsed = time.time() - start_time
        if event_count >= args.max_events or elapsed >= args.max_seconds:
            running = False
            client.stop()
            return

        handle_message(message)

    try:
        client.start(on_message)
    except FirehoseError as exc:
        if running:  # unexpected disconnection
            print(f"[FirehoseError] {exc}")
    except Exception as exc:
        if running:
            print(f"[Error] {exc}")

    elapsed = time.time() - start_time
    print(f"\nCollection finished: {event_count:,} events in {elapsed:.1f}s")
    save_data(output_dir)


if __name__ == "__main__":
    main()