import random
import shutil
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import networkx as nx
import pandas as pd


# helpers

def _backoff(attempt, base):
    """Exponential sleep between retry attempts, used by _paginate."""
    time.sleep(base * (2 ** attempt))


def _paginate(method, key, params, max_results, backoff_base, max_retries, rate_limit):
    """
    Calls any cursor-paginated AT Protocol method until results are exhausted 
    or max_results is hit, retrying on failure. 
    Used by reservoir_sample, crawl, and fetch_labels.
    """
    results = []
    cursor = None
    for attempt in range(max_retries):
        try:
            while True:
                p = dict(params)
                if cursor:
                    p['cursor'] = cursor
                resp = method(**p)
                time.sleep(rate_limit)
                batch = getattr(resp, key, []) or []
                results.extend(batch)
                cursor = getattr(resp, 'cursor', None)
                if not cursor or (max_results and len(results) >= max_results):
                    break
            return results[:max_results] if max_results else results
        except Exception:
            if attempt < max_retries - 1:
                _backoff(attempt, backoff_base)
            else:
                return results
    return results


# seed sampling

def reservoir_sample(client, cfg):
    """
    Pages through listRepos and reservoir-samples a fixed number of seed DIDs, 
    skipping inactive accounts. Feeds into crawl.
    """
    s = cfg['seed']
    sample_size = s['sample_size']
    max_scan = s['max_repos_to_scan']
    random.seed(s['random_seed'])
    bbase = cfg['general']['rate_limit_backoff_base']
    max_ret = cfg['general']['rate_limit_max_retries']

    reservoir = []
    n_seen = 0
    cursor = None

    while n_seen < max_scan:
        resp = None
        for attempt in range(max_ret):
            try:
                resp = client.com.atproto.sync.list_repos(limit=1000, cursor=cursor)
                break
            except Exception:
                if attempt < max_ret - 1:
                    _backoff(attempt, bbase)

        if resp is None:
            break

        repos = resp.repos or []
        if not repos:
            break

        for repo in repos:
            # skip deactivated or suspended repos
            if getattr(repo, 'active', True) is False:
                continue
            n_seen += 1
            # standard reservoir; fill until full then replace at random
            if len(reservoir) < sample_size:
                reservoir.append(repo.did)
            else:
                j = random.randint(0, n_seen - 1)
                if j < sample_size:
                    reservoir[j] = repo.did
            if n_seen >= max_scan:
                break

        if n_seen % 10000 == 0:
            print(f"  scanned {n_seen} repos  reservoir={len(reservoir)}")

        cursor = getattr(resp, 'cursor', None)
        if not cursor:
            break

    print(f"seed sampling done: {len(reservoir)} seeds from {n_seen} repos")
    return reservoir


# graph component check

def check_components(edges, component_type, min_size):
    """
    Builds a directed graph from the current edge list 
    and returns the count and membership of components at or above min_size. 
    Called periodically inside crawl.
    """
    G = nx.DiGraph()
    for e in edges:
        G.add_edge(e['src'], e['dst'])
    if component_type == 'scc':
        # scc
        components = list(nx.strongly_connected_components(G))
    else:
        # wcc
        components = list(nx.weakly_connected_components(G))
    large = [c for c in components if len(c) >= min_size]
    return len(large), large


# BFS crawl

def crawl(client, seeds, cfg):
    """
    Expands outward from seed DIDs via follows, followers, 
    and feed queries, stopping when the component target is met 
    or limits are hit. Returns nodes, edges, and posts for fetch_labels and save.
    """
    bbase = cfg['general']['rate_limit_backoff_base']
    max_ret = cfg['general']['rate_limit_max_retries']
    max_hops = cfg['crawl']['max_hops']
    max_nodes = cfg['crawl']['max_total_nodes']
    check_interval = cfg['crawl']['component_check_interval']
    max_nbr = cfg['crawl']['max_neighbors_per_node']
    target = cfg['crawl']['target_num_components']
    min_size = cfg['crawl']['min_component_size']
    ctype = cfg['crawl']['component_type']

    nodes = {}
    edges = []
    posts = []

    def ts():
        return datetime.now(timezone.utc).isoformat()

    def ensure(did, t):
        # add node with zero counters if not yet seen
        if did not in nodes:
            nodes[did] = {
                'did': did, 'first_seen': t,
                'post_count': 0, 'reply_count': 0,
                'repost_count': 0, 'follow_count': 0,
            }

    def author_from_uri(uri):
        # e.g. at://did:plc:xxx/collection/rkey
        if not uri:
            return None
        parts = uri.split('/')
        return parts[2] if len(parts) >= 3 else None

    frontier = deque((did, 0) for did in seeds)
    visited = set(seeds)
    for did in seeds:
        ensure(did, ts())

    nodes_since_check = 0

    while frontier:
        did, hop = frontier.popleft()
        t = ts()

        if len(nodes) >= max_nodes:
            print("max_nodes reached")
            break

        def add_neighbor(nbr, edge_dict):
            # record the edge and queue the neighbor if not yet visited
            nonlocal nodes_since_check
            ensure(nbr, t)
            edges.append(edge_dict)
            if nbr not in visited:
                visited.add(nbr)
                if hop + 1 < max_hops:
                    frontier.append((nbr, hop + 1))
                nodes_since_check += 1

        # outgoing follows
        follows = _paginate(
            client.app.bsky.graph.get_follows, 'follows',
            {'actor': did, 'limit': 100}, max_nbr, bbase, max_ret
        )
        for f in follows:
            nodes[did]['follow_count'] += 1
            add_neighbor(f.did, {'src': did, 'dst': f.did, 'type': 'follow', 'timestamp': t})

        # incoming followers
        followers = _paginate(
            client.app.bsky.graph.get_followers, 'followers',
            {'actor': did, 'limit': 100}, max_nbr, bbase, max_ret
        )
        for f in followers:
            add_neighbor(f.did, {'src': f.did, 'dst': did, 'type': 'follow', 'timestamp': t})

        # feed for reply and repost edges
        feed = _paginate(
            client.app.bsky.feed.get_author_feed, 'feed',
            {'actor': did, 'limit': 100}, None, bbase, max_ret
        )
        for item in feed:
            post = item.post
            record = post.record
            created_at = getattr(record, 'created_at', t)
            text = (getattr(record, 'text', '') or '').replace('\n', ' ')[:500]

            reply = getattr(record, 'reply', None)
            if reply:
                nodes[did]['reply_count'] += 1
                parent_uri = getattr(getattr(reply, 'parent', None), 'uri', '')
                parent_did = author_from_uri(parent_uri)
                if parent_did and parent_did != did:
                    add_neighbor(parent_did, {
                        'src': did, 'dst': parent_did,
                        'type': 'reply', 'timestamp': created_at
                    })
            else:
                nodes[did]['post_count'] += 1

            posts.append({
                'uri': f"at://{did}/{getattr(post, 'cid', '')}",
                'author_did': did, 'text': text, 'created_at': created_at,
            })

            # reason field is only present on reposts
            reason = getattr(item, 'reason', None)
            if reason:
                rtype = getattr(reason, 'py_type', '') or getattr(reason, '$type', '')
                if 'repost' in str(rtype).lower():
                    orig_did = post.author.did
                    nodes[did]['repost_count'] += 1
                    if orig_did and orig_did != did:
                        add_neighbor(orig_did, {
                            'src': did, 'dst': orig_did,
                            'type': 'repost', 'timestamp': t
                        })

        # check component criterion every check_interval new nodes
        if nodes_since_check >= check_interval:
            n_large, _ = check_components(edges, ctype, min_size)
            nodes_since_check = 0
            print(f"  nodes={len(nodes)}  edges={len(edges)}  large_components={n_large}")
            if n_large >= target:
                print("component target reached")
                break

    return nodes, edges, posts


# label queries and bot derivation

# labels that unambiguously indicate a bot or automated account
BOT_DEFINITE = {'bot', 'automated'}
# labels that suggest inauthentic behavior but need downstream review
BOT_REVIEW = {'spam', 'impersonation', 'misleading', 'scam'}


def fetch_labels(client, dids, cfg):
    """
    Queries all configured labelers for every node DID 
    and pulls profile metadata including self-labels. 
    Returns raw label records and a profiles dict, 
    both consumed by derive_is_bot and save.
    """
    labeler_dids = cfg['labels']['labeler_dids']
    batch_size = cfg['labels']['batch_size']
    bbase = cfg['general']['rate_limit_backoff_base']
    max_ret = cfg['general']['rate_limit_max_retries']

    all_labels = []
    profiles = {}

    # external labels from bluesky moderation, aegis, and any other configured labelers
    for i in range(0, len(dids), batch_size):
        batch = dids[i:i + batch_size]
        for attempt in range(max_ret):
            try:
                resp = client.com.atproto.label.query_labels(
                    uri_patterns=batch, sources=labeler_dids, limit=250
                )
                for lbl in (resp.labels or []):
                    all_labels.append({
                        'did': lbl.uri,
                        'label': lbl.val,
                        'labeler_did': lbl.src,
                        'negated': getattr(lbl, 'neg', False),  # negated means label was retracted
                        'timestamp': lbl.cts,
                    })
                break
            except Exception:
                if attempt < max_ret - 1:
                    _backoff(attempt, bbase)

    # profile data and self-labels
    for i in range(0, len(dids), batch_size):
        batch = dids[i:i + batch_size]
        for attempt in range(max_ret):
            try:
                resp = client.app.bsky.actor.get_profiles(actors=batch)
                for p in (resp.profiles or []):
                    self_labels = [
                        l.val for l in (p.labels or [])
                        if getattr(l, 'src', None) == p.did
                        and not getattr(l, 'neg', False)
                    ]
                    profiles[p.did] = {
                        'did': p.did,
                        'handle': getattr(p, 'handle', '') or '',
                        'display_name': getattr(p, 'display_name', '') or '',
                        'followers_count': getattr(p, 'followers_count', 0) or 0,
                        'follows_count': getattr(p, 'follows_count', 0) or 0,
                        'posts_count': getattr(p, 'posts_count', 0) or 0,
                        'self_labels': '|'.join(self_labels),
                        'created_at': getattr(p, 'created_at', '') or '',
                    }
                break
            except Exception:
                if attempt < max_ret - 1:
                    _backoff(attempt, bbase)

    return all_labels, profiles


def derive_is_bot(did, all_labels, profile):
    """
    Collapses all label and self-label signals for a DID into T, F, or review. 
    Called once per node in main after fetch_labels.
    """
    vals = {
        l['label'].lower() for l in all_labels
        if l['did'] == did and not l['negated']
    }
    if profile and profile.get('self_labels'):
        vals |= {v for v in profile['self_labels'].lower().split('|') if v}
    if vals & BOT_DEFINITE:
        return 'T'
    if vals & BOT_REVIEW:
        return 'review'
    return 'F'


# save

def save(nodes, edges, posts, labels, profiles, is_bot_map, output_dir):
    """
    Merges profile and bot data into node rows and writes timestamped 
    CSVs for nodes, edges, posts, and labels, copying each to a latest alias.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    # merge profile fields and is_bot into each node row before writing
    node_rows = list(nodes.values())
    for row in node_rows:
        did = row['did']
        p = profiles.get(did, {})
        row.update({
            'handle': p.get('handle', ''),
            'display_name': p.get('display_name', ''),
            'followers_count': p.get('followers_count', ''),
            'follows_count': p.get('follows_count', ''),
            'posts_count': p.get('posts_count', ''),
            'self_labels': p.get('self_labels', ''),
            'account_created_at': p.get('created_at', ''),
            'is_bot': is_bot_map.get(did, 'F'),
        })

    for name, data in [('nodes', node_rows), ('edges', edges), ('posts', posts), ('labels', labels)]:
        p = output_dir / f'{name}_{ts}.csv'
        pd.DataFrame(data).to_csv(p, index=False, lineterminator='\n')
        shutil.copy2(p, output_dir / f'{name}_latest.csv')
        print(f"  {name}: {len(data)} rows  ->  {p}")
