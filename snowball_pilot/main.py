import logging

import yaml
from atproto import Client

from collector import crawl, derive_is_bot, fetch_labels, reservoir_sample, save


def main():
    with open('config.yml') as f:
        cfg = yaml.safe_load(f)

    logging.basicConfig(level=getattr(logging, cfg['general']['log_level']))

    client = Client()
    auth = cfg.get('auth', {})
    if auth.get('handle') and auth.get('password'):
        client.login(auth['handle'], auth['password'])

    print('--- seed sampling')
    seeds = reservoir_sample(client, cfg)

    print(f'--- crawl from {len(seeds)} seeds')
    nodes, edges, posts = crawl(client, seeds, cfg)

    print(f'--- label queries for {len(nodes)} nodes')
    dids = list(nodes.keys())
    labels, profiles = fetch_labels(client, dids, cfg)

    is_bot_map = {did: derive_is_bot(did, labels, profiles.get(did)) for did in dids}
    counts = {'T': 0, 'F': 0, 'review': 0}
    for v in is_bot_map.values():
        counts[v] += 1
    print(f"  is_bot counts: {counts}")

    print('--- saving')
    save(nodes, edges, posts, labels, profiles, is_bot_map, cfg['general']['output_dir'])


if __name__ == '__main__':
    main()
