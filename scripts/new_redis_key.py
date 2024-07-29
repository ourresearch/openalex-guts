from argparse import ArgumentParser

from redis import Redis

from app import REDIS_QUEUE_URL

REDIS = Redis.from_url(REDIS_QUEUE_URL)
BATCH_SIZE = 100_000

methods_map = {'set': REDIS.sadd,
               'sorted_set': REDIS.zadd}


def put_batch(key, key_type, *args):
    method = methods_map[key_type]
    method(key, *args)


def chunks(file, chunk_size):
    """Yield successive chunk_size-sized chunks from a file-like object."""
    chunk = []
    for line in file:
        chunk.append(line.strip())
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def from_txt(fname, key, key_type):
    with open(fname, 'r') as f:
        count = 0
        for chunk in chunks(f, BATCH_SIZE):
            put_batch(key, key_type, *chunk)
            count += len(chunk)
            print(f'Added {count} members to {key_type} {key}')


def parse_args():
    parser = ArgumentParser(
        description="Create new redis key and populate")
    parser.add_argument('--key', '-k', type=str, required=True,
                        help="New redis key name")
    parser.add_argument('--type', '-t', type=str, choices=['set', 'sorted_set'],
                        required=True,
                        help="Type of Redis data structure (set or sorted_set)")
    parser.add_argument('--filename', '-f', type=str,
                        required=True,
                        help="Path to the text file containing work ids to populate key")
    return parser.parse_args()


def main():
    args = parse_args()
    from_txt(args.filename, args.key, args.type)


if __name__ == '__main__':
    main()
