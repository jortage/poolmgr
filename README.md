# Jortage Proxy
A miniature S3-to-S3 proxy that accepts incoming requests, hashes the uploaded
file, stores their desired path in an MVStore key-value store, along with the
hash, and then uploads the original file to a backing S3 server named after the
hash to offer limited deduplication.

It rejects any attempt to delete files, as this is designed for Project Jortage,
a fediverse storage pool. Mastodon won't ever attempt to delete media, and
allowing people to would allow them to yank media that isn't theirs.
