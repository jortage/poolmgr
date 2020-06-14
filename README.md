# Jortage Storage Pool Proxy
A miniature S3-to-S3 proxy that accepts incoming requests, hashes the uploaded
file, stores their desired path in a MariaDB database, along with the hash, and 
then uploads the original file to a backing S3 server named after the
hash to offer file-level deduplication. Also implements an acceleration server
called Rivet that allows users to request media be added to the pool by URL
without a potentially wasteful download/upload.
[Learn more](https://jortage.com).
