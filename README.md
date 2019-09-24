# ipfs-pump

`ipfs-pump` is a command line tool to copy data between IPFS nodes, cluster or storage.

It support multiple interfaces:
- the IPFS API
- direct access to a FlatFS datastore
- direct access to a S3 datastore
- a file with a list of CID

# Concepts

An `Enumerator` is a source that will enumerate the list of existing blocks.

A `Collector` is a source that where to retrieve block's data.

A `Drain` is a destination where to push blocks.

Note: even though you will likely want the `Enumerator` and the `Collector` to be the same source, they don't necessarily have to.

# Install

```
go get -u github.com/INFURA/ipfs-pump/cmd/ipfs-pump
```

# Usage and examples

You need to provide as arguments the three types of `Enumerator`, `Collector` and `Drain`, as well as the corresponding configuration flags.

Copy between two live nodes using the API:

```
ipfs-pump \
    apipin --enum-api-pin-url=127.0.0.1:5001 \
    api --coll-api-url=127.0.0.1:5001 \
    api --drain-api-url=127.0.0.1:5002 \
    --worker=10
```

Copy from a FlatFS storage to a S3 storage:

```
ipfs-pump \
    flatfs --enum-flatfs-path=~/.ipfs/blocks \
    flatfs --coll-flatfs-path=~/.ipfs/blocks \
    s3 --drain-s3-bucket=$BUCKET_NAME --drain-s3-region=$REGION \
    --worker=50
```

# Parallel processing

Using the `--worker` flag you can enable parallel processing and greatly increase the throughput.
