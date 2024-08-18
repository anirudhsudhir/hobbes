# Kvs

A [Bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)-like
log-structured key-value store with an in-memory index, written in Rust

## Technical details

### Serialization format

Initially, the commands were serialized to the on-disk log in the [BSON](https://bsonspec.org)
data format which was later changed to the [MessagePack](https://msgpack.org) format.

- The MessagePack format is designed to be compact and size-efficient unlike BSON,
  which trades size for a structure that facilitates document-oriented operations,
  which are not required by the on-disk log.

- The MessagePack format is designed to be performant. While BSON has generally
  good performance, the complexity and metadata might affect serialization and
  deserialization speeds.

### Deserializing from buffer

The on-disk commands can be directly deserialized using serde from an I/O
stream without using a buffer, which reduces memory usage.
However, a BufReader is used to reduce the number of system calls made for every
read to the log during the in-memory index creation
