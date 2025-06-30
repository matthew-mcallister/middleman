middleman
=========

Middleman is an event distribution service. It connects *producers* of data
with downstream *consumers* over the Internet. You can use it to provide
scalable, real-time updates on the status of RESTful resources to users of your
API.

## What does it do?

The Middleman daemon ingests events from a data source, such as a SQL database
table or Kafka topic, and makes them available to consumers via both a
pull-based API and push-based subscribers (a.k.a. webhooks).

Compared to traditional webhook solutions, Middleman offers a few key
advantages.

1. **Well-ordered events**: Every event is automatically assigned a serial ID
   that is unique within its stream. This enables consumers to ensure events
   are processed in the order they were sent.
2. **Cooperative delivery**: By polling for events rather than receiving
   webhooks, consumers receive data as fast as they are able to process it,
   even when usage is spiky. Consumers who choose to receive webhooks can cap
   the rate at which they receive events as well.
3. **Transactional consistency**: By configuring event ingestion via SQL or
   Kafka, you benefit from atomic and consistent transactions so that events
   are reliably created if, and only if, your data changes.

## Compiling and installation

Middleman can be compiled as a Debian package for Debian and Ubuntu systems.
```bash
cargo install cargo-deb
cargo deb
dpkg -i middleman_[VERSION]_amd64.deb
```

## Building docs

Docs are built using Sphinx.
```bash
pip install sphinx sphinxcontrib.openapi sphinxcontrib.httpdomain
cd docs
make html
```
## WIP for 1.0

- Real-time data notifications
- Kafka ingestion
- Configurable retention period
