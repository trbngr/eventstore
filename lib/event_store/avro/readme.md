You need a schema registry running to use this right now

A very simple way is run a single Redpanda node using their `rpk` tool. 

# Run Schema Regsitry

Install [`rpk`](https://docs.redpanda.com/docs/get-started/rpk-install/)

## Quick Install on MacOS

`brew install redpanda-data/tap/redpanda`

## Run

1. Start a Redpanda node

`rpk container start`

2. Find the Schema Registry Port

`docker port rp-node-0 8081`

3. Update config

Change the port in `test.exs`

```
config :eventstore, EventStore.Avro.AvroClient, registry_url: "http://localhost:52026"
```





