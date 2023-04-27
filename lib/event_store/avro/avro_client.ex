defmodule EventStore.Avro.AvroClient do
  use Avrora.Client,
    otp_app: :eventstore,
    names_cache_ttl: :timer.minutes(5),
    schemas_path: "./priv/avro",
    registry_schemas_autoreg: true
end
