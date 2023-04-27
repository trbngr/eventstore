defmodule EventStore.Avro.AvroSerializerTest do
  use EventStore.StorageCase

  alias EventStore.EventData
  alias EventStore.Avro.AvroClient
  alias EventStore.{Config, Storage, UUID}

  alias AvroClient.Utils.Registrar

  setup do
    reset_event_store!(AvroEventStore)
    start_supervised!({AvroEventStore, name: AvroEventStore})

    _ = Registrar.register_schema_by_name("EventStore.Metadata")
    _ = Registrar.register_schema_by_name("EventStore.Avro.AvroSerializerTest.EventOne")
    _ = Registrar.register_schema_by_name("EventStore.Avro.AvroSerializerTest.EventTwo")

    :ok
  end

  describe "EventOne" do
    defmodule EventOne do
      defstruct [:name]
    end

    test "append to and read from stream" do
      stream_uuid = UUID.uuid4()
      event = event_data(%EventOne{name: "chris"})

      assert :ok = AvroEventStore.append_to_stream(stream_uuid, 0, [event])

      {:ok, recorded_events} = AvroEventStore.read_stream_forward(stream_uuid)

      recorded_events |> IO.inspect(label: "recorded_events")
    end
  end

  describe "EventTwo" do
    defmodule EventTwo do
      defstruct [:name, :pet]
    end
  end

  defp event_data(event) do
    correlation_id = UUID.uuid4()
    causation_id = UUID.uuid4()

    %EventData{
      event_id: nil,
      correlation_id: correlation_id,
      causation_id: causation_id,
      event_type: Atom.to_string(event.__struct__),
      data: event,
      metadata: %{"user" => "user@example.com"}
    }
  end

  defp reset_event_store!(event_store) do
    config = event_store.config()
    postgrex_config = Config.default_postgrex_opts(config)

    {:ok, conn} = Postgrex.start_link(postgrex_config)

    try do
      Storage.Initializer.reset!(conn, config)
    after
      GenServer.stop(conn)
    end
  end
end
