defmodule EventStore.Avro.AvroSerializer do
  @behaviour EventStore.Serializer

  @meta_schema "EventStore.Metadata"

  alias EventStore.Avro.AvroClient

  @impl true
  def serialize(%{__struct__: event_type} = event) do
    schema_name = schema_name(event_type)
    {:ok, payload} = AvroClient.encode(event, schema_name: schema_name, format: :registry)
    payload
  end

  def serialize(term) do
    metadata = %{data: term}
    {:ok, payload} = AvroClient.encode(metadata, schema_name: @meta_schema, format: :registry)
    payload
  end

  # TODO: pluggable schema name discovery?
  defp schema_name(event_type) when is_atom(event_type) do
    event_type
    |> Module.split()
    |> Enum.join(".")
  end

  @impl true
  def deserialize(term, _config) do
    with {:ok, payload} <- AvroClient.decode(term),
         {:ok, schema} <- AvroClient.extract_schema(term) do
      case schema do
        %{full_name: @meta_schema} ->
          payload["data"]

        %{full_name: event_type} ->
          to_struct(payload, event_type)
      end
    end
  end

  defp to_struct(payload, event_type) do
    fields = Enum.map(payload, fn {k, v} -> {String.to_existing_atom(k), v} end)

    "Elixir.#{event_type}"
    |> String.to_existing_atom()
    |> struct(fields)
  end
end
