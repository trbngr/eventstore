defmodule EventStore.Storage.Appender do
  @moduledoc """
  Append-only storage of events to a stream
  """

  require Logger

  alias EventStore.Sql.Statements

  @doc """
  Append the given list of events to the given stream.

  Returns `{:ok, count}` on success, where count indicates the number of appended events.
  """
  def append(conn, stream_id, events) do
    conn
    |> stream_events_into_table(stream_id, events)
    |> handle_response(stream_id, events)
  end

  defp stream_events_into_table(conn, stream_id, events) do
    try do
      Postgrex.transaction(conn, fn (conn) ->
        stream = Postgrex.stream(conn, "COPY events(event_id, stream_id, stream_version, event_type, correlation_id, causation_id, data, metadata, created_at) FROM STDIN encoding 'UTF8'", [], max_rows: 0)

        events
        |> Stream.map(&to_row/1)
        |> Enum.into(stream)
      end)
    rescue
      error -> {:error, error}
    end
  end

  defp to_row(event) do
    [
      Integer.to_string(event.event_id), ?\t,
      Integer.to_string(event.stream_id), ?\t,
      Integer.to_string(event.stream_version), ?\t,
      to_string(event.event_type), ?\t,
      to_string(event.correlation_id), ?\t,
      to_string(event.causation_id), ?\t,
      to_string(event.data), ?\t,
      to_string(event.metadata), ?\t,
      to_string(event.created_at), ?\n,
    ]
  end

  defp handle_response({:ok, %Postgrex.Stream{}}, stream_id, events) do
    event_count = length(events)
      _ = Logger.info(fn -> "appended #{event_count} event(s) to stream id #{stream_id}" end)
    {:ok, event_count}
  end

  defp handle_response({:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation, message: message}}}, stream_id, _events) do
    _ = Logger.warn(fn -> "failed to append events to stream id #{stream_id} due to: #{inspect message}" end)
    {:error, :stream_not_found}
  end

  defp handle_response({:error, %Postgrex.Error{postgres: %{code: :unique_violation, message: message}}}, stream_id, _events) do
    _ = Logger.warn(fn -> "failed to append events to stream id #{stream_id} due to: #{inspect message}" end)
    {:error, :wrong_expected_version}
  end

  defp handle_response({:error, reason}, stream_id, _events) do
    _ = Logger.warn(fn -> "failed to append events to stream id #{stream_id} due to: #{inspect reason}" end)
    {:error, reason}
  end
end
