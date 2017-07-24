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

  @header ["PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0\0"]
  @trailer [<<-1 :: signed-16>>]

  defp stream_events_into_table(conn, stream_id, events) do
    try do
      Postgrex.transaction(conn, fn (conn) ->
        stream = Postgrex.stream(conn, "COPY events(event_id, stream_id, stream_version, event_type, correlation_id, causation_id, data, metadata, created_at) FROM STDIN (FORMAT BINARY)", [], max_rows: 0)

        encoded_events =
          events
          |> Stream.map(&encode/1)

        [@header, encoded_events, @trailer]
        |> Stream.concat()
        |> Enum.into(stream)
      end)
    rescue
      error -> {:error, error}
    end
  end

  defp encode(event) do
    [
      <<9>>,
      <<8 :: signed-32, event.event_id :: signed-64>>,
      <<8 :: signed-32, event.stream_id :: signed-64>>,
      <<8 :: signed-32, event.stream_version :: signed-64>>,
      <<byte_size(event.event_type) :: signed-32>>, event.event_type,
      <<byte_size(event.correlation_id) :: signed-32>>, event.correlation_id,
      <<byte_size(event.causation_id) :: signed-32>>, event.causation_id,
      <<byte_size(event.data) :: signed-32>>, event.data,
      <<byte_size(event.metadata) :: signed-32>>, event.metadata,
      encode_date(event.created_at),
    ]
  end

  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})

  defp encode_date(%NaiveDateTime{year: year, month: month, day: day, hour: hour, minute: min, second: sec, microsecond: {usec, _}}) do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @gs_epoch
    <<8 :: signed-32, secs * 1_000_000 + usec :: signed-64>>
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
