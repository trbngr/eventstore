defmodule EventStore.Avro.Supervisor do
  use Supervisor

  def start_link(_args) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      EventStore.Avro.AvroClient
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
