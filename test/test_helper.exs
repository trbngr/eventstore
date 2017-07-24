ExUnit.start(exclude: [:skip])

Mix.Task.run("event_store.create", ~w(--quiet))
