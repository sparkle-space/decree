defmodule Decree.PubSub.LocalPubSubTest do
  alias Decree.PubSub.{LocalPubSub, PubSubTestCase}

  use PubSubTestCase, pubsub: LocalPubSub

  setup do
    {:ok, child_spec, pubsub_meta} = LocalPubSub.child_spec(LocalPubSub, [])

    for child <- child_spec, do: start_supervised!(child)

    [pubsub_meta: pubsub_meta]
  end
end
