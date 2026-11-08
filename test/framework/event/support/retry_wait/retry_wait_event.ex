defmodule Decree.Event.RetryWaitEvent do
  @moduledoc false

  # `reply_to` is a `:erlang.pid_to_list/1` string; `fail_times` is how many attempts fail
  # before one succeeds; `delay` is the retry delay `error/3` answers, in milliseconds.
  @derive Jason.Encoder
  defstruct [:id, :reply_to, fail_times: 0, delay: 0]
end
