defmodule Decree.PersonalFields.LookupLogTest do
  use ExUnit.Case, async: false

  # A subject with no key is logged by the last eight characters of its id, never the whole
  # id, which can be a user's own; the telemetry event still carries the whole id for the
  # runbook (sparkle-space/sparklespace#8352).

  import ExUnit.CaptureLog

  alias Decree.PersonalFields.Lookup

  @subject_id "5b0f7c2a-9e4d-4a1b-8c3e-2f6d1a0b9c8e"
  @key_unknown [:decree, :personal_field, :key_unknown]

  test "the warning names the subject by its suffix and the telemetry by its id" do
    test = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        @key_unknown,
        fn _event, _measurements, metadata, _config -> send(test, {:key_unknown, metadata}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    log = capture_log([level: :debug], fn -> :unknown = Lookup.unknown(:app, @subject_id) end)

    assert log =~ "no personal field key for subject ...1a0b9c8e"
    refute log =~ @subject_id

    assert_received {:key_unknown, %{application: :app, subject_id: @subject_id}}
  end

  test "a subject id that is not UTF-8 answers unknown and logs a marker, not its bytes" do
    subject_id = "fake-subject-marker" <> <<0xC2, 0xAE, 0xF5>>

    log = capture_log([level: :debug], fn -> :unknown = Lookup.unknown(:app, subject_id) end)

    assert log =~ "no personal field key for subject ...(not utf-8)\n"
    refute log =~ "marker"
  end
end
