defmodule Decree.Decision.DispatchLogTermsTest do
  use Decree.DecisionStoreCase

  # A raise in a decision's dispatch is logged by the exception's module and a scrubbed
  # stacktrace, never the exception's message, which can hold what the decision read or was
  # sent (sparkle-space/sparklespace#8358). The caller's answer is unchanged.

  import ExUnit.CaptureLog

  @marker "decision-log-terms-marker"

  test "a raise in `query` is logged by the exception's module" do
    command = scripted(query: fn _command, _context -> raise "query raised " <> @marker end)

    log =
      capture_log(fn ->
        assert {:error, %RuntimeError{message: "query raised " <> @marker}} =
                 App.dispatch(command)
      end)

    assert log =~ "Decree.DecisionFixtures.ScriptedDecision raised: ** (RuntimeError)"
    assert log =~ "dispatch_log_terms_test.exs"

    refute log =~ @marker
    refute log =~ command.claim_id
  end
end
