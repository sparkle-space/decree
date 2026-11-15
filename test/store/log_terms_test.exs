defmodule Decree.Store.LogTermsTest do
  use ExUnit.Case, async: true

  # What the store's log lines show of an error term and of a stream id: the failure's class,
  # never a value it carries (sparkle-space/sparklespace#8352).

  alias Decree.Store.LogTerms

  @marker "log-terms-marker"

  describe "reduce/1" do
    test "an atom stays" do
      assert LogTerms.reduce(:stream_not_found) == :stream_not_found
    end

    test "a Postgrex error is its module and its code name" do
      error = %Postgrex.Error{
        message: @marker,
        postgres: %{
          code: :unique_violation,
          pg_code: "23505",
          message: @marker,
          detail: "Key (id)=(#{@marker}) already exists."
        }
      }

      assert LogTerms.reduce(error) == {Postgrex.Error, :unique_violation}
    end

    test "a Postgrex error whose code has no name is its module and its SQLSTATE" do
      error = %Postgrex.Error{postgres: %{code: nil, pg_code: "ZD001", message: @marker}}

      assert LogTerms.reduce(error) == {Postgrex.Error, "ZD001"}
    end

    test "a Postgrex error without SQLSTATE fields is its module" do
      assert LogTerms.reduce(%Postgrex.Error{message: @marker}) == Postgrex.Error
    end

    test "a connection error is its module and its reason" do
      error = DBConnection.ConnectionError.exception(message: @marker, reason: :queue_timeout)

      assert LogTerms.reduce(error) == {DBConnection.ConnectionError, :queue_timeout}
    end

    test "any other exception is its module" do
      assert LogTerms.reduce(%Jason.DecodeError{position: 0, data: @marker}) == Jason.DecodeError
      assert LogTerms.reduce(ArgumentError.exception(@marker)) == ArgumentError
    end

    test "an error tuple is reduced inside" do
      error = DBConnection.ConnectionError.exception(message: @marker, reason: :error)

      assert LogTerms.reduce({:error, error}) == {:error, {DBConnection.ConnectionError, :error}}
      assert LogTerms.reduce({:error, @marker}) == {:error, :redacted}
    end

    test "a tuple with an atom head is its head" do
      assert LogTerms.reduce({:timeout, {GenServer, :call, [@marker]}}) == {:timeout, :redacted}
    end

    test "anything else is redacted" do
      for term <- [@marker, %{id: @marker}, [@marker], {@marker, 1}, {}, 42] do
        assert LogTerms.reduce(term) == :redacted
      end
    end

    test "no reduced term carries the value" do
      for term <- [
            %Postgrex.Error{postgres: %{code: :check_violation, detail: @marker}},
            DBConnection.ConnectionError.exception(@marker),
            {:error, @marker},
            @marker
          ] do
        refute inspect(LogTerms.reduce(term)) =~ @marker
      end
    end
  end

  describe "suffix/1" do
    test "a stream id is its last eight characters" do
      assert LogTerms.suffix("0f6c1b2e-7d4a-4c1e-9b8a-5e3d2c1b0a99") == "...2c1b0a99"
    end

    test "a short id is shown whole after the ellipsis" do
      assert LogTerms.suffix("stream") == "...stream"
      assert LogTerms.suffix("") == "..."
    end

    test "the all stream is not an id and is shown whole" do
      assert LogTerms.suffix("$all") == "$all"
    end

    test "anything not a string shows no id" do
      assert LogTerms.suffix(nil) == "...(none)"
      assert LogTerms.suffix(42) == "...(none)"
    end
  end
end
