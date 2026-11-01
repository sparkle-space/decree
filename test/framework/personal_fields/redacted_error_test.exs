defmodule Decree.PersonalFields.RedactedErrorTest do
  use ExUnit.Case, async: true

  alias Decree.PersonalFields.RedactedError

  describe "reduce/1" do
    test "keeps an atom" do
      assert RedactedError.reduce(:email_taken) == :email_taken
    end

    test "answers an exception's type" do
      assert RedactedError.reduce(%ArgumentError{message: "x"}) == ArgumentError
    end

    test "answers :redacted for an exception map whose struct is not an atom" do
      assert RedactedError.reduce(%{__exception__: true, __struct__: "secret@example.com"}) ==
               :redacted
    end
  end
end
