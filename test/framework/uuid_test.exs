defmodule Decree.UUIDTest do
  use ExUnit.Case, async: true

  alias Decree.UUID

  describe "`canonical?/1`" do
    test "takes a generated UUID" do
      assert UUID.canonical?(UUID.uuid4())
    end

    test "takes any lowercase hex in the canonical shape" do
      assert UUID.canonical?("00000000-0000-0000-0000-000000000000")
      assert UUID.canonical?("abcdef01-2345-6789-abcd-ef0123456789")
    end

    for {name, value} <- [
          {"uppercase", "ABCDEF01-2345-6789-ABCD-EF0123456789"},
          {"mixed case", "abcdeF01-2345-6789-abcd-ef0123456789"},
          {"a letter past f", "gbcdef01-2345-6789-abcd-ef0123456789"},
          {"no hyphens", "abcdef0123456789abcdef0123456789"},
          {"a hyphen out of place", "abcdef012-345-6789-abcd-ef0123456789"},
          {"too short", "abcdef01-2345-6789-abcd-ef012345678"},
          {"too long", "abcdef01-2345-6789-abcd-ef01234567890"},
          {"empty", ""},
          {"a Stripe id", "cus_123"},
          {"nil", nil},
          {"a number", 1},
          {"an atom", :id}
        ] do
      test "refuses #{name}" do
        refute UUID.canonical?(unquote(value))
      end
    end

    test "refuses the raw 16-byte form" do
      refute UUID.canonical?(UUID.string_to_binary!(UUID.uuid4()))
    end

    test "refuses a hyphen in a hex position" do
      refute UUID.canonical?("abcdef01-2345-6789-abcd-ef01234567-9")
    end
  end
end
