defmodule Decree.PersonalFields.EnvelopeTest do
  use ExUnit.Case, async: true

  alias Decree.PersonalFields.Envelope

  @key :binary.copy(<<7>>, 32)
  @other_key :binary.copy(<<8>>, 32)
  @location {"stream-1", "Event", "email"}

  defp sealed(value \\ "alice@secret.example") do
    {:ok, envelope} = Envelope.encrypt(value, {"key-1", @key}, "subject-1", @location)

    envelope
  end

  defp open(envelope, key \\ @key, location \\ @location) do
    {:ok, parsed} = Envelope.parse(envelope)

    Envelope.decrypt(parsed, key, location)
  end

  defp atomized(envelope),
    do: Map.new(envelope, fn {key, value} -> {String.to_atom(key), value} end)

  test "is exactly the five parts, as a JSON object" do
    envelope = sealed()

    assert envelope |> Map.keys() |> Enum.sort() == ["$pf", "c", "k", "n", "s"]
    assert %{"$pf" => 1, "k" => "key-1", "s" => "subject-1"} = envelope
    assert byte_size(Base.decode64!(envelope["n"])) == 12

    # A JSON string of the value's own 22 bytes, then the 16-byte tag.
    assert byte_size(Base.decode64!(envelope["c"])) == 22 + 16

    assert {:ok, _json} = Jason.encode(envelope)
    refute inspect(envelope) =~ "alice"
  end

  test "takes a fresh nonce each time" do
    assert sealed()["n"] != sealed()["n"]
    assert sealed()["c"] != sealed()["c"]
  end

  test "keeps the type of a value that is not a string" do
    for value <- [12, 1.5, true, ["a", "b"], %{"a" => 1}] do
      assert open(sealed(value)) == {:ok, value}
    end
  end

  test "a value with no JSON form is an error that carries nothing" do
    for value <- [{:a, "tuple"}, <<0xFF>>, self()] do
      assert Envelope.encrypt(value, {"key-1", @key}, "subject-1", @location) ==
               {:error, :personal_field_not_encodable}
    end
  end

  test "reads atom and string keys" do
    assert open(sealed()) == {:ok, "alice@secret.example"}
    assert open(atomized(sealed())) == {:ok, "alice@secret.example"}

    assert Envelope.envelope?(sealed())
    assert Envelope.envelope?(atomized(sealed()))
    refute Envelope.envelope?("a string")
    refute Envelope.envelope?(%{"a" => 1})
  end

  test "does not read under another key" do
    assert open(sealed(), @other_key) == :error
  end

  for {name, location} <- [
        {"stream", {"stream-2", "Event", "email"}},
        {"event type", {"stream-1", "Other", "email"}},
        {"field", {"stream-1", "Event", "other"}},
        {"body to metadata", {"stream-1", "Event", "metadata.email"}}
      ] do
    test "does not read in another #{name}" do
      assert open(sealed(), @key, unquote(Macro.escape(location))) == :error
    end
  end

  test "the additional data separates its parts" do
    {:ok, envelope} = Envelope.encrypt("value", {"key-1", @key}, "s", {"a", "bc", "d"})

    assert open(envelope, @key, {"ab", "c", "d"}) == :error
  end

  test "does not read when the key id in the envelope changes" do
    assert open(%{sealed() | "k" => "key-2"}) == :error
  end

  for {name, change} <- [
        {"a wrong version", quote(do: &%{&1 | "$pf" => 2})},
        {"a version that is a string", quote(do: &%{&1 | "$pf" => "1"})},
        {"a float version", quote(do: &%{&1 | "$pf" => 1.0})},
        {"no key id", quote(do: &Map.delete(&1, "k"))},
        {"an empty key id", quote(do: &%{&1 | "k" => ""})},
        {"no subject", quote(do: &Map.delete(&1, "s"))},
        {"a subject that is not a string", quote(do: &%{&1 | "s" => 1})},
        {"no nonce", quote(do: &Map.delete(&1, "n"))},
        {"bad base64 in the nonce", quote(do: &%{&1 | "n" => "!!!"})},
        {"a short nonce", quote(do: &%{&1 | "n" => Base.encode64("short")})},
        {"no ciphertext", quote(do: &Map.delete(&1, "c"))},
        {"bad base64 in the ciphertext", quote(do: &%{&1 | "c" => "!!!"})},
        {"less than a tag of ciphertext", quote(do: &%{&1 | "c" => Base.encode64("short")})},
        {"a ciphertext that is a number", quote(do: &%{&1 | "c" => 1})},
        {"a part too many", quote(do: &Map.put(&1, "x", 1))}
      ] do
    test "a map with #{name} is malformed" do
      malformed = unquote(change).(sealed())

      assert Envelope.envelope?(malformed)
      assert Envelope.parse(malformed) == :error
      assert Envelope.parse(atomized(malformed)) == :error
    end
  end
end
