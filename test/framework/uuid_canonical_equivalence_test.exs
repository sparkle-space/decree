defmodule Decree.UUIDCanonicalEquivalenceTest do
  use ExUnit.Case, async: true

  # `Decree.UUID.canonical?/1` is what decree's `command_id` and identity refusals ask. It must
  # answer what sparklespace's own guard answers (`SparkCore.Validation.canonical_uuid_string?/1`),
  # whose definition is the right-hand side below, so that the framework refuses exactly what
  # the app's facades refuse.

  alias Decree.UUID

  defp app_canonical?(value) do
    is_binary(value) and byte_size(value) == 36 and Ecto.UUID.cast(value) == {:ok, value}
  end

  defp mixed_case(uuid) do
    uuid
    |> String.graphemes()
    |> Enum.with_index()
    |> Enum.map_join(fn {char, index} ->
      if rem(index, 2) == 0, do: String.upcase(char), else: char
    end)
  end

  defp replace_at(binary, index, char) do
    <<before::binary-size(^index), _::binary-size(1), rest::binary>> = binary
    before <> char <> rest
  end

  defp insert_at(binary, index, char) do
    <<before::binary-size(^index), rest::binary>> = binary
    before <> char <> rest
  end

  defp delete_at(binary, index) do
    <<before::binary-size(^index), _::binary-size(1), rest::binary>> = binary
    before <> rest
  end

  # Moves each dash one position left and one right.
  defp moved_dashes(uuid) do
    for dash <- [8, 13, 18, 23], shift <- [-1, 1] do
      <<char::binary-size(1)>> = binary_part(uuid, dash + shift, 1)

      uuid |> replace_at(dash, char) |> replace_at(dash + shift, "-")
    end
  end

  defp values do
    generated = for _ <- 1..50, do: UUID.uuid4()
    uuid = UUID.uuid4()

    positional =
      for index <- 0..35, char <- ["g", "G", "z", " ", "-", "_", "A", "F", "0", "\n", "é"] do
        replace_at(uuid, index, char)
      end

    lengths =
      for(index <- 0..35, do: delete_at(uuid, index)) ++
        for index <- 0..36, char <- ["0", "a", "-", " "], do: insert_at(uuid, index, char)

    generated ++
      Enum.map(generated, &String.upcase/1) ++
      Enum.map(generated, &mixed_case/1) ++
      positional ++
      lengths ++
      moved_dashes(uuid) ++
      [
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        String.replace(uuid, "-", ""),
        "{" <> uuid <> "}",
        "urn:uuid:" <> uuid,
        UUID.string_to_binary!(uuid),
        "",
        nil,
        123,
        :atom,
        %{},
        [uuid],
        String.to_charlist(uuid)
      ]
  end

  test "`canonical?/1` answers what the app's guard answers, value for value" do
    disagreements =
      for value <- values(), UUID.canonical?(value) != app_canonical?(value), do: value

    assert disagreements == []
  end

  test "the generated set holds both answers" do
    answers = values() |> Enum.map(&UUID.canonical?/1) |> Enum.frequencies()

    assert answers[true] > 50
    assert answers[false] > 500
  end
end
