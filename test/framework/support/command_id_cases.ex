defmodule Decree.CommandIdCases do
  @moduledoc false

  # The one table of `command_id` values every fence drives: dispatch down an aggregate
  # route, down a decision route, and `Decree.Command.check_id/1`, so the three cannot
  # answer differently. A value is a literal, so a failing case names what it sent.

  @canonical "6f9619ff-8b86-d011-b42d-00c04fc964ff"

  @doc """
  A canonical id, for a case that needs one to stand next to.
  """
  def canonical, do: @canonical

  @doc """
  `{name, value, refusal}` for every value a dispatch refuses.
  """
  def refusals do
    [
      {"nil", nil, :command_id_required},
      {"an empty string", "", :command_id_required},
      {"blank", "   ", :command_id_required},
      {"whitespace of several kinds", " \t\n", :command_id_required},
      {"an integer", 123, :invalid_command_id},
      {"an atom", :an_atom, :invalid_command_id},
      {"a map", %{}, :invalid_command_id},
      {"an uppercase UUID", String.upcase(@canonical), :invalid_command_id},
      {"a UUID with a leading space", " " <> @canonical, :invalid_command_id},
      {"a UUID with a trailing newline", @canonical <> "\n", :invalid_command_id},
      {"not a UUID", "not-a-uuid", :invalid_command_id},
      {"32 hex digits with no dashes", String.replace(@canonical, "-", ""), :invalid_command_id},
      {"the raw 16-byte form", Decree.UUID.string_to_binary!(@canonical), :invalid_command_id}
    ]
  end
end
