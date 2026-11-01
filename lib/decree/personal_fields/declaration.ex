defmodule Decree.PersonalFields.Declaration do
  @moduledoc """
  What an event declares through `Decree.Event.PersonalFields`.

    - `fields` - the personal fields of the event, each with its subject.

    - `metadata` - the personal metadata entries, each with its subject.

  A subject is one of `:x`, `{:first_present, [:a, :b]}` or `{:when_present, :x}`.
  """

  @type subject :: atom | {:first_present, [atom, ...]} | {:when_present, atom}

  @type t :: %__MODULE__{fields: [{atom, subject}], metadata: [{atom, subject}]}

  defstruct fields: [], metadata: []

  @options [:personal, :personal_metadata]

  @doc false
  # Runs at compile time, from the derive. Every refusal is a `CompileError`.
  @spec build!(module, [atom], term, Macro.Env.t()) :: t
  def build!(module, struct_fields, options, env) do
    refuse = fn problem ->
      raise CompileError,
        file: env.file,
        line: env.line,
        description:
          "cannot derive Decree.Event.PersonalFields for #{inspect(module)}: #{problem}"
    end

    unless Keyword.keyword?(options), do: refuse.("expected a keyword list of options")

    case Keyword.keys(options) -- @options do
      [] -> :ok
      unknown -> refuse.("unknown options #{inspect(unknown)}, expected #{inspect(@options)}")
    end

    fields = entries!(options, :personal, refuse)
    metadata = entries!(options, :personal_metadata, refuse)

    if fields == [] and metadata == [] do
      refuse.("`:personal` and `:personal_metadata` are both empty")
    end

    for {field, _subject} <- fields, field not in struct_fields do
      refuse.("the field #{inspect(field)} is not in the struct")
    end

    declared = Keyword.keys(fields)

    for {name, subject} <- fields ++ metadata, field <- subject_fields!(subject, name, refuse) do
      cond do
        field not in struct_fields ->
          refuse.("the subject field #{inspect(field)} is not in the struct")

        field in declared ->
          refuse.("the subject field #{inspect(field)} is itself declared personal")

        true ->
          :ok
      end
    end

    %__MODULE__{fields: fields, metadata: metadata}
  end

  defp entries!(options, option, refuse) do
    entries = Keyword.get(options, option, [])

    unless Keyword.keyword?(entries) do
      refuse.("`#{inspect(option)}` must be a keyword list, got: #{inspect(entries)}")
    end

    names = Keyword.keys(entries)

    case names -- Enum.uniq(names) do
      [] -> entries
      repeated -> refuse.("`#{inspect(option)}` repeats #{inspect(repeated)}")
    end
  end

  defp subject_fields!(field, _name, _refuse) when is_atom(field), do: [field]

  defp subject_fields!({:when_present, field}, _name, _refuse) when is_atom(field), do: [field]

  defp subject_fields!({:first_present, [_ | _] = fields} = subject, name, refuse) do
    if Enum.all?(fields, &is_atom/1), do: fields, else: invalid_subject(subject, name, refuse)
  end

  defp subject_fields!(subject, name, refuse), do: invalid_subject(subject, name, refuse)

  defp invalid_subject(subject, name, refuse) do
    refuse.(
      "#{inspect(name)} names the invalid subject #{inspect(subject)}, expected a field, " <>
        "`{:first_present, fields}` or `{:when_present, field}`"
    )
  end
end
