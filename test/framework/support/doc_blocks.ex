defmodule Decree.DocBlocks do
  @moduledoc """
  Code blocks read from a module's compiled docs, so a test can run a doc's example as written.
  """

  @doc "A module's moduledoc, or the doc of one of its functions or callbacks: `{kind, name, arity}`."
  def doc!(module, entry \\ :moduledoc) do
    {:docs_v1, _anno, _language, _format, moduledoc, _metadata, entries} = Code.fetch_docs(module)

    doc =
      case entry do
        :moduledoc ->
          moduledoc

        {_kind, _name, _arity} = key ->
          Enum.find_value(entries, fn
            {^key, _anno, _signature, doc, _metadata} -> doc
            _entry -> nil
          end)
      end

    case doc do
      %{"en" => text} -> text
      _none -> raise ArgumentError, "#{inspect(module)} has no doc for #{inspect(entry)}"
    end
  end

  @doc """
  The indented code block in `doc` whose first line is `first_line`, without its indentation.
  The block runs while lines are blank or indented at least as far as its first.
  """
  def block!(doc, first_line) do
    case blocks(doc, first_line) do
      [block | _rest] -> block
      [] -> raise ArgumentError, "no code block opens with #{inspect(first_line)}"
    end
  end

  @doc "Every indented code block in `doc` whose first line is `first_line`, in order."
  def blocks(doc, first_line), do: doc |> String.split("\n") |> blocks(first_line, [])

  defp blocks(lines, first_line, found) do
    case Enum.drop_while(lines, &(String.trim(&1) != first_line)) do
      [first | rest] ->
        indent = byte_size(first) - byte_size(String.trim_leading(first))
        prefix = String.duplicate(" ", indent)

        {body, rest} =
          Enum.split_while(rest, &(String.trim(&1) == "" or String.starts_with?(&1, prefix)))

        block = Enum.map_join([first | body], "\n", &String.replace_prefix(&1, prefix, ""))
        blocks(rest, first_line, [block <> "\n" | found])

      [] ->
        Enum.reverse(found)
    end
  end
end
