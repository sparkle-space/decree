defmodule Decree.ProvenanceTest do
  use ExUnit.Case, async: true

  # PROVENANCE.tsv describes the current files: column 1 of every row is a path that
  # exists in the repository.
  test "every PROVENANCE.tsv row names an existing file" do
    root = File.cwd!()

    missing =
      root
      |> Path.join("PROVENANCE.tsv")
      |> File.read!()
      |> String.split("\n")
      |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
      |> Enum.map(&(&1 |> String.split("\t") |> hd()))
      |> Enum.reject(&File.regular?(Path.join(root, &1)))

    assert missing == [], "PROVENANCE.tsv rows without a file: #{inspect(missing)}"
  end
end
