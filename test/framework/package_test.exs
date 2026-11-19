defmodule Decree.PackageTest do
  use ExUnit.Case, async: true

  # The upstream MIT notices ship in the Hex package: NOTICE carries them, LICENSE is decree's.
  @required ["lib", "priv", "mix.exs", "LICENSE", "NOTICE", "README.md", "CHANGELOG.md"]

  test "the Hex package ships the licence, the notices, the README and the changelog" do
    package = Mix.Project.config()[:package]

    assert is_list(package)
    assert package[:licenses] == ["MIT"]

    for file <- @required do
      assert file in package[:files], file
      assert File.exists?(file), file
    end
  end

  test "the package has a description" do
    assert is_binary(Mix.Project.config()[:description])
  end
end
