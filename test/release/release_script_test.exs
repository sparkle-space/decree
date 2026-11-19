# The release script that the Prepare release and Publish workflows run
# (.github/workflows/prepare-release.yml, publish.yml). Each test runs the script as the
# workflows do, `elixir scripts/release.exs ...`, in a scratch directory.
defmodule Decree.ReleaseScriptTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  @root Path.expand("../..", __DIR__)
  @script Path.join(@root, "scripts/release.exs")

  @mix_exs """
  defmodule Example.MixProject do
    use Mix.Project

    @version "VERSION"

    def project, do: [app: :example, version: @version]
  end
  """

  @changelog """
  # Changelog

  The intro.

  ## Unreleased

  ### Fixed

  - A fix.

  ## 0.1.0

  The first release.
  """

  defp project(dir, version, changelog \\ @changelog) do
    File.write!(Path.join(dir, "mix.exs"), String.replace(@mix_exs, "VERSION", version))
    File.write!(Path.join(dir, "CHANGELOG.md"), changelog)
  end

  defp release(dir, args) do
    System.cmd("elixir", [@script | args], cd: dir, stderr_to_stdout: true)
  end

  defp read(dir, file), do: File.read!(Path.join(dir, file))

  describe "version" do
    test "prints the version in mix.exs", %{tmp_dir: dir} do
      project(dir, "0.4.2")

      assert release(dir, ["version"]) == {"0.4.2\n", 0}
    end

    test "reads decree's own mix.exs" do
      assert release(@root, ["version"]) == {Mix.Project.config()[:version] <> "\n", 0}
    end

    test "refuses a mix.exs without exactly one @version", %{tmp_dir: dir} do
      File.write!(Path.join(dir, "mix.exs"), "@version \"0.1.0\"\n@version \"0.2.0\"\n")

      assert {output, 1} = release(dir, ["version"])
      assert output =~ "exactly one @version"
    end
  end

  describe "bump" do
    test "patch: 0.1.0 becomes 0.1.1 and Unreleased moves under it", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert release(dir, ["bump", "patch", "--date", "2026-11-20"]) == {"0.1.1\n", 0}
      assert read(dir, "mix.exs") =~ ~s(@version "0.1.1")

      assert read(dir, "CHANGELOG.md") == """
             # Changelog

             The intro.

             ## Unreleased

             ## 0.1.1 (2026-11-20)

             ### Fixed

             - A fix.

             ## 0.1.0

             The first release.
             """
    end

    test "minor before 1.0: 0.1.3 becomes 0.2.0", %{tmp_dir: dir} do
      project(dir, "0.1.3")

      assert release(dir, ["bump", "minor", "--date", "2026-11-20"]) == {"0.2.0\n", 0}
      assert read(dir, "mix.exs") =~ ~s(@version "0.2.0")
      assert read(dir, "CHANGELOG.md") =~ "## Unreleased\n\n## 0.2.0 (2026-11-20)\n\n### Fixed"
    end

    test "major before 1.0 is refused without --confirm-1-0", %{tmp_dir: dir} do
      project(dir, "0.4.0")

      assert {output, 1} = release(dir, ["bump", "major"])
      assert output =~ "1.0 is the founder's call"
      assert read(dir, "mix.exs") =~ ~s(@version "0.4.0")
      assert read(dir, "CHANGELOG.md") == @changelog
    end

    test "major before 1.0 with --confirm-1-0 becomes 1.0.0", %{tmp_dir: dir} do
      project(dir, "0.4.0")

      assert release(dir, ["bump", "major", "--confirm-1-0", "--date", "2026-11-20"]) ==
               {"1.0.0\n", 0}

      assert read(dir, "mix.exs") =~ ~s(@version "1.0.0")
    end

    test "major after 1.0 needs no confirmation: 1.2.3 becomes 2.0.0", %{tmp_dir: dir} do
      project(dir, "1.2.3")

      assert release(dir, ["bump", "major", "--date", "2026-11-20"]) == {"2.0.0\n", 0}
    end

    test "--confirm-1-0 is refused on any other bump", %{tmp_dir: dir} do
      project(dir, "0.4.0")

      assert {output, 1} = release(dir, ["bump", "patch", "--confirm-1-0"])
      assert output =~ "--confirm-1-0"
      assert read(dir, "mix.exs") =~ ~s(@version "0.4.0")
    end

    test "an unknown bump is refused", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert {output, 1} = release(dir, ["bump", "huge"])
      assert output =~ "patch, minor or major"
    end

    test "an empty Unreleased section is refused, and nothing changes", %{tmp_dir: dir} do
      changelog = "# Changelog\n\n## Unreleased\n\n  \n\n## 0.1.0\n\nThe first release.\n"
      project(dir, "0.1.0", changelog)

      assert {output, 1} = release(dir, ["bump", "patch"])
      assert output =~ "Unreleased section is empty"
      assert read(dir, "mix.exs") =~ ~s(@version "0.1.0")
      assert read(dir, "CHANGELOG.md") == changelog
    end

    test "an empty Unreleased section at the end of the file is refused", %{tmp_dir: dir} do
      project(dir, "0.1.0", "# Changelog\n\n## Unreleased\n")

      assert {output, 1} = release(dir, ["bump", "patch"])
      assert output =~ "Unreleased section is empty"
    end

    test "a CHANGELOG without an Unreleased section is refused", %{tmp_dir: dir} do
      project(dir, "0.1.0", "# Changelog\n\n## 0.1.0\n\nThe first release.\n")

      assert {output, 1} = release(dir, ["bump", "patch"])
      assert output =~ "no \"## Unreleased\" section"
    end

    test "bumps decree's own mix.exs and CHANGELOG.md", %{tmp_dir: dir} do
      File.cp!(Path.join(@root, "mix.exs"), Path.join(dir, "mix.exs"))

      changelog =
        @root
        |> Path.join("CHANGELOG.md")
        |> File.read!()
        |> String.replace("## Unreleased\n", "## Unreleased\n\n- An entry.\n", global: false)

      File.write!(Path.join(dir, "CHANGELOG.md"), changelog)
      [major, minor, patch] = Mix.Project.config()[:version] |> String.split(".")
      next = "#{major}.#{minor}.#{String.to_integer(patch) + 1}"

      assert release(dir, ["bump", "patch", "--date", "2026-11-20"]) == {next <> "\n", 0}
      assert read(dir, "mix.exs") =~ ~s(@version "#{next}")
      assert release(dir, ["notes", next]) == {"- An entry.\n", 0}
    end
  end

  describe "notes" do
    test "prints a version's section, without its heading", %{tmp_dir: dir} do
      project(dir, "0.1.0")
      release(dir, ["bump", "patch", "--date", "2026-11-20"])

      assert release(dir, ["notes", "0.1.1"]) == {"### Fixed\n\n- A fix.\n", 0}
      assert release(dir, ["notes", "0.1.0"]) == {"The first release.\n", 0}
    end

    test "reads decree's own CHANGELOG.md for 0.1.0" do
      assert {notes, 0} = release(@root, ["notes", "0.1.0"])
      assert notes =~ ~r/\AThe first release on Hex\./
    end

    test "a version with no section is refused", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert {output, 1} = release(dir, ["notes", "0.1"])
      assert output =~ "no section for 0.1"
    end

    test "an empty section is refused", %{tmp_dir: dir} do
      project(dir, "0.1.0", "# Changelog\n\n## 0.1.1 (2026-11-20)\n\n## 0.1.0\n\nFirst.\n")

      assert {output, 1} = release(dir, ["notes", "0.1.1"])
      assert output =~ "section for 0.1.1 is empty"
    end
  end

  # The Publish workflow's gate: it releases only when mix.exs's version is not the
  # latest v* tag, and refuses a version behind it.
  describe "plan" do
    test "an unchanged version does not release", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert release(dir, ["plan", "v0.1.0"]) == {"version=0.1.0\nrelease=false\n", 0}
    end

    test "decree's own version at its own tag does not release" do
      version = Mix.Project.config()[:version]

      assert release(@root, ["plan", "v" <> version]) ==
               {"version=#{version}\nrelease=false\n", 0}
    end

    test "a bumped version releases", %{tmp_dir: dir} do
      project(dir, "0.1.1")

      assert release(dir, ["plan", "v0.1.0"]) == {"version=0.1.1\nrelease=true\n", 0}
    end

    test "no tags at all releases", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert release(dir, ["plan"]) == {"version=0.1.0\nrelease=true\n", 0}
    end

    test "the latest tag is the highest version, not the last in the list", %{tmp_dir: dir} do
      project(dir, "0.10.0")

      assert release(dir, ["plan", "v0.10.0", "v0.9.0", "v0.2.0"]) ==
               {"version=0.10.0\nrelease=false\n", 0}
    end

    test "tags that are not vX.Y.Z are ignored", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert release(dir, ["plan", "v0.1.0", "v9", "vnext", "v9.0.0-rc.1"]) ==
               {"version=0.1.0\nrelease=false\n", 0}
    end

    test "a version behind the latest tag is refused", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert {output, 1} = release(dir, ["plan", "v0.1.0", "v0.2.0"])
      assert output =~ "0.1.0 is behind the latest tag v0.2.0"
    end
  end

  test "an unknown command is refused", %{tmp_dir: dir} do
    assert {output, 1} = release(dir, ["publish"])
    assert output =~ "usage:"
  end
end
