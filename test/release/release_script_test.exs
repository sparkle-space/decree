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

  The intro. Depend on `~> 0.1.0` to take patches only.

  ## Unreleased

  ### Fixed

  - A fix.

  ## 0.1.0

  The first release.
  """

  # The README's version references a minor or major bump moves: the install line, the
  # sentence after it and the Status line. The example version in Status is not one.
  defp readme(requirement, series) do
    """
    # decree

    decree is pre-1.0 (version #{series}). Until 1.0, a minor version (0.2.0) may break.

    ```elixir
    def deps do
      [
        {:decree, "#{requirement}"},
        {:jason, "~> 1.4"}
      ]
    end
    ```

    `#{requirement}` takes patch versions only.
    """
  end

  defp project(dir, version, changelog \\ @changelog) do
    [major, minor, _patch] = String.split(version, ".")
    File.write!(Path.join(dir, "mix.exs"), String.replace(@mix_exs, "VERSION", version))
    File.write!(Path.join(dir, "CHANGELOG.md"), changelog)

    File.write!(
      Path.join(dir, "README.md"),
      readme("~> #{major}.#{minor}.0", "#{major}.#{minor}.x")
    )
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

             The intro. Depend on `~> 0.1.0` to take patches only.

             ## Unreleased

             ## 0.1.1 (2026-11-20)

             ### Fixed

             - A fix.

             ## 0.1.0

             The first release.
             """
    end

    # `~> 0.1.0` already takes 0.1.1, so a patch moves no requirement.
    test "patch leaves the README alone", %{tmp_dir: dir} do
      project(dir, "0.1.0")

      assert release(dir, ["bump", "patch", "--date", "2026-11-20"]) == {"0.1.1\n", 0}
      assert read(dir, "README.md") == readme("~> 0.1.0", "0.1.x")
    end

    test "patch needs no README", %{tmp_dir: dir} do
      project(dir, "0.1.0")
      File.rm!(Path.join(dir, "README.md"))

      assert release(dir, ["bump", "patch", "--date", "2026-11-20"]) == {"0.1.1\n", 0}
      refute File.exists?(Path.join(dir, "README.md"))
    end

    test "minor before 1.0: 0.1.3 becomes 0.2.0", %{tmp_dir: dir} do
      project(dir, "0.1.3")

      assert release(dir, ["bump", "minor", "--date", "2026-11-20"]) == {"0.2.0\n", 0}
      assert read(dir, "mix.exs") =~ ~s(@version "0.2.0")
      assert read(dir, "CHANGELOG.md") =~ "## Unreleased\n\n## 0.2.0 (2026-11-20)\n\n### Fixed"
    end

    test "minor moves the README's requirement and Status, and the CHANGELOG's requirement",
         %{tmp_dir: dir} do
      # A requirement in a release's own section is history, and stays.
      changelog =
        String.replace(@changelog, "- A fix.", "- A fix. Move to `~> 0.1.0` from 0.0.x.")

      project(dir, "0.1.3", changelog)

      assert release(dir, ["bump", "minor", "--date", "2026-11-20"]) == {"0.2.0\n", 0}
      assert read(dir, "README.md") == readme("~> 0.2.0", "0.2.x")

      assert read(dir, "CHANGELOG.md") == """
             # Changelog

             The intro. Depend on `~> 0.2.0` to take patches only.

             ## Unreleased

             ## 0.2.0 (2026-11-20)

             ### Fixed

             - A fix. Move to `~> 0.1.0` from 0.0.x.

             ## 0.1.0

             The first release.
             """
    end

    test "major before 1.0 is refused without --confirm-1-0", %{tmp_dir: dir} do
      project(dir, "0.4.0")

      assert {output, 1} = release(dir, ["bump", "major"])
      assert output =~ "1.0 is the founder's call"
      assert read(dir, "mix.exs") =~ ~s(@version "0.4.0")
      assert read(dir, "CHANGELOG.md") == @changelog
      assert read(dir, "README.md") == readme("~> 0.4.0", "0.4.x")
    end

    test "major before 1.0 with --confirm-1-0 becomes 1.0.0", %{tmp_dir: dir} do
      project(dir, "0.4.0", String.replace(@changelog, "~> 0.1.0", "~> 0.4.0"))

      assert release(dir, ["bump", "major", "--confirm-1-0", "--date", "2026-11-20"]) ==
               {"1.0.0\n", 0}

      assert read(dir, "mix.exs") =~ ~s(@version "1.0.0")
      assert read(dir, "README.md") == readme("~> 1.0.0", "1.0.x")
      assert read(dir, "CHANGELOG.md") =~ "Depend on `~> 1.0.0` to take patches only."
    end

    test "major after 1.0 needs no confirmation: 1.2.3 becomes 2.0.0", %{tmp_dir: dir} do
      project(dir, "1.2.3", String.replace(@changelog, "~> 0.1.0", "~> 1.2.0"))

      assert release(dir, ["bump", "major", "--date", "2026-11-20"]) == {"2.0.0\n", 0}
      assert read(dir, "README.md") == readme("~> 2.0.0", "2.0.x")
      assert read(dir, "CHANGELOG.md") =~ "Depend on `~> 2.0.0` to take patches only."
    end

    # A reference the bump cannot find would leave the release PR red, so it writes nothing.
    for {name, change} <- [
          {"no README", :remove},
          {"a README without the install line",
           {"{:decree, \"~> 0.1.0\"}", "{:decree, \">= 0.0.0\"}"}},
          {"a README without the Status version", {"(version 0.1.x)", "(version 0.1)"}},
          {"a README with the install line twice",
           {"{:jason, \"~> 1.4\"}", "{:decree, \"~> 0.1.0\"}"}}
        ] do
      test "minor refuses #{name}, and nothing changes", %{tmp_dir: dir} do
        project(dir, "0.1.3")
        readme_path = Path.join(dir, "README.md")

        case unquote(Macro.escape(change)) do
          :remove -> File.rm!(readme_path)
          {from, to} -> File.write!(readme_path, String.replace(read(dir, "README.md"), from, to))
        end

        before = File.read(readme_path)

        assert {output, 1} = release(dir, ["bump", "minor", "--date", "2026-11-20"])
        assert output =~ "README.md"
        assert read(dir, "mix.exs") =~ ~s(@version "0.1.3")
        assert read(dir, "CHANGELOG.md") == @changelog
        assert File.read(readme_path) == before
      end
    end

    test "minor refuses a CHANGELOG whose preamble names no requirement, and nothing changes",
         %{tmp_dir: dir} do
      changelog = String.replace(@changelog, "Depend on `~> 0.1.0`", "Depend on a minor")
      project(dir, "0.1.3", changelog)

      assert {output, 1} = release(dir, ["bump", "minor", "--date", "2026-11-20"])
      assert output =~ "CHANGELOG.md"
      assert read(dir, "mix.exs") =~ ~s(@version "0.1.3")
      assert read(dir, "CHANGELOG.md") == changelog
      assert read(dir, "README.md") == readme("~> 0.1.0", "0.1.x")
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

    # decree's own CHANGELOG.md, its Unreleased section's body, whatever main holds there and
    # empty on a release commit, made one known entry.
    defp own_changelog(dir) do
      changelog =
        Regex.replace(
          ~r/^## Unreleased\n.*?(?=^## )/ms,
          @root |> Path.join("CHANGELOG.md") |> File.read!(),
          "## Unreleased\n\n- An entry.\n\n",
          global: false
        )

      File.write!(Path.join(dir, "CHANGELOG.md"), changelog)
    end

    test "bumps decree's own mix.exs and CHANGELOG.md", %{tmp_dir: dir} do
      File.cp!(Path.join(@root, "mix.exs"), Path.join(dir, "mix.exs"))
      own_changelog(dir)
      [major, minor, patch] = Mix.Project.config()[:version] |> String.split(".")
      next = "#{major}.#{minor}.#{String.to_integer(patch) + 1}"

      assert release(dir, ["bump", "patch", "--date", "2026-11-20"]) == {next <> "\n", 0}
      assert read(dir, "mix.exs") =~ ~s(@version "#{next}")
      assert release(dir, ["notes", next]) == {"- An entry.\n", 0}
    end

    # What test/framework/readme_example_test.exs reads from mix.exs's version, so the release
    # PR a minor bump opens passes CI as generated.
    test "minor moves every version reference in decree's own README.md and CHANGELOG.md",
         %{tmp_dir: dir} do
      for file <- ["mix.exs", "README.md"] do
        File.cp!(Path.join(@root, file), Path.join(dir, file))
      end

      own_changelog(dir)

      [major, minor, _patch] = Mix.Project.config()[:version] |> String.split(".")
      next_minor = String.to_integer(minor) + 1
      old = "~> #{major}.#{minor}.0"
      new = "~> #{major}.#{next_minor}.0"

      assert release(dir, ["bump", "minor", "--date", "2026-11-20"]) ==
               {"#{major}.#{next_minor}.0\n", 0}

      readme = read(dir, "README.md")
      assert length(String.split(readme, ~s({:decree, "#{new}"}))) == 2
      assert readme =~ "decree is pre-1.0 (version #{major}.#{next_minor}.x)."
      refute readme =~ old

      [preamble, _rest] = String.split(read(dir, "CHANGELOG.md"), "\n## Unreleased\n", parts: 2)
      assert preamble =~ "`#{new}`"
      refute preamble =~ old
    end
  end

  # The workflow commits what the bump wrote: a file left out stays at the old version in the
  # release PR.
  describe "the Prepare release workflow" do
    @workflow Path.join(@root, ".github/workflows/prepare-release.yml")

    test "commits every file a bump writes" do
      assert [files] =
               Regex.scan(~r/^\s*git add (.+)$/m, File.read!(@workflow), capture: :all_but_first)

      assert files |> hd() |> String.split() |> Enum.sort() == [
               "CHANGELOG.md",
               "README.md",
               "mix.exs"
             ]
    end

    test "asks for no hand edit of the README's version references" do
      refute File.read!(@workflow) =~ "update README.md's version references"
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

  # On a push, the Publish workflow passes the pushed-over commit's mix.exs: only the push
  # that changed the version releases. A later push while that release waits for approval
  # keeps the untagged version and must not release it from its own commit.
  describe "plan --previous-mix-exs" do
    defp previous(dir, version) do
      path = Path.join(dir, "previous-mix.exs")
      File.write!(path, String.replace(@mix_exs, "VERSION", version))
      path
    end

    test "the push that bumped the version releases", %{tmp_dir: dir} do
      project(dir, "0.1.1")

      assert release(dir, ["plan", "--previous-mix-exs", previous(dir, "0.1.0"), "v0.1.0"]) ==
               {"version=0.1.1\nrelease=true\n", 0}
    end

    test "a later push with the same untagged version does not release", %{tmp_dir: dir} do
      project(dir, "0.1.1")

      assert release(dir, ["plan", "--previous-mix-exs", previous(dir, "0.1.1"), "v0.1.0"]) ==
               {"version=0.1.1\nrelease=false\n", 0}
    end

    test "a bump to an already tagged version does not release", %{tmp_dir: dir} do
      project(dir, "0.1.1")

      assert release(dir, ["plan", "--previous-mix-exs", previous(dir, "0.1.0"), "v0.1.1"]) ==
               {"version=0.1.1\nrelease=false\n", 0}
    end

    test "a previous mix.exs without one @version is refused", %{tmp_dir: dir} do
      project(dir, "0.1.1")
      path = Path.join(dir, "previous-mix.exs")
      File.write!(path, "defmodule Empty do\nend\n")

      assert {output, 1} = release(dir, ["plan", "--previous-mix-exs", path, "v0.1.0"])
      assert output =~ "exactly one @version"
    end
  end

  # The publish job's next Hex step, from hex.pm's answer for the release
  # (GET /api/packages/decree/releases/VERSION): publish the package and docs, publish
  # the docs alone when an earlier run published only the package, or nothing.
  describe "hex-step" do
    defp answer(dir, body) do
      path = Path.join(dir, "release.json")
      File.write!(path, body)
      path
    end

    test "no release on Hex: publish the package and docs", %{tmp_dir: dir} do
      assert release(dir, ["hex-step", "404", answer(dir, ~s({"status":404}))]) ==
               {"publish\n", 0}
    end

    test "a release without docs: publish the docs alone", %{tmp_dir: dir} do
      json = answer(dir, ~s({"version":"0.1.1","has_docs":false}))

      assert release(dir, ["hex-step", "200", json]) == {"docs\n", 0}
    end

    test "a release with docs: nothing to do", %{tmp_dir: dir} do
      json = answer(dir, ~s({"version":"0.1.1","has_docs":true}))

      assert release(dir, ["hex-step", "200", json]) == {"none\n", 0}
    end

    test "a release whose answer has no has_docs is refused", %{tmp_dir: dir} do
      assert {output, 1} = release(dir, ["hex-step", "200", answer(dir, ~s({"version":"0.1.1"}))])
      assert output =~ "has_docs"
    end

    test "any other answer from hex.pm is refused", %{tmp_dir: dir} do
      assert {output, 1} = release(dir, ["hex-step", "503", answer(dir, "unavailable")])
      assert output =~ "hex.pm answered 503"
    end
  end

  test "an unknown command is refused", %{tmp_dir: dir} do
    assert {output, 1} = release(dir, ["publish"])
    assert output =~ "usage:"
  end
end
