# The release script. The Prepare release and Publish workflows run it
# (.github/workflows/prepare-release.yml, publish.yml); CONTRIBUTING.md, "Releases",
# describes the flow. It reads and writes mix.exs, CHANGELOG.md and README.md in the
# current directory, needs only Elixir, and fails with a message on stderr and exit status 1.
#
#     elixir scripts/release.exs version
#     elixir scripts/release.exs bump patch|minor|major [--confirm-1-0] [--date YYYY-MM-DD]
#     elixir scripts/release.exs notes VERSION
#     elixir scripts/release.exs plan [--previous-mix-exs PATH] [TAG ...]
#     elixir scripts/release.exs hex-step STATUS RELEASE_JSON
#
# - `version` prints mix.exs's version.
# - `bump` raises the version with the pre-1.0 rules and moves the CHANGELOG's
#   "## Unreleased" section under "## VERSION (DATE)". Before 1.0 a breaking change is a
#   minor bump; a major bump to 1.0.0 needs --confirm-1-0. An empty Unreleased section
#   is refused. A minor or major bump also moves the requirement users copy, `~> X.Y.0`:
#   README.md's install line, its `~> X.Y.0` sentence and its Status "(version X.Y.x)",
#   and the CHANGELOG preamble's `~> X.Y.0`. A patch moves none, since `~> X.Y.0` takes
#   it. A reference it cannot find, or finds twice, writes nothing. It prints the new
#   version.
# - `notes` prints a version's CHANGELOG section, without its heading: the GitHub
#   release body.
# - `plan` takes the repository's tags and prints `version=` and `release=` lines for
#   $GITHUB_OUTPUT. It releases when mix.exs's version is not the latest vX.Y.Z tag, and
#   refuses a version behind it. Other tags are ignored. With --previous-mix-exs, the
#   pushed-over commit's mix.exs, it releases only when the push changed the version: a
#   later push keeps a version that waits for approval, and must not release it too.
# - `hex-step` takes hex.pm's status code and answer for the release
#   (GET /api/packages/decree/releases/VERSION) and prints the publish job's next step:
#   `publish` (package and docs), `docs` (the package is there without its docs) or
#   `none`. Any other answer is refused.

defmodule Decree.ReleaseScript do
  @version_line ~r/^(\s*@version\s+")(\d+\.\d+\.\d+)(")$/m
  @unreleased "## Unreleased"

  def main(["version"]), do: IO.puts(version!())
  def main(["bump" | args]), do: bump(args)
  def main(["notes", version]), do: IO.write(notes!(File.read!("CHANGELOG.md"), version))
  def main(["plan" | args]), do: plan(args)
  def main(["hex-step", status, release_json]), do: IO.puts(hex_step!(status, release_json))

  def main(_args) do
    fail("""
    usage: elixir scripts/release.exs version
           elixir scripts/release.exs bump patch|minor|major [--confirm-1-0] [--date YYYY-MM-DD]
           elixir scripts/release.exs notes VERSION
           elixir scripts/release.exs plan [--previous-mix-exs PATH] [TAG ...]
           elixir scripts/release.exs hex-step STATUS RELEASE_JSON\
    """)
  end

  defp version!(path \\ "mix.exs") do
    case Regex.scan(@version_line, File.read!(path)) do
      [[_line, _prefix, version, _suffix]] -> version
      _ -> fail("#{path} must have exactly one @version \"X.Y.Z\" line")
    end
  end

  defp bump(args) do
    {opts, rest} = OptionParser.parse!(args, strict: [confirm_1_0: :boolean, date: :string])

    bump =
      case rest do
        [bump] when bump in ~w(patch minor major) -> bump
        _ -> fail("bump takes one of patch, minor or major")
      end

    current = Version.parse!(version!())
    next = next_version(current, bump, Keyword.get(opts, :confirm_1_0, false))
    date = Keyword.get_lazy(opts, :date, fn -> Date.to_iso8601(Date.utc_today()) end)
    changelog = move_unreleased!(File.read!("CHANGELOG.md"), next, date)
    mix_exs = Regex.replace(@version_line, File.read!("mix.exs"), "\\g{1}#{next}\\g{3}")

    # Everything is read and checked before anything is written.
    next_version = Version.parse!(next)

    writes =
      if requirement(next_version) == requirement(current) do
        [{"mix.exs", mix_exs}, {"CHANGELOG.md", changelog}]
      else
        [
          {"mix.exs", mix_exs},
          {"CHANGELOG.md", move_preamble_requirement!(changelog, current, next_version)},
          {"README.md", move_readme!(current, next_version)}
        ]
      end

    Enum.each(writes, fn {path, content} -> File.write!(path, content) end)
    IO.puts(next)
  end

  # `~> X.Y.0` takes X.Y.x only, so a minor or major bump moves the requirement users copy.
  defp requirement(%Version{major: major, minor: minor}), do: "~> #{major}.#{minor}.0"
  defp status(%Version{major: major, minor: minor}), do: "(version #{major}.#{minor}.x)"

  defp move_readme!(current, next) do
    readme =
      case File.read("README.md") do
        {:ok, readme} -> readme
        {:error, reason} -> fail("README.md does not read: #{:file.format_error(reason)}")
      end

    install = ~s({:decree, "#{requirement(current)}"})

    for reference <- [install, status(current)] do
      case occurrences(readme, reference) do
        1 -> :ok
        count -> fail("README.md must name #{reference} once, and names it #{count} times")
      end
    end

    readme =
      readme
      |> String.replace(install, ~s({:decree, "#{requirement(next)}"}))
      |> String.replace("`#{requirement(current)}`", "`#{requirement(next)}`")
      |> String.replace(status(current), status(next))

    if occurrences(readme, requirement(current)) > 0 do
      fail("README.md names #{requirement(current)} where the bump cannot move it")
    end

    readme
  end

  # Only the preamble: a release's own section keeps the requirement it was written with.
  defp move_preamble_requirement!(changelog, current, next) do
    {preamble, rest} = changelog |> String.split("\n") |> Enum.split_while(&(&1 != @unreleased))
    preamble = Enum.join(preamble, "\n")
    reference = "`#{requirement(current)}`"

    if occurrences(preamble, reference) == 0 do
      fail("CHANGELOG.md's preamble, above \"#{@unreleased}\", must name #{reference}")
    end

    preamble = String.replace(preamble, reference, "`#{requirement(next)}`")

    if occurrences(preamble, requirement(current)) > 0 do
      fail("CHANGELOG.md's preamble names #{requirement(current)} where the bump cannot move it")
    end

    Enum.join([preamble | rest], "\n")
  end

  defp occurrences(text, reference), do: length(String.split(text, reference)) - 1

  defp next_version(%Version{major: 0}, "major", false) do
    fail("a major bump from 0.x makes 1.0.0, and 1.0 is the founder's call: pass --confirm-1-0")
  end

  defp next_version(%Version{major: 0}, "major", true), do: "1.0.0"

  defp next_version(_current, _bump, true) do
    fail("--confirm-1-0 applies only to a major bump from 0.x")
  end

  defp next_version(%Version{major: major}, "major", false), do: "#{major + 1}.0.0"

  defp next_version(%Version{major: major, minor: minor}, "minor", false),
    do: "#{major}.#{minor + 1}.0"

  defp next_version(%Version{major: major, minor: minor, patch: patch}, "patch", false) do
    "#{major}.#{minor}.#{patch + 1}"
  end

  defp move_unreleased!(changelog, version, date) do
    lines = String.split(changelog, "\n")

    case Enum.count(lines, &(&1 == @unreleased)) do
      1 -> :ok
      _ -> fail("CHANGELOG.md has no \"#{@unreleased}\" section, or more than one")
    end

    {before, [@unreleased | after_heading]} = Enum.split_while(lines, &(&1 != @unreleased))
    {body, rest} = Enum.split_while(after_heading, &(not section_heading?(&1)))

    case trim_blank(body) do
      [] ->
        fail("the CHANGELOG's Unreleased section is empty: there is nothing to release")

      body ->
        heading = "## #{version} (#{date})"
        tail = if rest == [], do: [""], else: ["" | rest]
        Enum.join(before ++ [@unreleased, "", heading, ""] ++ body ++ tail, "\n")
    end
  end

  defp notes!(changelog, version) do
    heading = ~r/^## #{Regex.escape(version)}(\s.*)?$/
    lines = String.split(changelog, "\n")

    case Enum.drop_while(lines, &(not Regex.match?(heading, &1))) do
      [] ->
        fail("CHANGELOG.md has no section for #{version}")

      [_heading | after_heading] ->
        case after_heading |> Enum.take_while(&(not section_heading?(&1))) |> trim_blank() do
          [] -> fail("the CHANGELOG's section for #{version} is empty")
          body -> Enum.join(body, "\n") <> "\n"
        end
    end
  end

  defp plan(args) do
    {opts, tags} = OptionParser.parse!(args, strict: [previous_mix_exs: :string])
    version = version!()
    previous = if path = opts[:previous_mix_exs], do: version!(path)

    latest =
      tags
      |> Enum.flat_map(fn tag ->
        case Regex.run(~r/^v(\d+\.\d+\.\d+)$/, tag) do
          [^tag, number] -> [Version.parse!(number)]
          nil -> []
        end
      end)
      |> Enum.max(Version, fn -> nil end)

    release =
      case latest && Version.compare(Version.parse!(version), latest) do
        nil -> true
        :gt -> true
        :eq -> false
        :lt -> fail("mix.exs's version #{version} is behind the latest tag v#{latest}")
      end

    release = release and previous != version

    IO.puts("version=#{version}\nrelease=#{release}")
  end

  defp hex_step!("404", _release_json), do: "publish"

  defp hex_step!("200", release_json) do
    case release_json |> File.read!() |> JSON.decode() do
      {:ok, %{"has_docs" => true}} -> "none"
      {:ok, %{"has_docs" => false}} -> "docs"
      _ -> fail("hex.pm's answer for the release has no has_docs")
    end
  end

  defp hex_step!(status, _release_json), do: fail("hex.pm answered #{status} for the release")

  defp section_heading?(line), do: String.starts_with?(line, "## ")

  defp trim_blank(lines) do
    lines
    |> Enum.drop_while(&(String.trim(&1) == ""))
    |> Enum.reverse()
    |> Enum.drop_while(&(String.trim(&1) == ""))
    |> Enum.reverse()
  end

  defp fail(message) do
    IO.puts(:stderr, "error: " <> message)
    System.halt(1)
  end
end

Decree.ReleaseScript.main(System.argv())
