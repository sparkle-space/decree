# The action pin check: every action a workflow uses is pinned to a full commit SHA with a
# version comment, the form Dependabot keeps up to date.
#
#     elixir scripts/check_action_pins.exs
#
# It reads every line of `.github/workflows/*.yml` and `*.yaml` under the current directory
# with a `uses` key, in any YAML form: `- uses:`, `uses:`, `{uses: ...}` or `"uses":`. A line
# passes when its value is `owner/repo@<40 hex>` (or `owner/repo/path@<40 hex>`) and a comment
# that opens with the version, `# v7.0.1`, or a path in the repository, `./...`. A tag, a
# branch, a short SHA, a missing or other comment, a quoted ref, a flow-style step and a
# `docker://` image fail. Each failure prints its file and line, and the check exits 1. It
# reads the files only: it does not check that a SHA exists on GitHub.

defmodule Decree.CheckActionPins do
  @uses ~r/(?:^|[\s{,])["']?uses["']?\s*:\s*(.*?)\s*$/
  @pinned ~r/^[\w.-]+\/[\w.-]+(?:\/[^@\s]+)?@[0-9a-f]{40}\s+#\s*v\d\S*$/

  def run do
    paths =
      Enum.sort(
        Path.wildcard(".github/workflows/*.yml") ++ Path.wildcard(".github/workflows/*.yaml")
      )

    if paths == [] do
      IO.puts(:stderr, "action pins: no workflows in .github/workflows")
      System.halt(1)
    end

    unpinned =
      for path <- paths,
          {line, number} <- path |> File.read!() |> String.split("\n") |> Enum.with_index(1),
          [_line, uses] <- [Regex.run(@uses, line)],
          not pinned?(uses),
          do: "#{path}:#{number}: #{uses}"

    case unpinned do
      [] ->
        IO.puts("action pins: every action is pinned to a full commit SHA with a version comment")

      unpinned ->
        Enum.each(unpinned, &IO.puts(:stderr, &1))
        count = length(unpinned)
        noun = if count == 1, do: "action is", else: "actions are"

        IO.puts(
          :stderr,
          "action pins: #{count} #{noun} not pinned to a full commit SHA with a version comment"
        )

        System.halt(1)
    end
  end

  defp pinned?("./" <> _path), do: true
  defp pinned?(uses), do: Regex.match?(@pinned, uses)
end

Decree.CheckActionPins.run()
