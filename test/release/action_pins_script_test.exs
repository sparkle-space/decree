# The action pin check that CI's "Action pins" job runs, `elixir scripts/check_action_pins.exs`.
# Each test runs it as CI does, on the workflows in a scratch directory.
defmodule Decree.ActionPinsScriptTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  @root Path.expand("../..", __DIR__)
  @script Path.join(@root, "scripts/check_action_pins.exs")

  @sha "3d3c42e5aac5ba805825da76410c181273ba90b1"

  defp check(dir), do: System.cmd("elixir", [@script], cd: dir, stderr_to_stdout: true)

  defp workflow(dir, name, steps) do
    workflows = Path.join(dir, ".github/workflows")
    File.mkdir_p!(workflows)

    File.write!(Path.join(workflows, name), """
    name: Example
    on: pull_request
    jobs:
      example:
        runs-on: ubuntu-latest
        steps:
    #{steps}
    """)
  end

  test "decree's own workflows pass" do
    assert {output, 0} = System.cmd("elixir", [@script], cd: @root, stderr_to_stdout: true)
    assert output =~ "action pins: every action is pinned"
  end

  test "an action pinned to a full commit SHA with a version comment passes", %{tmp_dir: dir} do
    workflow(dir, "ci.yml", """
          - uses: actions/checkout@#{@sha} # v7.0.1
          - name: Cache
            uses: actions/cache@#{@sha}   #   v5
          - uses: github/codeql-action/init@#{@sha} # v4.31.2
          - uses: aquasecurity/trivy-action@#{@sha} # 0.28.0
    """)

    workflow(dir, "other.yaml", "      - uses: ./.github/actions/local\n")

    assert {output, 0} = check(dir)
    assert output =~ "action pins: every action is pinned"
  end

  test "a reusable workflow in the repository passes", %{tmp_dir: dir} do
    # A job's `uses:`, as publish.yml calls ci.yml.
    File.mkdir_p!(Path.join(dir, ".github/workflows"))

    File.write!(Path.join(dir, ".github/workflows/publish.yml"), """
    name: Publish
    on: push
    jobs:
      gates:
        uses: ./.github/workflows/ci.yml
    """)

    assert {_output, 0} = check(dir)
  end

  for {name, uses} <- [
        {"a tag", "actions/checkout@v7 # v7"},
        {"a branch", "actions/checkout@main # v7"},
        {"a short SHA", "actions/checkout@3d3c42e # v7.0.1"},
        {"a SHA with no comment", "actions/checkout@#{@sha}"},
        {"a SHA with a comment that is not a version", "actions/checkout@#{@sha} # pinned"},
        {"a SHA with a comment that opens with a word", "actions/checkout@#{@sha} # version 7"},
        {"no ref", "actions/checkout"},
        {"a quoted ref", ~s("actions/checkout@#{@sha}" # v7.0.1)},
        {"a Docker image", "docker://alpine:3.22 # v3.22"}
      ] do
    test "#{name} fails, naming the file and line", %{tmp_dir: dir} do
      workflow(dir, "ci.yml", """
            - uses: actions/checkout@#{@sha} # v7.0.1
            - uses: #{unquote(uses)}
      """)

      assert {output, 1} = check(dir)
      assert output =~ ".github/workflows/ci.yml:8: #{unquote(uses)}"
      refute output =~ ".github/workflows/ci.yml:7:"
      assert output =~ "1 action is not pinned to a full commit SHA with a version comment"
    end
  end

  test "every unpinned action is reported, across files", %{tmp_dir: dir} do
    workflow(dir, "a.yml", "      - uses: actions/checkout@v7\n")
    workflow(dir, "b.yaml", "      - uses: actions/cache@v5\n")

    assert {output, 1} = check(dir)
    assert output =~ ".github/workflows/a.yml:7: actions/checkout@v7"
    assert output =~ ".github/workflows/b.yaml:7: actions/cache@v5"
    assert output =~ "2 actions are not pinned"
  end

  test "a flow-style step and a quoted key are checked too", %{tmp_dir: dir} do
    workflow(dir, "ci.yml", """
          - {uses: actions/checkout@v7}
          - "uses": actions/cache@v5
    """)

    assert {output, 1} = check(dir)
    assert output =~ ".github/workflows/ci.yml:7: actions/checkout@v7}"
    assert output =~ ".github/workflows/ci.yml:8: actions/cache@v5"
  end

  test "a directory with no workflows fails", %{tmp_dir: dir} do
    assert {output, 1} = check(dir)
    assert output =~ "no workflows"
  end
end
