# The Prepare release workflow's release PR token (.github/workflows/prepare-release.yml) and
# CONTRIBUTING.md's release steps. GitHub starts no workflow on a pull request that a
# workflow's own GITHUB_TOKEN opened, so the workflow opens the release PR with an
# installation token of decree's release GitHub App, and CI runs on it with no manual step.
# The tests read the workflow's text, and run its configuration check as a runner does.
defmodule Decree.PrepareReleaseWorkflowTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)
  @workflow File.read!(Path.join(@root, ".github/workflows/prepare-release.yml"))
  @contributing File.read!(Path.join(@root, "CONTRIBUTING.md"))

  # The prepare job's steps, in order, each as its text: the lines from its `- ` to the next.
  defp steps do
    @workflow
    |> String.split(~r/^      - /m)
    |> tl()
  end

  defp step_index(name) do
    Enum.find_index(steps(), &(&1 =~ ~r/^\s*name: #{Regex.escape(name)}$/m)) ||
      flunk("no step named #{inspect(name)} in prepare-release.yml")
  end

  defp step(name), do: Enum.at(steps(), step_index(name))

  defp mint_index do
    Enum.find_index(steps(), &(&1 =~ ~r/^\s*uses: actions\/create-github-app-token@/m)) ||
      flunk("no actions/create-github-app-token step in prepare-release.yml")
  end

  defp mint, do: Enum.at(steps(), mint_index())

  # A step's `run: |` script, run as a runner runs it, with the step's env given.
  defp run_step(name, env) do
    [_all, script] = Regex.run(~r/^\s*run: \|\n(.*)\z/ms, step(name))

    System.cmd("bash", ["--noprofile", "--norc", "-eo", "pipefail", "-c", script],
      env: env,
      stderr_to_stdout: true
    )
  end

  # CONTRIBUTING.md's "Releases" step 2, up to step 3.
  defp review_step do
    [_all, text] = Regex.run(~r/^2\. \*\*Review the release PR\.\*\*(.*?)^3\. /ms, @contributing)
    text
  end

  @check "Check that the release App is configured"
  @push "Push the release branch and open the PR"

  describe "the release PR token" do
    test "opens the PR with the release App's token, never the workflow's own" do
      assert step(@push) =~ ~r/^\s*GH_TOKEN: \$\{\{ steps\.app-token\.outputs\.token \}\}$/m
      refute @workflow =~ "github.token"
      refute @workflow =~ "secrets.GITHUB_TOKEN"
    end

    test "mints the token with contents and pull-requests write, on decree only" do
      assert mint() =~ ~r/^\s*id: app-token$/m
      assert mint() =~ ~r/^\s*client-id: \$\{\{ vars\.RELEASE_APP_CLIENT_ID \}\}$/m
      assert mint() =~ ~r/^\s*private-key: \$\{\{ secrets\.RELEASE_APP_PRIVATE_KEY \}\}$/m

      permissions =
        ~r/^\s*(permission-[\w-]+: \w+)$/m
        |> Regex.scan(mint(), capture: :all_but_first)
        |> List.flatten()
        |> Enum.sort()

      assert permissions == ["permission-contents: write", "permission-pull-requests: write"]

      # With no owner and no repositories, the token covers the repository the run is in.
      refute mint() =~ ~r/^\s*(owner|repositories|enterprise|skip-token-revoke):/m
    end

    test "is minted after the bump, and only the push step uses it" do
      assert mint_index() == step_index(@push) - 1
      assert mint_index() > step_index("Bump the version and move the Unreleased section")

      users = for {text, index} <- Enum.with_index(steps()), text =~ "steps.app-token", do: index
      assert users == [step_index(@push)]

      refute step("Bump the version and move the Unreleased section") =~ ~r/token/i
    end

    test "reads the App's credentials from the release-pr environment only" do
      assert @workflow =~ ~r/^    environment: release-pr$/m

      # The key's value reaches the mint step only; the check sees whether it is set.
      assert length(String.split(@workflow, "secrets.RELEASE_APP_PRIVATE_KEY")) == 3
      assert step(@check) =~ "${{ secrets.RELEASE_APP_PRIVATE_KEY != '' }}"
      assert mint() =~ "${{ secrets.RELEASE_APP_PRIVATE_KEY }}"
    end

    test "leaves the job's own token read-only" do
      assert [[block]] =
               Regex.scan(~r/^    permissions:\n((?:      .*\n)+)/m, @workflow,
                 capture: :all_but_first
               )

      assert block == "      contents: read\n"
    end

    test "asks for no close and reopen in the PR body" do
      refute @workflow =~ ~r/close and reopen/i
    end
  end

  # Before the App is set up, or on a fork, the run stops at its first step and names what is
  # missing. It does not fall back to GITHUB_TOKEN, whose PR would run no CI.
  describe "the release App configuration check" do
    test "runs first" do
      assert step_index(@check) == 0
    end

    test "passes with the Client ID and the key set" do
      assert {_output, 0} =
               run_step(@check, [{"CLIENT_ID", "Iv23liExample"}, {"HAS_PRIVATE_KEY", "true"}])
    end

    test "names both when neither is set" do
      assert {output, 1} = run_step(@check, [{"CLIENT_ID", ""}, {"HAS_PRIVATE_KEY", "false"}])
      assert output =~ ~r/^::error::.*RELEASE_APP_CLIENT_ID.*release-pr/m
      assert output =~ ~r/^::error::.*RELEASE_APP_PRIVATE_KEY.*release-pr/m
    end

    test "names the Client ID alone when only it is missing" do
      assert {output, 1} = run_step(@check, [{"CLIENT_ID", ""}, {"HAS_PRIVATE_KEY", "true"}])
      assert output =~ ~r/^::error::.*RELEASE_APP_CLIENT_ID/m
      refute output =~ "RELEASE_APP_PRIVATE_KEY"
    end

    test "names the key alone when only it is missing" do
      assert {output, 1} =
               run_step(@check, [{"CLIENT_ID", "Iv23liExample"}, {"HAS_PRIVATE_KEY", "false"}])

      assert output =~ ~r/^::error::.*RELEASE_APP_PRIVATE_KEY/m
      refute output =~ "RELEASE_APP_CLIENT_ID"
    end
  end

  describe "CONTRIBUTING.md's release steps" do
    test "say CI runs on the release PR through the App, with no close and reopen" do
      refute review_step() =~ ~r/close and reopen/i
      assert review_step() =~ "release App"
      assert @contributing =~ "RELEASE_APP_CLIENT_ID"
      assert @contributing =~ "RELEASE_APP_PRIVATE_KEY"
      assert @contributing =~ "`release-pr`"
    end

    # The bump moves the versions in these passages, not their meaning, so a 1.0 PR rewrites
    # all three by hand.
    test "name every pre-1.0 passage a 1.0 release rewrites" do
      assert review_step() =~ "Status section"
      assert review_step() =~ "takes patch versions only"
      assert review_step() =~ "decree is pre-1.0"
      assert review_step() =~ "SECURITY.md"
    end
  end
end
