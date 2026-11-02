defmodule Decree.BuildFixture do
  @moduledoc false

  import ExUnit.Callbacks, only: [on_exit: 1]

  # `Code.eval_string/1` is not what a build runs. `compile_to_path/3` is what `mix compile`
  # calls, and under it the module is not loaded when the check runs, which is where an export
  # test would pass every bad declaration.
  def build(name, source) do
    dir = Path.join(System.tmp_dir!(), name)
    path = Path.join(dir, "decision.ex")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    File.write!(path, source)

    {dir, Kernel.ParallelCompiler.compile_to_path([path], dir, return_diagnostics: true)}
  end
end
