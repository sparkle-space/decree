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

  # One file at a time, in the order given. A file waiting on a module lets the next one start,
  # so a check in the first runs before a module the second defines is compiled, every time.
  def build_in_order(name, sources) do
    dir = Path.join(System.tmp_dir!(), name)

    paths =
      for {_source, index} <- Enum.with_index(sources) do
        Path.join(dir, "file#{index}.ex")
      end

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    Enum.zip_with(paths, sources, &File.write!/2)

    {dir,
     Kernel.ParallelCompiler.compile_to_path(paths, dir,
       return_diagnostics: true,
       max_concurrency: 1
     )}
  end
end
