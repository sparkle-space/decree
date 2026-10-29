# Used by "mix format"

locals_without_parens = [
  # Decree.Commands.Router
  dispatch: 2,
  identify: 2,
  middleware: 1,
  router: 1,
  # Decree.Projections.Ecto
  project: 2,
  project: 3
]

[
  import_deps: [:ecto, :ecto_sql, :telemetry_registry],
  inputs: [
    "{mix,.formatter}.exs",
    "{config,lib,scripts,test}/**/*.{ex,exs}"
  ],
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
