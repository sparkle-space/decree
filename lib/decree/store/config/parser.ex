defmodule Decree.Store.Config.Parser do
  @moduledoc false

  alias Decree.Store.Query

  @config_defaults [
    column_data_type: "bytea",
    correlation_id_type: "uuid",
    causation_id_type: "uuid",
    enable_hard_deletes: false,
    schema: "public",
    timeout: 15_000
  ]

  @deletable_keys [:stream_prefixes, :event_types, :tag_kinds]
  @conditional_appends_keys [:stream_prefixes, :targets]

  def parse(config) do
    config
    |> Enum.reduce(@config_defaults, fn
      {:port, value}, config ->
        Keyword.put(config, :port, get_config_integer(value))

      {:session_mode_url, value}, config ->
        parsed_value = get_config_value(value) |> parse_url()

        Keyword.put(config, :session_mode_pool, parsed_value)

      {:timeout, value}, config ->
        Keyword.put(config, :timeout, get_config_timeout(value))

      {:url, value}, config ->
        parsed_value = get_config_value(value) |> parse_url()

        Keyword.merge(config, parsed_value)

      {:deletable, value}, config ->
        Keyword.put(config, :deletable, validate_deletable!(value))

      {:conditional_appends, value}, config ->
        Keyword.put(config, :conditional_appends, validate_conditional_appends!(value))

      {key, value}, config ->
        Keyword.put(config, key, get_config_value(value))
    end)
    |> Keyword.put(:pool, Decree.Store.Config.get_pool())
  end

  # Raises, because a declaration that misses its mark refuses nothing and fails open: a
  # misspelt key, a repeated one of which only the first counts, or a module atom where its
  # string belongs. `command` cannot be declared, because the `command:` item is the one
  # exception to the refusal, so declaring it could not take effect.
  defp validate_deletable!(declared) do
    if not well_formed_deletable?(declared) do
      raise ArgumentError,
        message:
          "expected `:deletable` to be a keyword list of `:stream_prefixes`, `:event_types` " <>
            "and `:tag_kinds`, each once, each a list of non-empty strings, with no `:` in " <>
            "a tag kind, got: " <> inspect(declared)
    end

    if "command" in Keyword.get(declared, :tag_kinds, []) do
      raise ArgumentError,
        message:
          "`:deletable` cannot declare the tag kind `command`: a query's `command:` item " <>
            "is the one exception to the refusal"
    end

    declared
  end

  defp well_formed_deletable?(declared) do
    Keyword.keyword?(declared) and
      Keyword.keys(declared) -- @deletable_keys == [] and
      Keyword.keys(declared) == Enum.uniq(Keyword.keys(declared)) and
      non_empty_strings?(Keyword.get(declared, :stream_prefixes, [])) and
      non_empty_strings?(Keyword.get(declared, :event_types, [])) and
      Query.tag_kinds?(Keyword.get(declared, :tag_kinds, []))
  end

  # Raises for the reason `validate_deletable!/1` does. A declared prefix also refuses every
  # plain append to a stream starting with it, so `""` would refuse every plain append.
  defp validate_conditional_appends!(declared) do
    if not well_formed_conditional_appends?(declared) do
      raise ArgumentError,
        message:
          "expected `:conditional_appends` to be a keyword list of `:stream_prefixes`, a list " <>
            "of non-empty strings, and `:targets`, a list, each once, got: " <> inspect(declared)
    end

    declared
  end

  defp well_formed_conditional_appends?(declared) do
    Keyword.keyword?(declared) and
      Keyword.keys(declared) -- @conditional_appends_keys == [] and
      Keyword.keys(declared) == Enum.uniq(Keyword.keys(declared)) and
      non_empty_strings?(Keyword.get(declared, :stream_prefixes, [])) and
      is_list(Keyword.get(declared, :targets, []))
  end

  defp non_empty_strings?([]), do: true

  defp non_empty_strings?([string | strings]) when is_binary(string) and string != "",
    do: non_empty_strings?(strings)

  defp non_empty_strings?(_strings), do: false

  # Converts a database url into a Keyword list
  defp parse_url(url)

  defp parse_url(""), do: []

  defp parse_url(url) do
    info = URI.parse(url)

    if is_nil(info.host) do
      raise ArgumentError, message: "host is not present"
    end

    if is_nil(info.path) or not (info.path =~ ~r"^/([^/])+$") do
      raise ArgumentError, message: "path should be a database name"
    end

    destructure [username, password], info.userinfo && String.split(info.userinfo, ":")
    "/" <> database = info.path

    opts =
      Enum.reject(
        [
          username: username,
          password: password,
          database: database,
          hostname: info.host,
          port: info.port
        ],
        fn {_k, v} -> is_nil(v) end
      )

    query_opts = parse_uri_query(info)

    for {k, v} <- opts ++ query_opts,
        not is_nil(v),
        do: {k, if(is_binary(v), do: URI.decode(v), else: v)}
  end

  defp parse_uri_query(%URI{query: nil}), do: []

  defp parse_uri_query(%URI{query: query}) do
    query
    |> URI.query_decoder()
    |> Enum.reduce([], fn
      {"ssl", "true"}, acc ->
        [{:ssl, true} | acc]

      {"ssl", "false"}, acc ->
        [{:ssl, false} | acc]

      {key, value}, acc when key in ["pool_size", "queue_target", "queue_interval", "timeout"] ->
        [{String.to_atom(key), parse_integer!(key, value)} | acc]

      {key, _value}, _acc ->
        raise ArgumentError, message: "unsupported query parameter `#{key}`"
    end)
  end

  defp parse_integer!(key, value) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        raise ArgumentError,
          message: "can not parse value `#{value}` for parameter `#{key}` as an integer"
    end
  end

  defp get_config_value(value, default \\ nil)

  defp get_config_value({:system, env_var}, default) do
    case System.get_env(env_var) do
      nil -> default
      val -> val
    end
  end

  defp get_config_value({:system, env_var, default}, _default) do
    case System.get_env(env_var) do
      nil -> default
      val -> val
    end
  end

  defp get_config_value(nil, default), do: default
  defp get_config_value(value, _default), do: value

  defp get_config_integer(value, default \\ nil) do
    case get_config_value(value) do
      nil ->
        default

      n when is_integer(n) ->
        n

      n ->
        case Integer.parse(n) do
          {i, _} -> i
          :error -> default
        end
    end
  end

  defp get_config_timeout(value, default \\ nil) do
    case get_config_value(value) do
      nil ->
        default

      :infinity ->
        :infinity

      n when is_integer(n) ->
        n

      n ->
        case Integer.parse(n) do
          {i, _} -> i
          :error -> default
        end
    end
  end
end
