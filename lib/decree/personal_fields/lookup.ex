defmodule Decree.PersonalFields.Lookup do
  @moduledoc false

  # The only caller of `c:Decree.PersonalFields.KeySource.lookup/1`.
  #
  # `answers` is a local map a caller threads through one append, one fold or one delivered
  # event, so a subject is looked up once in it. It holds keys, so it never goes in a struct,
  # in process state or in the process dictionary, and it ends with the caller.

  require Logger

  alias Decree.Application
  alias Decree.Store.LogTerms

  @type answer :: {:ok, String.t(), binary} | :erased | :unknown
  @type answers :: %{optional(term) => answer}

  @spec fetch(Application.t(), term, answers) :: {answer, answers}
  def fetch(application, subject_id, answers) do
    case answers do
      %{^subject_id => answer} ->
        {answer, answers}

      %{} ->
        answer = ask(application, subject_id)

        {answer, Map.put(answers, subject_id, answer)}
    end
  end

  @doc """
  Reports a key that a value could not be read with, as a lookup answering `:unknown` does.
  The telemetry event carries the subject's id, which the runbook needs to find the subject;
  the warning shows its last eight characters only, as a subject's id can be a user's own
  (sparkle-space/sparklespace#8352).
  """
  @spec unknown(Application.t(), term) :: :unknown
  def unknown(application, subject_id) when is_binary(subject_id) and subject_id != "" do
    report(application, subject_id)

    Logger.warning("no personal field key for subject #{LogTerms.suffix(subject_id)}")

    :unknown
  end

  def unknown(application, _subject_id) do
    report(application, nil)

    Logger.warning("no personal field key: the subject is not an id")

    :unknown
  end

  defp report(application, subject_id) do
    :telemetry.execute([:decree, :personal_field, :key_unknown], %{}, %{
      application: application,
      subject_id: subject_id
    })
  end

  defp ask(application, subject_id) when is_binary(subject_id) and subject_id != "" do
    case call(Application.key_source(application), subject_id) do
      {:ok, _key_id, _key} = found -> found
      :erased -> :erased
      :unknown -> unknown(application, subject_id)
    end
  end

  defp ask(application, subject_id), do: unknown(application, subject_id)

  defp call(nil, _subject_id), do: :unknown

  defp call(key_source, subject_id) do
    key_source.lookup(subject_id) |> validate()
  catch
    _kind, _value -> :unknown
  end

  defp validate({:ok, key_id, <<_::binary-size(32)>> = key})
       when is_binary(key_id) and key_id != "",
       do: {:ok, key_id, key}

  defp validate(:erased), do: :erased
  defp validate(_answer), do: :unknown
end
