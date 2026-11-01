defmodule Decree.PersonalFields.Envelope do
  @moduledoc """
  The stored form of a personal value: a JSON object, never a string.

      {"$pf": 1, "k": "<key_id>", "s": "<subject_id>", "n": "<nonce>", "c": "<ciphertext+tag>"}

  An object can never be confused with the string a field held before, which a prefixed string
  could be for a field a person types into. `$pf` is the format version, and its presence is
  the shape test. A deserialized event body has atom keys and metadata string keys, so the test
  takes both. `k` names the data key, and `s` the subject, so that a read never has to work out
  the subject again. An id is not personal.

  ## The primitive

  AES-256-GCM through `:crypto.crypto_one_time_aead/7`, with a 16-byte tag. Each encryption
  takes a fresh random 12-byte nonce from `:crypto.strong_rand_bytes/1`. `n` and `c` are base64,
  and `c` is the ciphertext followed by the tag.

  The plaintext is the value's JSON, so a value that is not a string keeps its type.

  ## What the additional data binds

  The additional data is `"pf1"`, the key id, the stream, the event type and the field, joined
  with a NUL byte. So a value does not decrypt under another key, in another stream, in another
  event type, or in another field:

    - The stream is the source stream, `RecordedEvent.stream_id`, on every read.

    - The event type is the stored string, `EventData.event_type` and
      `RecordedEvent.event_type`, so it is unchanged by an upcaster. A snapshot's is the
      literal `"snapshot"`.

    - The field is the field's name. A metadata entry's is `"metadata.<key>"`, so a value can't
      move between body and metadata, and a snapshot's is `"<state_field>.<entry_key>"`.

  It does not bind the event id. Someone with write access to the store can copy a value
  between two events of the same type, stream and field, and it will decrypt.

  It does not bind the subject either: `s` is checked by the key it leads to, whose id must
  equal `k`.
  """

  @version 1
  @cipher :aes_256_gcm
  @nonce_bytes 12
  @tag_bytes 16
  @aad_version "pf1"

  @typedoc "Where a value is stored: its stream, its event type and its field."
  @type location :: {stream :: String.t(), event_type :: String.t(), field :: String.t()}

  @typedoc "An envelope's parts, decoded."
  @opaque t :: %{
            key_id: String.t(),
            subject_id: String.t(),
            nonce: binary,
            ciphertext: binary,
            tag: binary
          }

  @doc """
  Does this value have the envelope's shape? A map with `$pf` does, well formed or not.
  """
  @spec envelope?(term) :: boolean
  def envelope?(value) when is_map(value) do
    Map.has_key?(value, :"$pf") or Map.has_key?(value, "$pf")
  end

  def envelope?(_value), do: false

  @doc """
  Encrypts a value under `key` for the given location, with a fresh nonce.

  Answers `{:error, :personal_field_not_encodable}` for a value with no JSON form, never a raise
  that could carry it.
  """
  @spec encrypt(term, {String.t(), binary}, String.t(), location) ::
          {:ok, map} | {:error, :personal_field_not_encodable}
  def encrypt(value, {key_id, key}, subject_id, location) do
    with {:ok, plaintext} <- encode(value) do
      nonce = :crypto.strong_rand_bytes(@nonce_bytes)
      aad = aad(key_id, location)

      {ciphertext, tag} =
        :crypto.crypto_one_time_aead(@cipher, key, nonce, plaintext, aad, @tag_bytes, true)

      {:ok,
       %{
         "$pf" => @version,
         "k" => key_id,
         "s" => subject_id,
         "n" => Base.encode64(nonce),
         "c" => Base.encode64(ciphertext <> tag)
       }}
    end
  end

  @doc """
  Reads an envelope's parts. A map that has the shape and not the parts is `:error`, never a
  value: exactly `$pf` at this version, `k`, `s`, `n` and `c`, each decoding to its size.
  """
  @spec parse(map) :: {:ok, t} | :error
  def parse(value) when is_map(value) and map_size(value) == 5 do
    with @version <- part(value, :"$pf", "$pf"),
         key_id when is_binary(key_id) and key_id != "" <- part(value, :k, "k"),
         subject_id when is_binary(subject_id) and subject_id != "" <- part(value, :s, "s"),
         {:ok, nonce} <- decode(part(value, :n, "n")),
         @nonce_bytes <- byte_size(nonce),
         {:ok, sealed} <- decode(part(value, :c, "c")),
         size when size >= @tag_bytes <- byte_size(sealed) do
      {ciphertext, tag} = :erlang.split_binary(sealed, size - @tag_bytes)

      {:ok,
       %{key_id: key_id, subject_id: subject_id, nonce: nonce, ciphertext: ciphertext, tag: tag}}
    else
      _malformed -> :error
    end
  end

  def parse(_value), do: :error

  @doc """
  The key id an envelope names.
  """
  @spec key_id(t) :: String.t()
  def key_id(%{key_id: key_id}), do: key_id

  @doc """
  The subject an envelope names.
  """
  @spec subject_id(t) :: String.t()
  def subject_id(%{subject_id: subject_id}), do: subject_id

  @doc """
  Decrypts an envelope, answering `:error` when the key, the location or the bytes do not match.
  """
  @spec decrypt(t, binary, location) :: {:ok, term} | :error
  def decrypt(%{key_id: key_id} = envelope, key, location) do
    %{nonce: nonce, ciphertext: ciphertext, tag: tag} = envelope
    aad = aad(key_id, location)

    case :crypto.crypto_one_time_aead(@cipher, key, nonce, ciphertext, aad, tag, false) do
      plaintext when is_binary(plaintext) -> decode_plaintext(plaintext)
      :error -> :error
    end
  rescue
    _error -> :error
  end

  defp aad(key_id, {stream, event_type, field}) do
    Enum.join([@aad_version, key_id, stream, event_type, field], <<0>>)
  end

  defp part(value, atom, string) do
    case value do
      %{^atom => part} -> part
      %{^string => part} -> part
      %{} -> nil
    end
  end

  defp decode(part) when is_binary(part), do: Base.decode64(part)
  defp decode(_part), do: :error

  # `jason` is an optional dependency of decree. A host without it can declare nothing it can
  # store: an append refuses, and a read answers unknown.
  if Code.ensure_loaded?(Jason) do
    defp encode(value) do
      case Jason.encode(value) do
        {:ok, plaintext} -> {:ok, plaintext}
        {:error, _error} -> {:error, :personal_field_not_encodable}
      end
    catch
      _kind, _value -> {:error, :personal_field_not_encodable}
    end

    defp decode_plaintext(plaintext) do
      case Jason.decode(plaintext) do
        {:ok, value} -> {:ok, value}
        {:error, _error} -> :error
      end
    end
  else
    defp encode(_value), do: {:error, :personal_field_not_encodable}
    defp decode_plaintext(_plaintext), do: :error
  end
end
