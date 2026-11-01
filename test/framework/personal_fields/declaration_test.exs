defmodule Decree.PersonalFields.DeclarationTest do
  use ExUnit.Case, async: true

  alias Decree.Event.PersonalFields
  alias Decree.PersonalFields.Declaration
  alias Decree.PersonalFields.Fixtures.Events.{Invited, MetadataOnly, Plain, PreAuthFailed}
  alias Decree.PersonalFields.Fixtures.Events.Registered

  defp compile_event(fields, options) do
    module = :"Decree.PersonalFields.DeclarationTest.E#{System.unique_integer([:positive])}"

    Code.compile_string("""
    defmodule #{inspect(module)} do
      @derive {Decree.Event.PersonalFields, #{options}}
      defstruct #{inspect(fields)}
    end
    """)

    module
  end

  test "an event that declares nothing answers nil" do
    assert PersonalFields.declaration(%Plain{}) == nil
    assert PersonalFields.declaration(%{not: :a_struct}) == nil
  end

  test "the three subject forms and metadata reach the declaration" do
    assert PersonalFields.declaration(%Registered{}) == %Declaration{
             fields: [email: :user_id],
             metadata: [email: :user_id, password_hash: :user_id]
           }

    assert PersonalFields.declaration(%Invited{}) == %Declaration{
             fields: [email: {:first_present, [:target_user_id, :invitation_id]}],
             metadata: []
           }

    assert %Declaration{fields: [email: {:when_present, :user_id}, ip_address: _]} =
             PersonalFields.declaration(%PreAuthFailed{})
  end

  test "a metadata entry need not be a field of the struct" do
    assert %Declaration{fields: [], metadata: [password_hash: :user_id]} =
             PersonalFields.declaration(%MetadataOnly{})
  end

  for {name, fields, options, message} <- [
        {"a subject form it does not know", [:a, :b], "personal: [a: {:maybe, :b}]",
         "invalid subject"},
        {"a subject that is not an atom", [:a, :b], ~s(personal: [a: "b"]), "invalid subject"},
        {"an empty `:first_present`", [:a, :b], "personal: [a: {:first_present, []}]",
         "invalid subject"},
        {"a `:first_present` that is not atoms", [:a, :b],
         ~s(personal: [a: {:first_present, ["b"]}]), "invalid subject"},
        {"a declared field not in the struct", [:a, :b], "personal: [typo: :b]",
         "the field :typo is not in the struct"},
        {"a subject field not in the struct", [:a, :b], "personal: [a: :typo]",
         "the subject field :typo is not in the struct"},
        {"a `:when_present` subject not in the struct", [:a, :b],
         "personal: [a: {:when_present, :typo}]", "the subject field :typo"},
        {"a metadata subject not in the struct", [:a], "personal_metadata: [email: :typo]",
         "the subject field :typo"},
        {"a subject field declared personal", [:a, :b], "personal: [a: :b, b: :a]",
         "is itself declared personal"},
        {"a metadata subject declared personal", [:a, :b],
         "personal: [a: :b], personal_metadata: [email: :a]", "is itself declared personal"},
        {"empty lists", [:a], "personal: [], personal_metadata: []", "are both empty"},
        {"no list", [:a], "other: []", "unknown options"},
        {"a misspelt option", [:a, :b], "personel: [a: :b]", "unknown options"},
        {"a list that is not a keyword list", [:a, :b], "personal: [:a]",
         "must be a keyword list"},
        {"a repeated field", [:a, :b], "personal: [a: :b, a: :b]", "repeats"}
      ] do
    test "the derive refuses #{name}" do
      assert_raise CompileError, ~r/#{unquote(message)}/, fn ->
        compile_event(unquote(fields), unquote(options))
      end
    end
  end

  test "the derive refuses options that are not a keyword list" do
    assert_raise CompileError, ~r/expected a keyword list/, fn ->
      compile_event([:a], ":nope")
    end
  end
end
