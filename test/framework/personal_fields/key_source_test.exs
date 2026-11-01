defmodule Decree.PersonalFields.KeySourceTest do
  use Decree.PostgresCase

  import ExUnit.CaptureLog

  alias Decree.Application
  alias Decree.PersonalFields.Fixtures.{App, EnvApp, FencedApp, Keys}
  alias Decree.PersonalFields.KeySource
  alias Decree.PersonalFields.Lookup
  alias Decree.UUID

  setup do
    start_supervised!(Keys)
    Keys.sink()

    [subject: Keys.mint(UUID.uuid4())]
  end

  describe "the behaviour" do
    test "has exactly one callback" do
      assert KeySource.behaviour_info(:callbacks) == [lookup: 1]
      assert KeySource.behaviour_info(:optional_callbacks) == []
    end
  end

  describe "the source-level options" do
    test "are read through the application, by its name" do
      start_supervised!(App)
      start_supervised!({FencedApp, name: :fenced_instance})

      assert Application.key_source(App) == Keys
      assert Application.personal_metadata_keys(App) == []
      assert Application.key_source(:fenced_instance) == Keys

      assert Application.personal_metadata_keys(:fenced_instance) == [:email, :password_hash]
    end

    test "are not taken from the application's environment" do
      assert Elixir.Application.get_env(:decree, EnvApp)[:key_source] == Keys
      assert Elixir.Application.get_env(:decree, EnvApp)[:personal_metadata_keys] == [:email]

      assert EnvApp.__key_source__() == nil
      assert EnvApp.__personal_metadata_keys__() == []
    end

    test "are not taken from `start_link/1` options, nor from `init/1`" do
      start_supervised!({EnvApp, key_source: Keys, personal_metadata_keys: [:password_hash]})

      assert Application.key_source(EnvApp) == nil
      assert Application.personal_metadata_keys(EnvApp) == []
    end

    test "do not reach the runtime config" do
      start_supervised!(App)

      refute Keyword.has_key?(App.config(), :personal_metadata_keys)
      refute Keyword.has_key?(App.config(), :key_source)
    end

    test "a `:key_source` that is not a module fails at compile time" do
      assert_raise ArgumentError, ~r/expected :key_source/, fn ->
        Code.compile_string("""
        defmodule Decree.PersonalFields.KeySourceTest.BadSource do
          use Decree.Application, otp_app: :decree, key_source: "keys"
        end
        """)
      end
    end

    test "`:personal_metadata_keys` that are not atoms fail at compile time" do
      assert_raise ArgumentError, ~r/expected :personal_metadata_keys/, fn ->
        Code.compile_string("""
        defmodule Decree.PersonalFields.KeySourceTest.BadKeys do
          use Decree.Application, otp_app: :decree, personal_metadata_keys: ["email"]
        end
        """)
      end
    end
  end

  describe "a lookup" do
    setup do
      start_supervised!(App)

      :ok
    end

    test "answers the key, and `:erased`", %{subject: subject} do
      {key_id, key} = Keys.key(subject)

      assert {{:ok, ^key_id, ^key}, %{}} = Lookup.fetch(App, subject, %{})

      Keys.set(subject, :erased)

      assert {:erased, %{}} = Lookup.fetch(App, subject, %{})
    end

    test "answers one lookup per subject for a threaded map", %{subject: subject} do
      {answer, answers} = Lookup.fetch(App, subject, %{})

      assert {^answer, ^answers} = Lookup.fetch(App, subject, answers)
      assert Keys.lookups(subject) == 1
    end

    for mode <- [:raise, :exit, :throw, :garbage, :empty_id, :other, :unknown] do
      test "is unknown, with no crash and no exception term, for #{mode}", %{subject: subject} do
        Keys.set(subject, unquote(mode))

        :telemetry.attach(
          "#{unquote(mode)}-#{subject}",
          [:decree, :personal_field, :key_unknown],
          fn _event, measurements, metadata, pid -> send(pid, {measurements, metadata}) end,
          self()
        )

        log =
          capture_log(fn ->
            assert {:unknown, _answers} = Lookup.fetch(App, subject, %{})
          end)

        :telemetry.detach("#{unquote(mode)}-#{subject}")

        assert_receive {%{}, %{application: App, subject_id: ^subject}}
        assert log =~ subject
        refute log =~ "key store down"
        refute log =~ "key_store_down"
        refute log =~ "too short"
      end
    end

    test "with no key source is unknown", %{subject: subject} do
      start_supervised!(EnvApp)

      assert {:unknown, _answers} = Lookup.fetch(EnvApp, subject, %{})
    end

    test "for a subject that is not an id is unknown, and does not name it" do
      log =
        capture_log(fn ->
          assert {:unknown, _answers} = Lookup.fetch(App, {:not, "an id"}, %{})
          assert {:unknown, _answers} = Lookup.fetch(App, "", %{})
        end)

      refute log =~ "an id\""
    end
  end
end
