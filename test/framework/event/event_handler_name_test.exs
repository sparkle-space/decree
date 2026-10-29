defmodule Decree.Event.EventHandlerNameTest do
  use Decree.PostgresCase

  alias Decree.Event.Handler

  describe "event handler name" do
    test "should parse string" do
      assert Handler.parse_name("foo") == "foo"
    end

    test "should parse atom to string" do
      assert Handler.parse_name(:foo) == ":foo"
    end

    test "should parse tuple to string" do
      assert Handler.parse_name({:foo, :bar}) == "{:foo, :bar}"
    end

    test "should parse empty string to `nil`" do
      assert Handler.parse_name("") == nil
    end

    test "should parse `nil` to `nil`" do
      assert Handler.parse_name(nil) == nil
    end
  end

  defmodule NoAppEventHandler do
    use Handler, name: __MODULE__
  end

  test "should ensure an application is provided" do
    expected_error =
      "Decree.Event.EventHandlerNameTest.NoAppEventHandler expects :application option"

    assert_raise ArgumentError, expected_error, fn ->
      NoAppEventHandler.start_link()
    end
  end

  defmodule UnnamedEventHandler do
    use Handler, application: Decree.DefaultApp
  end

  test "should ensure an event handler name is provided" do
    expected_error =
      "Decree.Event.EventHandlerNameTest.UnnamedEventHandler expects :name option"

    assert_raise ArgumentError, expected_error, fn ->
      UnnamedEventHandler.start_link()
    end
  end

  defmodule ModuleNamedEventHandler do
    use Handler, application: Decree.DefaultApp, name: __MODULE__
  end

  test "should allow using event handler module as name" do
    start_supervised!(Decree.DefaultApp)

    assert {:ok, _pid} = ModuleNamedEventHandler.start_link()
  end
end
