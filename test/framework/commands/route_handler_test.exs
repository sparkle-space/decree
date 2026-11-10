defmodule Decree.Commands.RouteHandlerTest do
  @moduledoc """
  `__route__/1` names the module and function a dispatch calls, so a check that reads the
  router's registry (`mix decree.verify.eventless`, sparkle-space/sparklespace#7865) finds
  each routed handler without reading paths.
  """

  use ExUnit.Case, async: true

  alias Decree.Commands.AggregateRoot
  alias Decree.Commands.AggregateRouter
  alias Decree.Commands.Composite.ExampleCompositeRouter
  alias Decree.DecisionFixtures
  alias Decree.ExampleDomain.BankAccount
  alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount
  alias Decree.ExampleDomain.BankRouter
  alias Decree.ExampleDomain.OpenAccountHandler

  test "a default `dispatch Cmd, to: Agg` route calls the aggregate's `execute/2`" do
    route = AggregateRouter.__route__(AggregateRoot.Command)

    assert route[:handler] == AggregateRoot
    assert route[:function] == :execute
    assert route[:aggregate] == AggregateRoot
  end

  test "a `function:` route calls that function" do
    route = AggregateRouter.__route__(AggregateRoot.Command2)

    assert route[:handler] == AggregateRoot
    assert route[:function] == :custom_function_name
  end

  test "a `to: Handler, aggregate: Agg` route calls the handler's `handle/2`" do
    route = BankRouter.__route__(OpenAccount)

    assert route[:handler] == OpenAccountHandler
    assert route[:function] == :handle
    assert route[:aggregate] == BankAccount
  end

  test "a `decide` route calls the decision's `decide/2`" do
    route = DecisionFixtures.Router.__route__(DecisionFixtures.Claim)

    assert route[:handler] == DecisionFixtures.ClaimDecision
    assert route[:function] == :decide
    assert route[:decision] == DecisionFixtures.ClaimDecision
    assert route[:aggregate] == nil
  end

  test "a composite router answers its leaf router's route" do
    route = ExampleCompositeRouter.__route__(OpenAccount)

    assert route[:handler] == OpenAccountHandler
    assert route[:function] == :handle
    assert route[:aggregate] == BankAccount
  end
end
