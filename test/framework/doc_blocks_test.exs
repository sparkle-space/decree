defmodule Decree.DocBlocksTest do
  use ExUnit.Case, async: true

  alias Decree.DocBlocks

  test "an indented line after a blank line opens a code block" do
    doc = """
    Text.

        first = 1
        second = 2

        third = 3

    More text.

        fourth = 4
    """

    assert DocBlocks.first_lines(doc) == ["first = 1", "fourth = 4"]
  end

  test "outside a list, an indented line that opens with a list marker is code" do
    doc = """
    Text.

        - removed = 1
        + added = 2
    """

    assert DocBlocks.first_lines(doc) == ["- removed = 1"]
  end

  test "a list continuation paragraph indented four spaces is not a code block" do
    doc = """
      - `option` is the first option, wrapped
        onto a second line.

        A second paragraph of the same item, after a blank line.

          A third paragraph, indented deeper.

            - a nested item

              Its own paragraph.

    Text that ends the list.

        code = 1
    """

    assert DocBlocks.first_lines(doc) == ["code = 1"]
  end

  test "a wrapped line keeps its item, however far it is indented" do
    doc = """
      - `option` is the first option, wrapped
    onto a line at column 0.

        The item's second paragraph.
    """

    assert DocBlocks.first_lines(doc) == []
  end

  test "a list continuation paragraph straight after a code block is not part of it" do
    doc = """
      - `option` is the first option:

            inside = 1

        The item's paragraph after its code.

            again = 2
    """

    assert DocBlocks.first_lines(doc) == ["inside = 1", "again = 2"]
  end

  test "an item's code block runs while lines are blank or indented as far as its code" do
    doc = """
    - an item:

          inside = 1

          still_inside = 2

      After it.

      - a nested item:

            nested = 3

    Text that ends the list.

        top = 4

        still_top = 5
    """

    assert DocBlocks.first_lines(doc) == ["inside = 1", "nested = 3", "top = 4"]
  end
end
