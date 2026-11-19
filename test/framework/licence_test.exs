defmodule Decree.LicenceTest do
  # GitHub reads a repository's licence with licensee, which matches LICENSE against the MIT
  # text exactly: any paragraph after it makes the licence NOASSERTION. The derived-from
  # statement lives in NOTICE, ahead of the upstream MIT notices, which stay byte for byte as
  # they were copied.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  @mit """
  MIT License

  Copyright (c) 2026 sparkle.space

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
  """

  @statement """
  Portions of this software are derived from Commanded, EventStore,
  Commanded EventStore Adapter and Commanded Ecto Projections, each
  Copyright (c) Ben Smith, under the MIT License. decree as a whole is
  licensed under the MIT License in LICENSE.

  """

  @upstream_marker "decree includes code copied"

  # SHA-256 of NOTICE's upstream section, from `@upstream_marker` to the end, as it stood
  # before the derived-from statement moved in.
  @upstream_notices_sha256 "b353f8be83ba6261dce1849d61cfdbde9d768c76ebb84710acfc71b746043d8b"

  test "LICENSE is the MIT License text alone" do
    assert File.read!(Path.join(@root, "LICENSE")) == @mit
  end

  test "NOTICE is the derived-from statement, then the upstream notices unchanged" do
    notice = File.read!(Path.join(@root, "NOTICE"))

    assert String.starts_with?(notice, @statement <> @upstream_marker)

    upstream =
      binary_part(notice, byte_size(@statement), byte_size(notice) - byte_size(@statement))

    assert :crypto.hash(:sha256, upstream) |> Base.encode16(case: :lower) ==
             @upstream_notices_sha256
  end
end
