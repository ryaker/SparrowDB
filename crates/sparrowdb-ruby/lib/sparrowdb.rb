# frozen_string_literal: true

# Thin Ruby wrapper around the native SparrowDB extension.
#
# The native extension (sparrowdb_ruby.bundle / sparrowdb_ruby.so) must be
# built with `cargo build --release` and the resulting shared library placed
# somewhere on $LOAD_PATH, or built via the extconf.rb / rb-sys workflow.
#
# Usage:
#
#   require 'sparrowdb'
#
#   db = SparrowDB::GraphDb.new('/tmp/my.db')
#   rows = db.execute('MATCH (n:Person) RETURN n.name, n.age')
#   # => [{"n.name" => "Alice", "n.age" => 30}, ...]
#
#   tx = db.begin_write
#   txn_id = tx.commit
#
#   db.checkpoint
#   db.optimize
#   db.close

begin
  # Try loading the pre-built native extension from the gem's ext/ directory.
  require 'sparrowdb_ruby'
rescue LoadError
  # Fallback: look for the extension next to this file (development builds).
  require_relative '../target/debug/libsparrowdb_ruby'
end

module SparrowDB
  VERSION = '0.1.0'
end
