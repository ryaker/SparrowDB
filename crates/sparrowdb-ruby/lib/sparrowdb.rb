# frozen_string_literal: true

# sparrowdb.rb — Thin Ruby wrapper around the SparrowDB native extension.
#
# Usage:
#
#   require 'sparrowdb'
#
#   db = SparrowDB::GraphDb.new('/tmp/my.db')
#
#   # Execute Cypher — returns Array of Hashes
#   rows = db.execute('MATCH (n:Person) RETURN n.name, n.age')
#   rows.each { |row| puts row }
#   # => {"n.name" => "Alice", "n.age" => 30}
#
#   # Write transaction
#   tx = db.begin_write
#   txn_id = tx.commit
#
#   # Maintenance
#   db.checkpoint
#   db.optimize

require_relative "sparrowdb/sparrowdb_ruby"

module SparrowDB
  VERSION = "0.1.0"

  class GraphDb
    # Convenience: open a database, yield it, then ensure it is checkpointed.
    #
    #   SparrowDB::GraphDb.open('/tmp/my.db') do |db|
    #     db.execute('MATCH (n) RETURN n LIMIT 1')
    #   end
    def self.open(path)
      db = new(path)
      return db unless block_given?

      begin
        yield db
      ensure
        db.checkpoint rescue nil
      end
    end
  end
end
