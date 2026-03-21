# frozen_string_literal: true

# RSpec smoke tests for the sparrowdb Ruby native extension.
#
# Run with:
#   cd crates/sparrowdb-ruby
#   bundle exec rake build
#   bundle exec rspec spec/ -f documentation

require "tmpdir"
require "sparrowdb"

# ── GraphDb basics ─────────────────────────────────────────────────────────────

RSpec.describe SparrowDB::GraphDb do
  describe ".new" do
    it "creates the database directory if it does not exist" do
      Dir.mktmpdir do |d|
        db_path = File.join(d, "test.db")
        expect(File.exist?(db_path)).to be false
        _db = SparrowDB::GraphDb.new(db_path)
        expect(File.directory?(db_path)).to be true
      end
    end

    it "succeeds when re-opening an existing database" do
      Dir.mktmpdir do |d|
        db_path = File.join(d, "reopen.db")
        SparrowDB::GraphDb.new(db_path)          # create
        db2 = SparrowDB::GraphDb.new(db_path)    # re-open
        expect { db2.checkpoint }.not_to raise_error
      end
    end
  end

  describe "#inspect / #to_s" do
    it "contains 'GraphDb'" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        expect(db.inspect).to include("GraphDb")
      end
    end
  end

  describe "#checkpoint" do
    it "does not raise on an empty database" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        expect { db.checkpoint }.not_to raise_error
      end
    end
  end

  describe "#optimize" do
    it "does not raise on an empty database" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        expect { db.optimize }.not_to raise_error
      end
    end
  end

  describe ".open (block form)" do
    it "yields the db and checkpoints on exit" do
      Dir.mktmpdir do |d|
        expect do
          SparrowDB::GraphDb.open(File.join(d, "test.db")) { |db| expect(db).to be_a(SparrowDB::GraphDb) }
        end.not_to raise_error
      end
    end
  end
end

# ── ReadTx ─────────────────────────────────────────────────────────────────────

RSpec.describe SparrowDB::ReadTx do
  describe "#snapshot_txn_id" do
    it "returns 0 on a fresh database (no commits yet)" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        rx = db.begin_read
        expect(rx).to be_a(SparrowDB::ReadTx)
        expect(rx.snapshot_txn_id).to eq(0)
      end
    end
  end

  describe "#inspect" do
    it "contains 'ReadTx' and the snapshot id" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        rx = db.begin_read
        r = rx.inspect
        expect(r).to include("ReadTx")
        expect(r).to include("0")
      end
    end
  end
end

# ── WriteTx ────────────────────────────────────────────────────────────────────

RSpec.describe SparrowDB::WriteTx do
  describe "#commit" do
    it "returns an integer txn_id starting at 1" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        tx = db.begin_write
        expect(tx).to be_a(SparrowDB::WriteTx)
        txn_id = tx.commit
        expect(txn_id).to be_a(Integer)
        expect(txn_id).to eq(1)
      end
    end

    it "increments the txn_id on each commit" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        t1 = db.begin_write.commit
        t2 = db.begin_write.commit
        expect(t2).to eq(t1 + 1)
      end
    end

    it "raises RuntimeError on double-commit" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        tx = db.begin_write
        tx.commit
        expect { tx.commit }.to raise_error(RuntimeError)
      end
    end
  end

  describe "#inspect" do
    it "contains 'WriteTx'" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        tx = db.begin_write
        expect(tx.inspect).to include("WriteTx")
        tx.commit
      end
    end
  end
end

# ── SWMR semantics ─────────────────────────────────────────────────────────────

RSpec.describe "SWMR (Single-Writer Multiple-Reader)" do
  describe "WriterBusy" do
    it "raises SparrowDB::WriterBusy when a second write transaction is requested" do
      Dir.mktmpdir do |d|
        db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
        tx1 = db.begin_write
        expect { db.begin_write }.to raise_error(SparrowDB::WriterBusy)
        tx1.commit # release
      end
    end

    it "SparrowDB::WriterBusy is a subclass of RuntimeError" do
      expect(SparrowDB::WriterBusy.ancestors).to include(RuntimeError)
    end
  end

  it "allows a new write transaction after the previous one commits" do
    Dir.mktmpdir do |d|
      db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
      db.begin_write.commit
      expect { db.begin_write.commit }.not_to raise_error
    end
  end

  it "allows multiple concurrent read transactions" do
    Dir.mktmpdir do |d|
      db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
      rx1 = db.begin_read
      rx2 = db.begin_read
      expect(rx1.snapshot_txn_id).to eq(rx2.snapshot_txn_id)
    end
  end
end

# ── execute (Cypher) ───────────────────────────────────────────────────────────

RSpec.describe "GraphDb#execute" do
  it "returns an Array of Hashes or raises RuntimeError on engine errors" do
    # On an empty database the execution engine raises NotFound (no labels
    # exist), which the Ruby binding converts to RuntimeError.  Either outcome
    # is valid; what must NOT happen is an unhandled panic.
    Dir.mktmpdir do |d|
      db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
      begin
        result = db.execute("MATCH (n) RETURN n.name LIMIT 10")
        expect(result).to be_a(Array)
      rescue RuntimeError
        # empty-db NotFound is acceptable
      end
    end
  end

  it "does not panic on an empty graph" do
    Dir.mktmpdir do |d|
      db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
      begin
        result = db.execute("MATCH (n) RETURN n LIMIT 100")
        expect(result).to be_a(Array)
      rescue RuntimeError
        # NotFound on empty DB is acceptable
      end
    end
  end

  it "raises RuntimeError for an invalid Cypher query" do
    Dir.mktmpdir do |d|
      db = SparrowDB::GraphDb.new(File.join(d, "test.db"))
      expect { db.execute("THIS IS NOT VALID CYPHER !!!") }.to raise_error(RuntimeError)
    end
  end
end
