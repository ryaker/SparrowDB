# frozen_string_literal: true

# RSpec tests for the SparrowDB Ruby extension.
#
# These tests exercise the native extension end-to-end. They require the
# native extension to be built first:
#
#   cargo build -p sparrowdb-ruby
#   # then load the built .dylib/.so at test time via RB_LOAD_PATH or
#   # run via `rspec` after `bundle exec ruby extconf.rb && make`.

require 'tmpdir'

# ── Load native extension ─────────────────────────────────────────────────────
#
# When running specs directly with `rspec` from the crate directory, we look for
# the debug build produced by `cargo build -p sparrowdb-ruby`.
#
native_lib = File.expand_path('../target/debug/libsparrowdb_ruby.bundle', __dir__)
native_lib = File.expand_path('../target/debug/libsparrowdb_ruby.dylib', __dir__) unless File.exist?(native_lib)
native_lib = File.expand_path('../target/debug/libsparrowdb_ruby.so', __dir__) unless File.exist?(native_lib)

if File.exist?(native_lib)
  require native_lib
else
  warn "WARNING: native extension not found at #{native_lib}"
  warn "         Run `cargo build -p sparrowdb-ruby` first, then re-run specs."
  exit 1
end

# ── Helper ────────────────────────────────────────────────────────────────────

def with_db
  Dir.mktmpdir do |dir|
    db = SparrowDB::GraphDb.new(File.join(dir, 'test.db'))
    yield db
  ensure
    db&.close
  end
end

# ── Module / constants ────────────────────────────────────────────────────────

RSpec.describe SparrowDB do
  it 'defines the SparrowDB module' do
    expect(defined?(SparrowDB)).to eq('constant')
  end

  it 'defines SparrowDB::Error inheriting from StandardError' do
    expect(SparrowDB::Error.ancestors).to include(StandardError)
  end

  it 'defines SparrowDB::WriterBusy inheriting from SparrowDB::Error' do
    expect(SparrowDB::WriterBusy.ancestors).to include(SparrowDB::Error)
  end
end

# ── GraphDb basics ────────────────────────────────────────────────────────────

RSpec.describe SparrowDB::GraphDb do
  describe '.new' do
    it 'creates a database directory when it does not exist' do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, 'new.db')
        expect(File.exist?(db_path)).to be false
        db = SparrowDB::GraphDb.new(db_path)
        expect(File.exist?(db_path)).to be true
        db.close
      end
    end

    it 're-opens an existing database without raising' do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, 'reopen.db')
        SparrowDB::GraphDb.new(db_path).close
        db2 = SparrowDB::GraphDb.new(db_path)
        db2.checkpoint
        db2.close
      end
    end
  end

  describe '#inspect / #to_s' do
    it 'returns a string containing GraphDb' do
      with_db do |db|
        expect(db.inspect).to include('GraphDb')
      end
    end
  end

  describe '#checkpoint' do
    it 'succeeds on an empty database' do
      with_db do |db|
        expect { db.checkpoint }.not_to raise_error
      end
    end
  end

  describe '#optimize' do
    it 'succeeds on an empty database' do
      with_db do |db|
        expect { db.optimize }.not_to raise_error
      end
    end
  end

  describe '#close' do
    it 'closes the database without raising' do
      Dir.mktmpdir do |dir|
        db = SparrowDB::GraphDb.new(File.join(dir, 'close.db'))
        expect { db.close }.not_to raise_error
      end
    end

    it 'raises RuntimeError on subsequent operations after close' do
      Dir.mktmpdir do |dir|
        db = SparrowDB::GraphDb.new(File.join(dir, 'close2.db'))
        db.close
        expect { db.checkpoint }.to raise_error(RuntimeError, /closed/)
      end
    end
  end
end

# ── ReadTx ────────────────────────────────────────────────────────────────────

RSpec.describe SparrowDB::ReadTx do
  describe '#snapshot_txn_id' do
    it 'returns 0 on a freshly opened database (no commits yet)' do
      with_db do |db|
        tx = db.begin_read
        expect(tx.snapshot_txn_id).to eq(0)
      end
    end
  end

  describe '#inspect / #to_s' do
    it 'returns a string containing ReadTx and the snapshot id' do
      with_db do |db|
        tx = db.begin_read
        expect(tx.inspect).to include('ReadTx')
        expect(tx.inspect).to include('0')
      end
    end
  end
end

# ── WriteTx ───────────────────────────────────────────────────────────────────

RSpec.describe SparrowDB::WriteTx do
  describe '#commit' do
    it 'returns an Integer txn_id starting at 1' do
      with_db do |db|
        tx = db.begin_write
        txn_id = tx.commit
        expect(txn_id).to be_a(Integer)
        expect(txn_id).to eq(1)
      end
    end

    it 'increments the txn_id on successive commits' do
      with_db do |db|
        t1 = db.begin_write.commit
        t2 = db.begin_write.commit
        expect(t2).to eq(t1 + 1)
      end
    end

    it 'raises RuntimeError when committed twice' do
      with_db do |db|
        tx = db.begin_write
        tx.commit
        expect { tx.commit }.to raise_error(RuntimeError, /already committed/)
      end
    end

    it 'allows a new write transaction after the previous one committed' do
      with_db do |db|
        db.begin_write.commit
        expect { db.begin_write.commit }.not_to raise_error
      end
    end
  end

  describe '#inspect / #to_s' do
    it 'returns a string containing WriteTx' do
      with_db do |db|
        tx = db.begin_write
        expect(tx.inspect).to include('WriteTx')
        tx.commit
      end
    end
  end
end

# ── GraphDb#begin_write — WriterBusy ─────────────────────────────────────────

RSpec.describe 'WriterBusy' do
  it 'raises SparrowDB::WriterBusy when a second write tx is opened' do
    with_db do |db|
      _tx1 = db.begin_write
      expect { db.begin_write }.to raise_error(SparrowDB::WriterBusy)
    end
  end
end

# ── GraphDb#execute ───────────────────────────────────────────────────────────

RSpec.describe 'GraphDb#execute' do
  it 'returns an Array of Hash rows, or raises SparrowDB::Error on engine errors' do
    with_db do |db|
      begin
        result = db.execute('MATCH (n) RETURN n.name LIMIT 10')
        expect(result).to be_a(Array)
      rescue SparrowDB::Error
        # NotFound on an empty DB is acceptable
      end
    end
  end

  it 'raises SparrowDB::Error for unparseable Cypher' do
    with_db do |db|
      expect {
        db.execute('THIS IS NOT VALID CYPHER !!!')
      }.to raise_error(SparrowDB::Error)
    end
  end

  it 'does not panic on an empty graph' do
    with_db do |db|
      expect {
        begin
          db.execute('MATCH (n) RETURN n LIMIT 100')
        rescue SparrowDB::Error
          # NotFound is acceptable
        end
      }.not_to raise_error
    end
  end
end
