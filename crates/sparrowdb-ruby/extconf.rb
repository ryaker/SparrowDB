# frozen_string_literal: true

# extconf.rb — mkmf-based build script for the sparrowdb native Ruby extension.
#
# This script is invoked by `ruby extconf.rb` (or automatically by RubyGems
# when installing from source).  It uses rb-sys to compile the Rust cdylib and
# produce the platform-native Makefile.
#
# Requirements:
#   - Rust toolchain (stable or nightly)
#   - rb-sys gem:  gem install rb-sys
#
# Build:
#   cd crates/sparrowdb-ruby
#   bundle install
#   ruby extconf.rb
#   make

require 'mkmf'
require 'rb_sys/mkmf'

create_rust_makefile('sparrowdb_ruby') do |r|
  r.profile = ENV.fetch('RB_SYS_BUILD_PROFILE', 'release').to_sym
end
