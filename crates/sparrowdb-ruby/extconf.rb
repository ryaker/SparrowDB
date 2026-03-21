# frozen_string_literal: true

# extconf.rb — rb_sys / Magnus build configuration for sparrowdb native gem.
#
# This file is consumed by mkmf (via rb_sys) when building the native extension.
# Run `bundle exec rake build` or `gem build sparrowdb.gemspec` to compile.

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("sparrowdb/sparrowdb_ruby") do |r|
  r.features = ["ruby"]
  r.profile = ENV.fetch("RB_SYS_CARGO_PROFILE", "release").to_sym
end
