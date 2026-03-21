# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name          = "sparrowdb"
  spec.version       = "0.1.0"
  spec.authors       = ["Rich Yaker"]
  spec.summary       = "Ruby bindings for SparrowDB embedded graph database"
  spec.description   = <<~DESC
    SparrowDB is a fast, embedded graph database with Cypher query support.
    This gem provides native Ruby bindings via Magnus (Rust → Ruby FFI),
    allowing Ruby and Rails applications to use SparrowDB as a local graph
    layer without a separate server process.
  DESC
  spec.license       = "MIT OR Apache-2.0"
  spec.homepage      = "https://github.com/ryaker/SparrowDB"

  spec.required_ruby_version = ">= 3.1.0"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rb,rs,toml}",
    "Cargo.toml",
    "Cargo.lock",
    "src/**/*.rs",
  ]
  spec.require_paths = ["lib"]

  spec.extensions = ["extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9"
end
