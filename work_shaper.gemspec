# frozen_string_literal: true

require_relative "lib/work_shaper/version"

Gem::Specification.new do |spec|
  spec.name = "work_shaper"
  spec.version = WorkShaper::VERSION
  spec.authors = ["Jerry Fernholz"]
  spec.email = ["jerryf@broadvoice.com"]

  spec.summary = "Parallelize work across many threads."
  spec.description = "WorkShaper was built to parallelize the work needed to process Kafka messages."
  spec.homepage = "https://github.com/broadvoice/work-shaper"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  spec.metadata["allowed_push_host"] = "https://rubygems.org"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/broadvoice/work-shaper"
  spec.metadata["changelog_uri"] = "https://github.com/broadvoice/work-shaper/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) || f.start_with?(*%w[bin/ test/ spec/ features/ .git .circleci appveyor])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  spec.add_dependency "sorted_set", "~> 1.0"
  spec.add_dependency "concurrent-ruby", "~> 1.2"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
