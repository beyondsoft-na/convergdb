require_relative 'lib/version.rb'

Gem::Specification.new do |s|
  s.name        = 'convergdb'
  s.version     = ConvergDB::VERSION
  s.date        = '2017-12-04'
  s.summary     = "cloud data lifecycle management"
  s.description = "manages your data from ingestion to archive"
  s.authors     = ["jeremy winters"]
  s.email       = 'jeremyranierwinters@gmail.com'
  s.files       = ["lib/convergdb.rb"]
  s.executables << 'convergdb'

  Dir.glob('lib/**/*rb').each {|f| s.files << f; puts f}
  Dir.glob('lib/**/*py').each {|f| s.files << f; puts f}
  Dir.glob('lib/**/*tf').each {|f| s.files << f; puts f}

  s.homepage    = 'https://www.beyondsoft.com'
  s.license     = 'MIT'
  s.platform = 'java'

  s.add_runtime_dependency 'logger','~>1.2'
  s.add_development_dependency 'minitest','~>5.9'
  # s.add_runtime_dependency 'git','~> 1.3'
  s.add_runtime_dependency "thor",'~> 0.20'
  s.add_runtime_dependency "dotenv", '~> 2.1'
  s.add_runtime_dependency "filigree", "=0.3.3" # necessary until jruby supports ruby 2.4
  s.add_runtime_dependency "rltk", "~> 3.0"
  s.add_runtime_dependency "rainbow", '~> 3.0'
  s.add_runtime_dependency "hashdiff", '~> 0.3.7'
  s.add_runtime_dependency "aws-sdk-glue", '~> 1'
  s.add_runtime_dependency "aws-sdk-s3", '~> 1'
  s.add_development_dependency "simplecov", '~> 0.15.1'
  
  s.required_ruby_version = '~> 2.2'
end
