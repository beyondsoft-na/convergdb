task :default => :test

task :test do
  require 'minitest'
  require 'minitest/ci'
  Dir.glob('./test/**/*_test.rb').each { |file| require file }
end

def shell_command(command)
  puts "executing: #{command}"
  e = `#{command}`
  puts e
end

namespace :dev do
  desc "resets dev environment... need to remove shell dependency"
  task :reset do
    e = shell_command(%{aws s3 rm s3://fakedata-target.beyondsoft.us --recursive})
    e = shell_command(%{aws s3 rm s3://fakedata-state.beyondsoft.us --recursive})
    e = shell_command(%{aws s3 rm s3://fakedata-source.beyondsoft.us --recursive})
    e = shell_command(%{aws s3 cp test/fixtures/data/books.json s3://fakedata-source.beyondsoft.us/books1.json})
    e = shell_command(%{aws s3 rm s3://fakedata.beyondsoft.us/glue_scripts/s3_to_athena_test_database_target_table_glue.py})
    e = shell_command(%{aws glue delete-job --job-name s3_to_athena_test_database_target_table})
  end
end

namespace :integration do
  desc "resets dev environment... need to remove shell dependency"
  task :test_python do
    e = shell_command(%{python test/python/python_lib_integration_tests.py})
  end

  task :test_lambdas do
    e = shell_command(%{python lib/generators/athena/modules/aws_athena_dashboard/athena_query_tracker_lambda/athena_query_tracker_test.py})
    e = shell_command(%{python lib/generators/athena/modules/aws_athena_dashboard/athena_query_reporter_lambda/athena_query_reporter_test.py})
    e = shell_command(%{node lib/generators/html_doc/modules/aws_private_s3_website/basic_auth_lambda/basic_auth_test.js})
  end
end

=begin
1. unit tests of ruby
2. unit tests of python libraries
3. integration testing
  0. testing for individual spaces CLI commands
  1. reset dev environment (needs credentials)
  2. run end to end ruby (create artifacts for data warehouse)
  3. deploy artifacts (needs credentials)
  4. run jobs in AWS (needs credentials)
  ..
  ..
  ..
=end
