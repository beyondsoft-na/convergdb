require_relative '../../../lib/ir/dsd/dsd_ir.rb'
require_relative '../../../lib/ir/ddd/ddd_ir.rb'

f = Spaces::DSD::DSDIRBuilder.new

f.domain('business')
f.schema('inventory')

f.relation('books')
f.relation_type('base')

f.attribute('title')
f.data_type('varchar(100)')
f.field_type('dimension')
f.required('true')

f.attribute('author')
f.data_type('varchar(100)')
f.field_type('dimension')
f.required('true')

f.attribute('publisher')
f.data_type('varchar(100)')
f.field_type('dimension')
f.required('true')

f.attribute('genre')
f.data_type('varchar(100)')
f.field_type('dimension')
f.required('true')

# 
f.relation('books_target')
f.relation_type('derived')
f.relation_source('books')

f.attribute('title')
f.data_type('varchar(100)')
f.field_type('dimension')
f.expression('books.title')
f.required('true')

f.attribute('author')
f.data_type('varchar(100)')
f.field_type('dimension')
f.expression('books.author')
f.required('true')

f.attribute('publisher')
f.data_type('varchar(100)')
f.field_type('dimension')
f.expression('books.publisher')
f.required('true')

f.attribute('genre')
f.data_type('varchar(100)')
f.field_type('dimension')
f.expression('books.genre')
f.required('true')

require 'json'

f.top_level.resolve!
f.top_level.validate

dsd = File.open('test_dsd.json', 'w')
dsd.puts JSON.pretty_generate(f.top_level.structure)
dsd.close

f = Spaces::DDD::DDDIRBuilder.new

f.deployment(:s3_source, 'production')
f.attribute('region','us-west-2')

f.relation
f.attribute('dsd','business.inventory.books')
f.attribute('storage_bucket', 'fakedata-source.beyondsoft.us')
f.attribute('storage_format', 'json')

f.deployment(:athena, 'production')
f.attribute('region','us-west-2')
f.attribute('service_role','glueService')
f.attribute('temp_s3_location','s3://fakedata.beyondsoft.us/temp/')
f.attribute('script_bucket', 'fakedata-scripts.beyondsoft.us')
f.relation
f.attribute('dsd','business.inventory.books_target')
f.attribute('storage_bucket', 'fakedata-target.beyondsoft.us')
f.attribute('state_bucket', 'fakedata-state.beyondsoft.us')
f.attribute('storage_format', 'parquet')

f.top_level.resolve!
f.top_level.validate

ddd = File.open('test_ddd.json', 'w')
ddd.puts JSON.pretty_generate(f.top_level.structure)
ddd.close