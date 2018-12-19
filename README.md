<img src="https://github.com/beyondsoft-na/convergdb/blob/master/images/convergdb.png" alt="convergdb" width="800">

# DevOps for Data

[http://convergdb.com](http://convergdb.com) - Official ConvergDB site.

Please refer to the Github wiki for install and technical documentation.


# Quick Example

Define a schema...

```
domain "inventory" {
  schema "vehicles" {
    # definition for your source data files
    relation "cars_source" {
     relation_type = base
     attributes {
      attribute "make"  { data_type = varchar(128) }
      attribute "model" { data_type = varchar(128) }
      attribute "year"  { data_type = integer }
    }
    
    # definition for your data lake table
    relation "cars" {
     relation_type = derived {
       source = "cars_source"
     }
     attributes {
      attribute "make"  { data_type = varchar(128) }
      attribute "model" { data_type = varchar(128) }
      attribute "year"  { data_type = integer }
    }
  }
}
```

Define your deployment...

```
# your source data location
s3_source "production" {
  relations {
    relation {
      dsd = "inventor.vehicles.cars"
      storage_bucket = "s3/path/to/cars/source/data"
      storage_format = "json"
    }
  }
}

# your data lake table created with a glue etl job that runs at midnight"
athena "production" {
  etl_job_name = "car_load"
  etl_job_schedule = "cron(0 0 * * ? *)"
  relations {
    relation {
      dsd = "inventor.vehicles.cars"
      storage_format = "parquet"
    }
  }
}
```