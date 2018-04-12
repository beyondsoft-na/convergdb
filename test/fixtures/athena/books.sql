create external table books_target (
  title string,
  author string,
  publisher string,
  genre string
)
stored as parquet
location 's3://fakedata-target.beyondsoft.us/'
tblproperties ('classification'='parquet');