from context import convergdb
from structure import *
import pytest
import json
import time

def test_get_state():
  # needs functional refactoring
  pass

def test_data_files_for_batch():
  # needs functional refactoring
  pass

def test_state_folder_prefix():
  t = convergdb.state_folder_prefix(
    structure_1()
  )
  assert t == "e969ca618e222a58/state/production.ecommerce.inventory.books"
  
def test_current_state_key():
  t = convergdb.current_state_key(
    structure_1()
  )
  assert t == "e969ca618e222a58/state/production.ecommerce.inventory.books/state.json"

def test_state_success():
  this_time = convergdb.sql_utc_timestamp(time.gmtime())
  t = convergdb.state_success(
    "201701011234123",
    this_time,
    this_time,
    structure_1(),
    this_time
  )
  
  assert t == {
    "state" : "success",
    "state_time" : this_time,
    "batch_id": "201701011234123",
    "start_time" : this_time,
    "end_time" : this_time,
    "structure" : structure_1()
  }
  
def test_state_load_in_progress():
  this_time = convergdb.sql_utc_timestamp(time.gmtime())
  t = convergdb.state_load_in_progress(
    structure_1(),
    "201701011234123",
    this_time,
    ["obj_1", "obj_2"],
    this_time
  )

  assert t == {
    "state" : "load_in_progress",
    "state_time" : this_time,
    "batch_id": "201701011234123",
    "start_time" : this_time,
    "source_objects" : ["obj_1", "obj_2"],
    "structure" : structure_1()
  }
  
def test_write_success():
  # too much state for a unit test
  pass

def test_write_load_in_progress():
  # too much state for a unit test
  pass