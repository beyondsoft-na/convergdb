provider "aws" {
  alias  = "east"
  region = "us-east-1"
}

# unique identifier for this module
resource "random_id" "module_id" {
  byte_length = 8
}
