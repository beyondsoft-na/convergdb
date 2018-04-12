## Generators

This section of the code is concerned with "generator" classes that are used to create artifacts based upon the primary_ir of the relations.

A single relation can have multiple generators attached to it's primary_ir. Each of these primary_ir generator.

#### Examples of artifacts created by generators:

* Terraform configuration files
* Terraform modules
* Pyspark scripts for use in AWS Glue
* HTML and markdown documentation.

### Methods

Generator classes have the following notable methods in common:

#### `initialize`

The initialize method for every generator class accepts the following parameters:

* **structure** - primary_ir structure for a given relation.
* **terraform_builder** - a terraform builder object that is global to all generators.

#### `generate!`

Performs the actual generation of artifacts managed by the generator object, for the given relation.
