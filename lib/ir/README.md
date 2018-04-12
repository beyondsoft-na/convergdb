## ConvergDB Internal Represenations

The classes in this section of the code are used to create and manage the various stages of internal representation (IR). 

### Internal Representation patterns

Each IR follows the same general pattern.

* IRs are created as trees of objects representing parent/child relationships. These trees are created by a builder class which has methods designed to work cleanly with the AST traversal.
* `resolve!` method performs a lookup for any attributes of the object which need to be set. Each `resolve!` method is responsible for calling the `resolve!` method of it's children, if any.
* `validate` method is called to perform a validation of the object. Each `validate` method is responsible for calling the `validate` method of it's children, if any. These methods should raise an exception if a validation fails. 
* `validate_string_attributes` - method performs string validations based upon the hash provided by `validation_regex`.
* `validation_regex` - is a hash specific to each object, providing the details of the regex match validation to be performed.
* `structure` method returns a hash of the resolved attributes for a given object. `structure` is responsible for recursively calling the `structure` method of it's children, if any.

`structure` delivers the true IR.

### Schema and Deployment files; IR flow

The schema and deployment files contain a data warehouse definition with a user friendly syntax (similar to HCL used by Terraform). These files are the first step in the chain of representation, shown below

```
+-----------------+          +-----------------+
|                 |          |                 |
|  schema files   |          | deployment files|
|                 |          |                 |
+--------+--------+          +--------+--------+
         |                            |
         |                            |
+--------v--------+          +--------v--------+
|                 |          |                 |
|  lexer/parser   |          |  lexer/parser   |
|                 |          |                 |
+--------+--------+          +--------+--------+
         |                            |
         |                            |
+--------v--------+          +--------v--------+
|                 |          |                 |
|     builder     |          |     builder     |
|                 |          |                 |
+--------+--------+          +--------+--------+
         |                            |
         |                            |
+--------v--------+          +--------v--------+
|                 |          |                 |
|      dsd_ir     |          |      ddd_ir     |
|                 |          |                 |
+--------+--------+          +--------+--------+
         |                            |
         |                            |
         |                   +--------v--------+
         |                   |                 |
         +------------------->   primary_ir    |
                             |                 |
                             +-----------------+
```

Schema files contain logical relation definitions, with relationships between base and derived relations. These definitions are akin to DDL in a relational database.

Deployment files contain the definition for the mapping of schema definitions to physical (cloud) resources. 

Schema and deployment files follow the same basic chain to transform to their IR structures:

* Files are handled by lexer/parser... which outputs an AST
* AST is traversed passing commands to a builder class which creates the IR structure
* IR structure is resolved and validated.
* `structure` method provides the actual structure in hash form.

### PrimaryIR

The primary IR is built by passing the dsd_ir and ddd_ir to a PrimaryIR object. PrimaryIR performs the following general steps:

* Transform the dsd_ir into a hash.
* Perform a topological sort to identify any circular dependencies between the relations.
* Transform the ddd_ir into a hash.
* Augment the hash values of the ddd_ir with lookups to the dsd_r.
* Perform a topological sort to identify any circular dependencies in the ddd_ir.
