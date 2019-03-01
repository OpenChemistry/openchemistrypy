# Code container interface

The idea here is to define as simple interface that can be implemented by
container containing various codes, to allow them to be used with the OpenChemistry
framework. The interface consists of a simple CLI defined below and a could of
schemas describing what CLI should produce or consume.

## CLI

The container should support a CLI with the following options:

### Describe

This option is used to query the container for a description of the code. When the
container is run using this option it should return descriptor JSON document
conforming to the following [schema](code.schema.json), outlined the input/outputs and various metadata
associated with the code.

```
-d |--describe
```

### Input

These options should accept a space separated list of file paths to the data files
that have been mounted into the container. The particular options and formats
provides will depend on the JSON description. *TODO Need examples of other input options we would like to support ( all our current code just take the geometry ).*

```
-g | --geometry
```

### Output

This option should accept a file path that should be used to write the output
produced by the code, it should be written in the formatted specified in the
descriptor. *TODO We probably need to support multiple outputs.*

```
-o | --output

```

### Parameters


This option should accept a file path to a JSON file containing the parameters
to control the execution of the execution of the code. This is the information
that should be used to generate the appropriate input 'deck' for the
specific code. The following [schema](params.schema.json) describes this format. The basic parameters
are those that are understood by the OpenChemsitry infrastructure, such as
basis. They have enumerations defining their values, they need to be mapped
to code specfic values. The document can also contain code specific values pass by they caller.

```
-p | --parameters
```


