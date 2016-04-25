
Intel DAAL utilities
====================================
This module contains extensions to the Intel DAAL libraries, and
the pre-compiled Intel DAAL jars and dynamic libraries for 64-bit Linux
environments required by the TAP Analytics Toolkit.

The extensions to Intel DAAL include:
* Java wrappers for model serialization and deserialization

## Requirements
This module depends on:
* Intel DAAL 2016 Update 2


## Build libraries
To re-build the dynamic libraries for the Intel DAAL extensions:
Export the Intel DAAL environment variables to set the path to the
Intel DAAL root directory. The best way to do this is run this command
from Intel DAAL's installation directory

```
 %linux-prompt> source bin/daalvars.sh intel64
```


Build the Intel DAAL extensions used by the TAP analytics toolkit

```
 %linux-prompt> cd src/main/c++
 %linux-prompt> make all
```

This step builds the library lib/intel64_lin/libAtkDaalJavaAPI.so
