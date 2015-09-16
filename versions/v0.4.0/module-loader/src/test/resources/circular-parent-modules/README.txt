

This is for testing for the error condition of circular parents:

module-a has parent module-b
module-b has parent module-c
module-c has parent module-a // not allowed