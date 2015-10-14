

This is for testing for the error condition of multiple levels of "member-of"

Not allowed (this is what is contained in this directory):
module-a is member-of module-b
module-b is member-of module-c

Only "one level deep" is allowed:
module-a is member-of module-c
module-b is member-of module-c
