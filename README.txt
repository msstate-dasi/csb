SVN Structure:

CyberSecurityBenchmark
|
+---branches -- This is the folder which contains branches, currently not used (Branch-When-Needed
|               approach).
|
+---tags -- Used for mark a state of the code (i.e., release), not for active development.
|
\---trunk -- Contains the most current development code at all times. /trunk must compile all times
    |        (and it should also pass regression tests, but wait... do we have any test? :-) ).
    |
    +---csb_GraphGen -- Main Spark application, contains the Scala and Java source code with Maven
    |                   as build manager.
    |
    \---workspace-python -- Contains the sn2bro.py script which converts SNORT and Bro logs both
                            into an augmented 'conn.log' file.
