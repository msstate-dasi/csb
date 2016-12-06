SVN Structure:

CyberSecurityBenchmark
|
+---branches -- This is the folder which contains subfolders, 
|   |          one each for each developer, labeled accordingly (Ex. spencer-dev)
|   |
|   +---justin-dev -- Justin's development Folder
|   |
|   \---spencer-dev -- Spencer's development Folder
|       |
|       +---workspace-python -- contains the source code written in Python,
|       |                      usually just formatting scripts.
|       |
|       \---workspace-scala -- contains the java and scala code for the Apache Spark Application
|           |
|           \---csb_GraphGen -- Main project folder, contains the source folders and the build
|               |              output directories.
|               |
|               +---src
|               |   +---main
|               |   |   +---java -- java source files go in here
|               |   |   \---scala -- scala source files go in here
|               |   |
|               \---target -- Build output directory. Contains the runnable 'csb-_.jar' file,
|                            which is the actual application that is run on the cluster with
|                            spark-submit.
|
+---tags -- Possibly used for revision numbers (Ex. 1.0-dev, 2.2-dev, etc.)
|
\---trunk -- Contains the finished application, contains working code that is used for testing.
    |
    +---StatProgramJava -- Arindam's distribution statistic parsing program. Written in Java.
    |
    \---workspace-python -- contains the sn2bro.py script which converts SNORT and Bro logs both
                            into an augmented 'conn.log' file.
