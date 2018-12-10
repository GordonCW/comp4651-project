# Files Merging Program
It is recommended to build the program using Maven.

Issuing `spark-submit --class comp4651.project.spark.MergeFiles --master yarn --deploy-mode client
<local file system path to the project jar file> --input "<input directory pattern>" --output "<output directory pattern>"`
will merge the small files specified by "<input directory pattern>" and put the result in the location specified by "<output directory pattern>".

You can add a `-d` option at the very end of the command to delete the small files after performing files merging.
