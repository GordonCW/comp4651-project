## Workflow
1. Build jar files with `mvn clean package` executed separately within directory `Merge_Files` and `Setup`
2. Upload 2 jar files generated and the cluster-config.json file to Amazon S3
3. Launch an Amazon EMR cluster with the configuration pointing to cluster-config.json on AWS S3.
4. Connect to the master node of Amazon EMR cluster with `ssh <hadoop@ec2-###-##-##-###.compute-1.amazonaws.com> -i <path/to/KeyPair.pem>`
5. Move the setup jar file from S3 to HDFS with `hadoop distcp s3n://<your bucket name>/<name of setup>.jar /jars/setup.jar`
6. Move the merge jar file from S3 to HDFS with `hadoop distcp s3n://<your bucket name>/<name of merge>.jar /jars/merge.jar`
7. Move the setup jar file from HDFS to local file system of master node with `hadoop fs -get /jars/setup.jar ./setup.jar`
8. Move the merge jar file from HDFS to local file system of master node with `hadoop fs -get /jars/merge.jar ./merge.jar`
9. Generate multiple small files with `hadoop jar setup.jar comp4651.project.setup.Generate <i> <j> <k> <l>`
10. Run small files merging program with `spark-submit --class comp4651.project.spark.MergeFiles --master yarn --deploy-mode client merge.jar --input "/inputs/<key1>/<key2>/" --output "/outputs/<key1>/<key2>/"`
11. Validate the output of small files merging program with `hadoop jar setup.jar comp4651.project.setup.Test <i> <j> <k> <l>`
12. Evaluate small files merging program with `hadoop jar setup.jar comp4651.project.setup.UseCase <N> <i> <j> <k>` and `hadoop jar setup.jar comp4651.project.setup.LargeFileOptimized <N> <i> <j> <k>`
