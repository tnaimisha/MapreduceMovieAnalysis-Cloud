# IMDB Movie analysis using Java Map Reduce code

This is created as a part of Cloud computing project. The goal is to test the performance of the Big Data PaaS services and Hadoop Clusters in particular, provided by various cloud platforms. We would be doing a comparison study of the below three services

1. Amazon EMR
2. Azure HDInsight
3. Google Dataproc

The datasets used for this purpose are from the IMDB dataset sampled in various sizes starting from 50MB to 4GB.
The Mapreduce code consists of two parts.

i. The output of MovieRank mapreduce code is the list of all movies sorted by their popularity which is determined by the number of reviews they have.
ii. The output of the MovieRating mapreduce code is the list of all movies along with their average ratings, sorting by the highest average rating. Movies with number of ratings fewer than 10 are ignored in this as they can skew the results.

The above two processes involved parsing, filtering, joining, aggregating and sorting operations, and would qualify as a way of benchmarking. 

Steps to execute it -

1. Create a maven project in IntelliJ
2. Enter groupID, ArtifactID, project name
3. Use the pom file provided above
4. Do import changes
5. Create a MovieRank class for Mapreduce
6. Create a MovieRating class for Mapreduce
7. Add the samples files to the resources folder.
8. Edit run configuration accordingly and mention the input and output paths
9. Run the configuration
