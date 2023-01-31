# CS435 PA3
Information about programming assignment 3 for CS 435.

## Running the Application
This application takes in four command-line arguments:
1. The URL for the Spark master
2. The HDFS file path for the links file (i.e., "links-simlpe-sorted.txt")
3. The HDFS file path for the titles file (i.e., "titles-sorted.txt")
4. The HDFS file path to output the results in
5. The profile to run.

The profiles that can be run are:
1. IdealizedPageRank - Performs the idealized PageRank algorithm on the input data files.
2. TaxationPageRank - Performs the taxation PageRank algorithm on the input data files.
3. SurfSubgraph - Gets the subgraph of the links which only contains articles with "surfing" in the title.
4. ModifySurfGraph - Modifies the surfing subgraph so all pages link to the "Rocky Mountain National Park" page.
5. TPageRankOnSubGraph - Gets the surfing subgraph, then performs the taxation PageRank algorithm on it.
6. TPageRankOnModifiedSubGraph - Gets the surfing subgraph, modifies it so all pages link to "Rocky Mountain National Park", then performs the taxation PageRank algorithm on it.
7. runAll - Performs both the "TPageRankOnSubGraph" and the "TPageRankOnModifiedSubGraph" profiles.

To compile the application and run one of these profiles,
copy the links and titles files to HDFS and run the commands below.
These commands assume the files are in the HDFS folder "/pa3",
and that the Spark master is running on Dover on port 30295.

Note: running the Idealized Page Rank application takes over 5 minutes,
and the Taxation Page Rank application takes over 7 minutes.

```
sbt package
spark-submit --class SurfingPageRank --deploy-mode cluster --supervise ./target/scala-2.12/pa3_2.12-1.0.jar spark://dover.cs.colostate.edu:30295 /pa3/links-simple-sorted.txt /pa3/titles-sorted.txt /pa3/output runAll
```

## Input File Format
For this assignment, we have two input files: `titles-sorted.txt` and `links-simple-sorted.txt`.
These describe various Wikipedia articles and how they link to each other.
Each article is given an ID number starting with 1.

### titles-sorted.txt
This file is just a bunch of article titles, each on their own line.
Many of the titles contain non-ASCII characters, some of which can't even be displayed normally.

The first line is for article with an ID of 1.
There are 5,716,808 articles in total.
The unzipped file is a little over 100 MB.

### links-simple-sorted.txt
This file maps one article ID to a list of other article IDs.
Each mapping is on its own line.
A mapping from article A -> B means that article A contains a hyperlink to article B.

Each mapping is formatted as follows:
1. A document ID (the "source" article)
2. A colon character (':')
3. Space
4. Another document ID (a "destination" article that the "source" article contains a hyperlink to)
5. Repeat 3 & 4 as necessary

It appears that every line contains at least one destination article,
but it would be best to avoid this assumption.

The file contains 5,706,070 lines, and is about 1 GB unzipped.
Note: this is fewer lines than there are titles,
so there are definitely some articles with no links going out.

## The PageRank Algorithm
Given a representation of pages and the links between them, the PageRank algorithm calculates the probability that a random web-surfer will end up on a particular page. Let's say we have a links file that looks like the following:

```
1: 2 4
2: 1 4
3: 1
4: 2
```

The initial probabilities that a random web-surfer will be at any of the pages is `V0 = [0.25, 0.25, 0.25, 0.25]`. The transition matrix is `M = [[0, 0.5, 1, 0], [0.5, 0, 0, 1], [0, 0, 0, 0], [0.5, 0.5, 0, 0]]`. Perform a dot product on M and V0, This should give `V1 = [0.375, 0.375, 0, 0.25]`. For the next iteration, perform a dot product on M and V1. You can perform this process as many times as you like. 

As you can see, page 3's rank was wiped out after the first iteration since there are no incoming links, which means that there is no way to get back once a surfer leaves page 3.