kmeans
======

KMeans


This is a parallel and a normal implementation of K means plus plus. The initial centroids are chosen using kmeans ++ algorithm. http://en.wikipedia.org/wiki/K-means%2B%2B. Then first the normal non parallel algorithm is run on the chosen centroids on the iris data set. After that the parallel kmeans is run using hadoop on the same centroids. Similar results have been obtained from the two algorithms.
