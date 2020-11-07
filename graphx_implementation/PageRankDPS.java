package org.apache.spark.graphx.impl;

import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Option;
import scala.reflect.ClassTag;

public class PageRankDPS {

	public PageRankDPS() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		int loop = Integer.parseInt(args[0]);
		String filename = args[1]; // twitter_rv.txt
		String sparkMaster = args[2]; // node110.ib.cluster
		String hdfsNameNode = args[3]; // node114.ib.cluster
		SparkSession ss = SparkSession.builder().master("spark://" + sparkMaster + ":7077")
				.appName("Graphx-Pagerank-App-HDFS").config("spark.submit.deployMode", "cluster")
				.config("spark.driver.memory", "24G").config("spark.executor.memory", "24G")
				.config("spark.executor.cores", "32").config("spark.task.cpus", "1")
				.config("spark.driver.host", sparkMaster).config("spark.local.dir", "/local/ddps2015/spark-tmp/")
				.getOrCreate();
		SparkContext sc = ss.sparkContext();
		System.out.println("Started Reading Graph.");
		Graph<Object, Object> graph = GraphLoader.edgeListFile(sc, "hdfs://" + hdfsNameNode + ":9000/" + filename, true,
				216, StorageLevel.MEMORY_AND_DISK(), StorageLevel.MEMORY_AND_DISK());
		System.out.println("Finished Reading Graph.");
		VertexRDD<Object> vertices = graph.vertices();
		vertices.cache();
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		Graph<Object, Object> preRankGraph = null;
		for (int i = 0; i < loop; i++) {
			System.out.println("Running pagerank Iteration:" + i);
			long startTime = System.currentTimeMillis();
			if (i == 0) {
				preRankGraph = PageRank.run(graph, 1, 0.15, objectTag, objectTag);
			} else {
				preRankGraph = PageRank.runWithOptionsWithPreviousPageRank(graph, 1, 0.15, Option.empty(), preRankGraph,
						objectTag, objectTag);
			}
			long endTime = System.currentTimeMillis();
			long duration = (endTime - startTime);
			System.out.println("Finished running pagerank.");
			System.out.println("Duration:" + (duration / 1000));
		}
		ss.stop();
	}
}
