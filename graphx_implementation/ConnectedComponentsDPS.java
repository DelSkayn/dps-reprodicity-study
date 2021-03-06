package org.apache.spark.graphx.impl;

import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.reflect.ClassTag;

public class ConnectedComponentsDPS {

	public static void main(String[] args) {
		int loop = Integer.parseInt(args[0]);
		String filename = args[1]; // twitter_rv.txt
		String sparkMaster = args[2]; // node110.ib.cluster
		String hdfsNameNode = args[3]; // node114.ib.cluster
		SparkSession ss = SparkSession.builder().master("spark://" + sparkMaster + ":7077")
				.appName("Graphx-ConnectedComponents-App-HDFS").config("spark.submit.deployMode", "cluster")
				.config("spark.driver.memory", "24G").config("spark.executor.memory", "24G")
				.config("spark.executor.cores", "32").config("spark.task.cpus", "1")
				.config("spark.driver.bindAddress", sparkMaster).config("spark.driver.host", sparkMaster)
				.config("spark.local.dir", "/local/ddps2015/spark-tmp/").getOrCreate();
		SparkContext sc = ss.sparkContext();
		System.out.println("Started Reading Graph.");
		Graph<Object, Object> graph = GraphLoader.edgeListFile(sc, "hdfs://" + hdfsNameNode + ":9000/" + filename, true,
				800, StorageLevel.DISK_ONLY(), StorageLevel.DISK_ONLY());
		System.out.println("Finished Reading Graph.");
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		for (int i = 0; i < loop; i++) {
			System.out.println("Running Connected Components Iteration:" + i);
			long startTime = System.currentTimeMillis();
			ConnectedComponents.run(graph, 1, objectTag, objectTag);
			long endTime = System.currentTimeMillis();
			long duration = (endTime - startTime);
			System.out.println("Finished running Connected Components.");
			System.out.println("Duration:" + (duration / 1000));
		}
		ss.stop();
	}
}
