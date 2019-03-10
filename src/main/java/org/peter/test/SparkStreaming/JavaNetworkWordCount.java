/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.peter.test.SparkStreaming;

import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class JavaNetworkWordCount {

  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
    JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 8888);
    JavaDStream<String> words = lines.flatMap(x ->{
      System.out.println(x);
      String[] split = x.split(" ");
      return Arrays.asList(split).iterator();
    });
    words.print();
    JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> {
      System.out.println(s);
      return new Tuple2<>(s, 1);
    });
    pairs.print();
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> {
      System.out.println(i1 + " : " + i2);
      return i1 + i2;
    });

    wordCounts.print();
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
