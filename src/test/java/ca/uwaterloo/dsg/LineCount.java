import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class LineCount {
    String fileName;
    Function<String, Boolean> filterA;
    Function<String, Boolean> filterB;

    public void test1() {
        // this test case triggers
        //      index 1 - first use of cached RDD
        //      index 2 - was cached when re-used
        //      index 4 - RDD without cache annotation
        //      index 7 - cached, but not used because stage descendant was cached
        //      index 8 - not cached, but OK because stage descendant was cached

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test1");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.memory.fraction", "0.5");
        conf.set("spark.executor.memory", "500m");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(fileName).cache();

        JavaRDD<String> filtered = input.filter(filterA).cache();
        long count1 = filtered.count();
        long count2 = filtered.count();

        sc.stop();
    }

    public void test2() {
        // this test case triggers
        //      index 1 - first use of cached RDD
        //      index 3 - was partially cached when re-used
        //      index 4 - not cached when reused, because app didn't cache

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test2");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.memory.fraction", "0.2");
        conf.set("spark.executor.memory", "500m");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(fileName).cache();

        JavaRDD<String> filtered = input.filter(filterA);
        long count1 = filtered.count();
        long count2 = filtered.count();

        sc.stop();
    }

    public void test3() {
        // this test case triggers
        //      index 1 - first use of cached RDD
        //      index 4 - not cached when reused, because app didn't cache
        //      index 5 - not cached when reused, because had been evicted before re-use

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test3");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.memory.fraction", "0.1");
        conf.set("spark.executor.memory", "500m");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(fileName);

        JavaRDD<String> filteredA = input.filter(filterA).cache();
        JavaRDD<String> filteredB = input.filter(filterB).cache();
        long count1 = filteredA.count();
        long count2 = filteredB.count();
        long count3 = filteredA.count();

        sc.stop();
    }

    public void test4() {
        // this test case triggers
        //      index 1 - first use of cached RDD
        //      index 4 - not cached when reused, because app didn't cache
        //      index 6 - not cached when reused, because had been unpersisted before re-use

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test3");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.memory.fraction", "0.3");
        conf.set("spark.executor.memory", "500m");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(fileName);

        JavaRDD<String> filtered = input.filter(filterA).cache();
        long count1 = filtered.count();
        filtered.unpersist();
        long count2 = filtered.count();

        sc.stop();
    }

    public LineCount(String fileName) {
        this.fileName = fileName;

        filterA = l -> l.contains("a");
        filterB = l -> l.contains("b");
    }

}
