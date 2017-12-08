import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class LineCount {
    String fileName;
    Function<String, Boolean> filterA;
    Function<String, Boolean> filterB;

    public void test1() {
        // Define a configuration to use to interact with Spark
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
        // Define a configuration to use to interact with Spark
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
        // Define a configuration to use to interact with Spark
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
        // Define a configuration to use to interact with Spark
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
