import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class WordCount(var logFile: String) {

  // index 0 - RDD id
  // index 1 - first use of cached RDD
  // index 2 - was cached when re-used
  // index 3 - was partially cached when re-used
  // index 4 - not cached when reused, because app didn't cache
  // index 5 - not cached when reused, because had been evicted before re-use
  // index 6 - not cached when reused, because had been unpersisted before re-use
  // index 7 - cached, but not used because stage descendant was cached
  // index 8 - not cached, but OK because stage descendant was cached

  def test1() = {
    val whereami = System.getProperty("user.dir")
    println(whereami)

    val conf = new SparkConf().setAppName("Test1").setMaster("local[*]")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.memory.fraction", "0.5")
    conf.set("spark.executor.memory", "500m")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile).cache()
    val filtered = logData.filter(line => line.contains("a")).cache()
    val count1 = filtered.count()
    val count2 = filtered.count()

    sc.stop()
  }

  def test2() = {
    val whereami = System.getProperty("user.dir")
    println(whereami)

    val conf = new SparkConf().setAppName("Test2").setMaster("local[*]")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.memory.fraction", "0.2")
    conf.set("spark.executor.memory", "500m")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile).cache()
    val filtered = logData.filter(line => line.contains("a"))
    val count1 = filtered.count()
    val count2 = filtered.count()

    sc.stop()
  }

  def test3() = {
    val whereami = System.getProperty("user.dir")
    println(whereami)

    val conf = new SparkConf().setAppName("Test3").setMaster("local[*]")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.memory.fraction", "0.1")
    conf.set("spark.executor.memory", "500m")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile)
    val filteredA = logData.filter(line => line.contains("a")).cache()
    val filteredB = logData.filter(line => line.contains("b")).cache()
    val count1 = filteredA.count()
    val count2 = filteredB.count()
    val count3 = filteredA.count()

    sc.stop()
  }

  def test4() = {
    val whereami = System.getProperty("user.dir")
    println(whereami)

    val conf = new SparkConf().setAppName("Test4").setMaster("local[*]")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.memory.fraction", "0.3")
    conf.set("spark.executor.memory", "500m")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile)
    val filteredA = logData.filter(line => line.contains("a")).cache()
    val count1 = filteredA.count()
    filteredA.unpersist()
    val count2 = filteredA.count()

    sc.stop()
  }
}