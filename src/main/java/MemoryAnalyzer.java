import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.DeserializationException;
import org.json.simple.JsonArray;
import org.json.simple.JsonObject;
import org.json.simple.Jsoner;

public class MemoryAnalyzer {
    HashMap<Integer, Job> jobs;                         // all jobs
    HashMap<Integer, RDD> RDDs;                         // all RDDs
    Job currentJob;
//    private Stage currentStage;

    private MemoryAnalyzer() {
        jobs = new HashMap<>();
        RDDs = new HashMap<>();
    }

    private void analyze(JsonArray events) {
        JsonObject event;
        for (Object json : events) {
            event = (JsonObject) json;
            String eventType = (String) event.get("Event");
            int stageId;
            switch (eventType) {
                case "SparkListenerJobStart":
                    // create a job & stages for this job
                    Job job = new Job(event, RDDs);
                    jobs.put(job.getId(), job);
                    currentJob = job;
                    break;

                case "SparkListenerStageSubmitted":
                    //update current stage
                    JsonObject stageInfoJson = (JsonObject) event.get("Stage Info");
//                    stageId = ((BigDecimal) stageInfoJson.get("Stage ID")).intValue();
//                    currentStage = currentJob.getStage(stageId);
//                    currentStage.printCacheStatus("  ");
                    break;

                case "SparkListenerTaskStart":
                    // # tasks = # partitions for each RDD
                    // create partition index for partition id
                    stageId = ((BigDecimal) event.get("Stage ID")).intValue();
                    JsonObject taskInfoJson = (JsonObject) event.get("Task Info");
                    int taskIndex = ((BigDecimal) taskInfoJson.get("Index")).intValue();
                    currentJob.createPartition(stageId, taskIndex);
                    break;

                case "SparkListenerTaskEnd":
                    // SparkListenerTaskEnd contains Updated Block information
                    // contains information regarding change in cache
                    // collect and update the current status of the status

                    JsonObject taskMetricsJson = (JsonObject) event.get("Task Metrics");
                    JsonArray updatedBlocks = (JsonArray) taskMetricsJson.get("Updated Blocks");

                    JsonUtil util = JsonUtil.getInstance();
                    List<JsonObject> sortedUpdatedBlocks = util.sortJsonArray(updatedBlocks, "Block ID");

                    for (JsonObject updatedBlock : sortedUpdatedBlocks) {
                        String blockId = (String) updatedBlock.get("Block ID");
                        String[] word = blockId.split("_");
                        if (word[0].equals("rdd")) {
                            // only interested in rdd block storage information
                            int originalRDD = Integer.parseInt(word[1]);
                            int partitionId = Integer.parseInt(word[2]);

                            RDD rdd = RDDs.get(originalRDD);
                            JsonObject statusJson = (JsonObject) updatedBlock.get("Status");
                            rdd.updatePartitionStatus(partitionId, statusJson);

//                            To check when rdd block gets evicted
//                            if (rdd.getPartition(partitionId).isCached()) {
//                                System.out.println(blockId + " is now in cache by Task " + Integer.valueOf(taskId));
//                            } else {
//                                System.out.println(blockId + " is evicted from cache by Task " + Integer.valueOf(taskId));
//                            }
                        }
                    }
                    break;

                case "SparkListenerStageCompleted":
//                    currentStage = null;
                    break;

                case "SparkListenerUnpersistRDD":
                    int rddId = ((BigDecimal) event.get("RDD ID")).intValue();
                    if (RDDs.containsKey(rddId)) {
                        // RDDs can be made but doesn't get used at all
                        // Do RDD actually gets created??
                        RDDs.get(rddId).unpersist();
                    }

//                    To check when rdd gets unpersisted
                    System.out.println(" Unpersist is called on RDD " + Integer.valueOf(rddId));
                    break;

                case "SparkListenerJobEnd":
//                    currentJob.printCacheStatus();
                    currentJob = null;
                    break;

            }
        }

//        printRDDSummaryReport();
        printRDDUsageReport();

    }

    // print
    private void printRDDSummaryReport() {
        System.out.println("======== RDD SUMMARY =======");
        System.out.println();
        System.out.println("// RDD informations for each stage and jobs");
        System.out.println();

        for (Map.Entry<Integer, Job> entry : jobs.entrySet()) {
            entry.getValue().printCacheStatus();
        }
        System.out.println();

        System.out.println("==============================");
    }

    private void printRDDUsageReport() {
        System.out.println("======== RDD USAGE =======");
        System.out.println();
        System.out.println("// index 0 - was cached when re-used");
        System.out.println("// index 1 - was partially cached when re-used");
        System.out.println("// index 2 - not cached when reused, because app didn't cache");
        System.out.println("// index 3 - not cached when reused, because had been evicted before re-use");
        System.out.println("// index 4 - not cached when reused, because had been unpersisted before re-use");
        System.out.println("// index 5 - cached, but not used because stage descendant was cached");
        System.out.println("// index 6 - not cached, but OK because stage descendant was cached");
        System.out.println();

        System.out.println("Total number of " + Integer.toString(RDDs.size()) + " RDDs");
        System.out.println();

        for (Map.Entry<Integer, RDD> entry : RDDs.entrySet()) {
            entry.getValue().printUsage();
        }
        System.out.println();
        System.out.println("==============================");
    }

    // main
    public static void main(String[] args) {
        String fileName;
        if (args.length > 0) {
            fileName = args[0];
        } else {
            // TODO :: grab spark config, find where data gets loaded, grab most recent log
            // right now, the default is /tmp/spark-events
            File folder = new File("/tmp/spark-events");
            File[] listOfFiles = folder.listFiles();

//            for (File f : listOfFiles) {
//                System.out.println(f.getName());
//            }

            fileName = "/tmp/spark-events/"+listOfFiles[listOfFiles.length-1].getName();
//            fileName = "src/main/resources/sample_log.log";

            System.out.println(fileName);
        }

        JsonArray jsonOutput = null;

        try {
            FileReader fileReader = new FileReader(fileName);
            jsonOutput = Jsoner.deserializeMany(fileReader);
        } catch (final DeserializationException caught) {
            /* Oops, json input didn't work. */
            jsonOutput = null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        MemoryAnalyzer memoryAnalyzer = new MemoryAnalyzer();
        memoryAnalyzer.analyze(jsonOutput);
    }

}
