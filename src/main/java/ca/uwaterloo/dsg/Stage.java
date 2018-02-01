package ca.uwaterloo.dsg;

import org.json.simple.JsonArray;
import org.json.simple.JsonObject;

import java.math.BigDecimal;
import java.util.*;

public class Stage {
    int stageId;
    String stageName;
    int numTasks;
    HashMap<Integer, RDD> stageRDDs;
    HashMap<Integer, RDD> RDDs;                             // reference to all rdds

    public Stage(JsonObject stageJson, HashMap<Integer, RDD> RDDs) {
        this.stageId = ((BigDecimal) stageJson.get("Stage ID")).intValue();
        this.stageName = (String) stageJson.get("Stage Name");
        this.numTasks = ((BigDecimal) stageJson.get("Number of Tasks")).intValue();
        this.RDDs = RDDs;

        this.stageRDDs = new HashMap<>();

        JsonArray RDDInfos = (JsonArray) stageJson.get("RDD Info");

        initializeRDD(RDDInfos);
    }

    public void initializeRDD(JsonArray RDDInfos){
        // link RDDs and initialized their status
        JsonUtil util = JsonUtil.getInstance();
        List<JsonObject> sortedRDDInfos = util.sortJsonArray(RDDInfos, "RDD ID");

        // in order to correctly update RDD usage information, iterating twice is necessary
        // 1) initializing RDDs in this stage
        // 2) updating RDD status corresponding to other RDD (iterate in tree form)

        RDD rdd = null;
        Stack<RDD> stack = new Stack<>();

        for (JsonObject rddJson : sortedRDDInfos) {
            int rddId = ((BigDecimal) rddJson.get("RDD ID")).intValue();
            if (RDDs.containsKey(rddId)) {
                // RDD has already been initialized
                rdd = this.RDDs.get(rddId);
            } else {
                // new RDD
                rdd = new RDD(rddJson);

                JsonArray parentIds = (JsonArray) rddJson.get("Parent IDs");
                int parentId;
                for (Object jsonEntry : parentIds) {
                    parentId = ((BigDecimal) jsonEntry).intValue();
                    rdd.addParent(RDDs.get(parentId));
                }

                this.RDDs.put(rdd.getId(), rdd);
            }

            stack.push(rdd);

            rdd.incrementUsage(stageId);

            this.stageRDDs.put(rdd.getId(), rdd);

        }

//        printCacheStatus("  ");
        rdd.updateUsageCounter(stageId, false, stageRDDs.keySet());

    }

    public void createPartition(int partitionId) {
        for (Map.Entry<Integer, RDD> entry : stageRDDs.entrySet()) {
            RDD rdd = entry.getValue();
            if (rdd.getPartition(partitionId) == null) {
                rdd.addPartition(partitionId, new Partition(partitionId));
            }
        }

    }

    // getter
    public int getId() {
        return stageId;
    }

    //print
    public void print() {
        print("");
    }

    public void print(String tab) {
        System.out.println(tab + "STAGE ID : " + Integer.toString(stageId));
        System.out.println(tab + "  stage name : " + stageName);
        System.out.println(tab + "  number of tasks : " + Integer.toString(numTasks));

        if (stageRDDs.size() > 0) {
            System.out.println(tab + "  < RDDS >");
            for (Map.Entry<Integer, RDD> entry : stageRDDs.entrySet()) {
                entry.getValue().print(tab + "  ");
            }
        }

    }

    public void printCacheStatus(String tab) {
        System.out.println(tab + "Stage " + Integer.toString(this.stageId));

        StringBuffer sb = new StringBuffer();
        sb.append(tab + tab + "RDDs - [ ");
        for (Map.Entry<Integer, RDD> rddEntry : stageRDDs.entrySet()) {
            sb.append(rddEntry.getValue().getId());
            sb.append(" ");
        }
        sb.append("]");
        System.out.println(sb.toString());

        if (RDDs.size() == 0) {
            return;
        }

        System.out.println(tab + tab + "< RDD >");
        for (Map.Entry<Integer, RDD> entry : stageRDDs.entrySet()) {
            RDD rdd = entry.getValue();
            rdd.printCacheStatus(tab + tab + "    ");
        }
    }

    public void printRDDStats(String tab) {
        System.out.println(tab + "Stage " + stageId);
        for (Map.Entry<Integer, RDD> entry : stageRDDs.entrySet()) {
            entry.getValue().printRDDStats(tab + "   ");
        }
    }
}
