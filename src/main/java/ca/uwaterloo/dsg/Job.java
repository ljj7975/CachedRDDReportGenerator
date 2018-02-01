package ca.uwaterloo.dsg;

import org.json.simple.JsonArray;
import org.json.simple.JsonObject;

import java.math.BigDecimal;
import java.util.*;

public class Job {
    int jobId;
    HashMap<Integer, Stage> stages;
    HashMap<Integer, RDD> RDDs;                             // reference to all rdds

    public Job(JsonObject jobJson, HashMap<Integer, RDD> RDDs) {
        this.jobId = ((BigDecimal) jobJson.get("Job ID")).intValue();
        this.RDDs = RDDs;

        this.stages = new HashMap<>();
        JsonArray stagesJson = (JsonArray) jobJson.get("Stage Infos");
        JsonUtil util = JsonUtil.getInstance();
        List<JsonObject> sortedStageJson = util.sortJsonArray(stagesJson, "Stage ID");

        for (JsonObject stageJson : sortedStageJson) {
            Stage stage = new Stage(stageJson, RDDs);
            this.stages.put(stage.getId(), stage);
        }
    }

    public void createPartition(int stageId, int taskId) {
        Stage stage = stages.get(stageId);
        stage.createPartition(taskId);
    }

    // getter
    public Stage getStage(int id) {
        return stages.get(id);
    }

    public int getId() {
        return jobId;
    }

    // print
    public void print() {
        print("");
    }

    public void print(String tab) {
        System.out.println(tab + "Job ID : " + Integer.toString(jobId));
        if (stages.size() > 0) {
            System.out.println(tab + "  " + "< STAGES >");
            for (Map.Entry<Integer, Stage> entry : stages.entrySet()) {
                entry.getValue().print(tab + "  ");
            }
        }

        if (RDDs.size() > 0) {
            System.out.println(tab + "  " + "< RDD >");
            for (Map.Entry<Integer, RDD> entry : RDDs.entrySet()) {
                entry.getValue().print(tab + "  ");
            }
        }
    }

    public void printCacheStatus() {
        System.out.println();
        System.out.println("~~~ cache status for job "+Integer.toString(jobId)+" ~~~");

        System.out.println("    < STAGE >");
        for (Map.Entry<Integer, Stage> stageEntry : stages.entrySet()) {
            stageEntry.getValue().printCacheStatus("    ");
        }

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println();

    }

}
