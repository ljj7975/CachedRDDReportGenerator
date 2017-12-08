import java.io.File;

public class Test {

    // index 0 - RDD id
    // index 1 - first use of cached RDD
    // index 2 - was cached when re-used
    // index 3 - was partially cached when re-used
    // index 4 - not cached when reused, because app didn't cache
    // index 5 - not cached when reused, because had been evicted before re-use
    // index 6 - not cached when reused, because had been unpersisted before re-use
    // index 7 - cached, but not used because stage descendant was cached
    // index 8 - not cached, but OK because stage descendant was cached

    public String getLogFileName() {
        File folder = new File("/tmp/spark-events");
        File[] listOfFiles = folder.listFiles();

        return "/tmp/spark-events/"+listOfFiles[listOfFiles.length-1].getName();
    }

    public static void main(String[] args) {
        Test test = new Test();

        // Test 1

        System.out.println("-- Test 1 --");

        System.out.println();
        System.out.println("testing cases:");
        System.out.println("    index 1 : first use of cached RDD");
        System.out.println("    index 2 - was cached when re-used");
        System.out.println("    index 4 - RDD without cache annotation");
        System.out.println("    index 7 - cached, but not used because stage descendant was cached");
        System.out.println("    index 8 - not cached, but OK because stage descendant was cached");
        System.out.println();

        LineCount lc = new LineCount("/tmp/wiki_test.html");
        lc.test1();

        MemoryAnalyzer memoryAnalyzer = new MemoryAnalyzer();
        memoryAnalyzer.runAnalysis(test.getLogFileName());

        System.out.println("-- Test 1 Completed --");
        System.out.println();


        // Test 2

        System.out.println("-- Test 2 --");

        System.out.println();
        System.out.println("testing cases:");
        System.out.println("    index 1 : first use of cached RDD");
        System.out.println("    index 3 - was partially cached when re-used");
        System.out.println("    index 4 - not cached when reused, because app didn't cache");
        System.out.println();

        lc = new LineCount("/tmp/wiki_test.html");
        lc.test2();

        memoryAnalyzer = new MemoryAnalyzer();
        memoryAnalyzer.runAnalysis(test.getLogFileName());

        System.out.println("-- Test 2 Completed --");
        System.out.println();


        // Test 3

        System.out.println("-- Test 3 --");

        System.out.println();
        System.out.println("testing cases:");
        System.out.println("    index 1 : first use of cached RDD");
        System.out.println("    index 4 - not cached when reused, because app didn't cache");
        System.out.println("    index 5 - not cached when reused, because had been evicted before re-use");
        System.out.println();

        lc.test3();

        memoryAnalyzer = new MemoryAnalyzer();
        memoryAnalyzer.runAnalysis(test.getLogFileName());

        System.out.println("-- Test 3 Completed --");
        System.out.println();


        // Test 4

        System.out.println("-- Test 4 --");

        System.out.println();
        System.out.println("testing cases:");
        System.out.println("    index 1 : first use of cached RDD");
        System.out.println("    index 4 - not cached when reused, because app didn't cache");
        System.out.println("    index 6 - not cached when reused, because had been unpersisted before re-use");
        System.out.println();

        lc.test4();

        memoryAnalyzer = new MemoryAnalyzer();
        memoryAnalyzer.runAnalysis(test.getLogFileName());

        System.out.println("-- Test 4 Completed --");
        System.out.println();

    }

}
