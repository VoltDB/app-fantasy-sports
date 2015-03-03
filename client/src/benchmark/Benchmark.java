package benchmark;

import java.util.Random;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.voltdb.*;
import org.voltdb.client.*;

public class Benchmark {

    // CONSTANTS
    private int nflPlayers = 300;
    private int customerCount = 1000000;
    private int largeContests = 10;
    private int smallContests = 0;
    

    private Random rand = new Random();
    private Client client;


    public Benchmark(String servers) throws Exception {
        client = ClientFactory.createClient();
        String[] serverArray = servers.split(",");
        for (String server : serverArray) {
            client.createConnection(server);
        }
    }


    public void init() throws Exception {

        // generate large contests
        System.out.println("Generating " + largeContests + " large contests...");
        for (int i=0; i<largeContests; i++) {
            client.callProcedure(new BenchmarkCallback("NFL_CONTEST_LARGE.upsert"),
                                 "NFL_CONTEST_LARGE.upsert",
                                 i,
                                 1
                                 );
        }

        // generate small contests
        // for (int i=0; i<smallContests; i++) {
        //     client.callProcedure(new BenchmarkCallback("NFL_CONTEST_SMALL.upsert"),
        //                          "NFL_CONTEST_SMALL.upsert",
        //                          i,
        //                          1
        //                          );
        // }
        
        // generate customers
        System.out.println("Generating " + customerCount + " customers...");
        for (int i=0; i<customerCount; i++) {
            client.callProcedure(new BenchmarkCallback("CUSTOMER.upsert"),
                                 "CUSTOMER.upsert",
                                 i,
                                 "Customer " + i
                                 );

            // TODO: roster may need to be a different table
            //int smallContest = rand.nextInt(smallContests);
            // each customer has 9 (randomly selected) players on their roster
            // for (int j=0; j<9; j++) {
            //     client.callProcedure(new BenchmarkCallback("CUSTOMER_CONTEST_ROSTER.upsert"),
            //                          "CUSTOMER_CONTEST_ROSTER.upsert",
            //                          smallContest,
            //                          i,
            //                          rand.nextInt(nflPlayers)
            //                          );
            // }

            int largeContest = rand.nextInt(largeContests);
            // each customer has 9 (randomly selected) players on their roster
            for (int j=0; j<9; j++) {
                client.callProcedure(new BenchmarkCallback("CUSTOMER_CONTEST_ROSTER.upsert"),
                                     "CUSTOMER_CONTEST_ROSTER.upsert",
                                     largeContest,
                                     i,
                                     rand.nextInt(nflPlayers)
                                     );
            }
        }

        
    }

    
    public void runBenchmark() throws Exception {

        // for Run Everywhere procedures
        ArrayList<Integer> partitionKeys = new ArrayList<Integer>();
        VoltTable partitions = client.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];
        while (partitions.advanceRow()) {
            int p = (int)partitions.getLong(1);
            partitionKeys.add(p);
            System.out.println("Partition " + partitions.getLong(0) + " has key " + p);
        }

        // Run Ranker threads (see below) in a pool
        ExecutorService executor = Executors.newFixedThreadPool(1);
        
        for (int i=0; i<1; i++) {
        
            // generate player stats
            System.out.println("Updating NFL player stats...");
            for (int p=0; p<nflPlayers; p++) {
                client.callProcedure(new BenchmarkCallback("NFL_PLAYER_GAME_SCORE.upsert"),
                                     "NFL_PLAYER_GAME_SCORE.upsert",
                                     p,
                                     1,
                                     rand.nextInt(50)
                                     );
            }


            // run everywhere
            // for (int partVal : partitionKeys) {
            //     client.callProcedure(new BenchmarkCallback("SelectAllScoresInPartition"),
            //                          "SelectAllScoresInPartition",
            //                          partVal);
            // }
            // for (int partVal : partitionKeys) {
            //     for (int c=0; c<largeContests; c++) {
                    
            //         client.callProcedure(new BenchmarkCallback("SelectContestScoresInPartition"),
            //                              "SelectContestScoresInPartition",
            //                              partVal,
            //                              c);

                    // client.callProcedure(new BenchmarkCallback("UpsertCustomerScores"),
                    //                      "UpsertCustomerScores",
                    //                      partVal,
                    //                      c);

            //     }
            // }
            client.drain();

            System.out.println("Updating scores and rankings...");
            
            for (int c=0; c<largeContests; c++) {

                // run a Ranker
                Runnable r = new Ranker(partitionKeys, c, client);
                //r.run();
                executor.execute(r);
                
            }
        }
        
        // stop the executor & wait for any threads to finish
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        
        client.drain();

        BenchmarkCallback.printAllResults();

        client.close();
    }
    
    
    public static void main(String[] args) throws Exception {

        String serverlist = "localhost";
        if (args.length > 0) { serverlist = args[0]; }
        Benchmark benchmark = new Benchmark(serverlist);
        if (args.length <= 1) {
            benchmark.init();
            benchmark.runBenchmark();
        } else {
            for (int i=1; i<args.length; i++) {
                String arg = args[i];
                if (arg.equals("init")) {
                    benchmark.init();
                }
                if (arg.equals("benchmark")) {
                    benchmark.runBenchmark();
                }
            }
        }
    }
}
