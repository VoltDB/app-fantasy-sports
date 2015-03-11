package benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import org.voltdb.client.*;
import org.voltdb.*;

public class Ranker implements Runnable {

    ArrayList<Integer> partitionKeys;
    int contestId;
    Client client;

    Ranker(ArrayList<Integer> keys, int contest, Client client) {
        this.partitionKeys = keys;
        this.contestId = contest;
        this.client = client;

    }


    @Override
    public void run() {
        try {

            ArrayList<SyncCallback> callbacks = new ArrayList<SyncCallback>();
            
            for (int partVal : partitionKeys) {

                SyncCallback cb = new SyncCallback();
                callbacks.add(cb);
            
                client.callProcedure(cb,
                                     "SelectContestScoresInPartition",
                                     partVal,
                                     contestId);

            }

            // keep a queue of the VoltTables 
            PriorityQueue<ScoreTable> pq = new PriorityQueue<ScoreTable>(partitionKeys.size());
            
            for (SyncCallback cb : callbacks) {
                cb.waitForResponse();
                VoltTable t = cb.getResponse().getResults()[0];
                ScoreTable st = new ScoreTable(t);
                pq.add(st);
            }

            // initialize
            int rank = 1;
            int count = 0;
            int lastScore = 0;

            while (pq.size() > 0) {

                // get the list with the highest top score
                ScoreTable st = pq.remove();
                
                // increment rank if not the same as the last score
                count++;
                //rank++;
                if (st.score < lastScore) {
                    rank=count;
                }
                lastScore = st.score;

                // output the record
                upsertScore(st, rank);
                
                // advance table to the next score.  If there was one, add table back to the Priority Queue
                if (st.advance()) {
                    pq.add(st);
                }
                
            }

            //} catch (IOException | ProcCallException e) {
        } catch (IOException | InterruptedException e) {
            // track failures in a pretty simple way for the reporter task
            //failureCount.incrementAndGet();
            e.printStackTrace();
        }
    }   
    

    private void upsertScore(ScoreTable t, int rank) throws IOException {

        client.callProcedure(new BenchmarkCallback("USER_CONTEST_SCORE.upsert"),
                             "USER_CONTEST_SCORE.upsert",
                             t.getUserId(),
                             contestId,
                             t.score,
                             rank
                             );
        
    }
    


    // sortable wrapper of a VoltTable of scores for one partition
    class ScoreTable implements Comparable<ScoreTable>{
        public int score;
        public VoltTable t;

        ScoreTable(VoltTable table) {
            this.t = table;
            t.advanceRow();
            this.score = (int)t.getLong(1);
        }

        @Override
        public int compareTo(ScoreTable o) {
            // Descending order
            return o.score-this.score;
            //return this.score-o.score;
        }

        public int getUserId() {
            return (int)t.getLong(0);
        }
        
        public boolean advance() {
            boolean hasNext = t.advanceRow();
            if (hasNext) {
                this.score = (int)t.getLong(1);
            }
            return hasNext;
        }
    }
    

}
