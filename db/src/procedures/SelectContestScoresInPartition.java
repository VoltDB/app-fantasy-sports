package procedures;

//import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.voltdb.*;
import org.voltdb.types.TimestampType;
import org.voltdb.client.ClientResponse;

public class SelectContestScoresInPartition extends VoltProcedure {

    public final SQLStmt selectScores = new SQLStmt(
        "SELECT r.customer_id, SUM(p.score) AS score "+
        "FROM "+
        "  customer_contest_roster r "+
        "INNER JOIN nfl_contest_large c ON r.contest_id = c.contest_id "+
        "INNER JOIN nfl_player_game_score p ON r.player_id = p.player_id AND c.game_id = p.game_id "+
        "WHERE "+
        " r.contest_id = ? "+
        "GROUP BY r.customer_id "+
        "ORDER BY score DESC"+
        ";");

    

    public VoltTable[] run(int partitionVal, int contest) throws VoltAbortException {

	voltQueueSQL(selectScores, contest);
        return voltExecuteSQL(true);

    }
}
