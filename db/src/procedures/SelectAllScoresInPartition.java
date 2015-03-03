package procedures;

//import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.voltdb.*;
import org.voltdb.types.TimestampType;
import org.voltdb.client.ClientResponse;

public class SelectAllScoresInPartition extends VoltProcedure {

    public final SQLStmt selectScores = new SQLStmt(
        "SELECT c.contest_id, r.customer_id, SUM(pgs.score) "+
        "FROM "+
        "  nfl_player_game_score pgs, "+
        "  nfl_contest_large c,"+
        "  customer_contest_roster r "+
        "WHERE "+
        " r.contest_id = c.contest_id AND "+
        " r.player_id = pgs.player_id AND "+
        " c.game_id = pgs.game_id "+
        "GROUP BY c.contest_id, r.customer_id "+
        "ORDER BY 1,2,3"+
        ";");

    

    public VoltTable[] run(int partitionVal) throws VoltAbortException {

	voltQueueSQL(selectScores);
        return voltExecuteSQL(true);

    }
}
