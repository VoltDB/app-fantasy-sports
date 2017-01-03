/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package procedures;

//import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.voltdb.*;
import org.voltdb.types.TimestampType;
import org.voltdb.client.ClientResponse;

public class SelectContestScoresInPartition extends VoltProcedure {

    public final SQLStmt selectScores = new SQLStmt(
        "SELECT r.user_id, SUM(p.score) AS score "+
        "FROM "+
        "  user_contest_roster r "+
        "INNER JOIN nfl_contest_large c ON r.contest_id = c.contest_id "+
        "INNER JOIN nfl_player_game_score p ON r.player_id = p.player_id AND c.game_id = p.game_id "+
        "WHERE "+
        " r.contest_id = ? "+
        "GROUP BY r.user_id "+
        "ORDER BY score DESC"+
        ";");



    public VoltTable[] run(int partitionVal, int contest) throws VoltAbortException {

	voltQueueSQL(selectScores, contest);
        return voltExecuteSQL(true);

    }
}
