package gq.erokhin.databee

import java.time.Duration

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 16.05.17
 */

class DataBeeTestUtils {
    static String DB_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL"
    static int SAMPLE_ROW_COUNT = 1000
    static Duration MAX_WAIT = Duration.ofSeconds(5)
}
