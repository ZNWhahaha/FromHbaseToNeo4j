package FromHbaseToNeo4j;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Main {
    //private static Logger logger = Logger.getLogger(Main.class);

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        FromHbase fromHbase = new FromHbase();
        fromHbase.setup();
        fromHbase.queryTable("PersonProfile");
        
    }
}
