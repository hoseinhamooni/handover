import scala.Tuple2;

import java.util.List;
import java.util.TreeSet;

/**
 * Created by hossein on 10/2/15.
 */
public class Sorter{



    public static void main(String[] args) {
        String filename_in = args[0];
        String filename_out = args[1];
        long startTime1=0, endTime1=0;
        startTime1 =  System.currentTimeMillis();
        Processing processing = new Processing();
        processing.find_change_screenname(filename_in);
        //List<Tuple2<String, TreeSet<IdDate>>> sus = processing.find_handover(filename_in, filename_out);
        //processing.process_handovers(sus);
        endTime1 = System.currentTimeMillis();
        System.out.println("It took " + String.valueOf((endTime1 - startTime1)/1000) + " seconds to sort ");// + String.valueOf(local_sorted.size()) + " tweets");

    }
}
