import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.TreeSet;

/**
 * Created by hossein on 10/2/15.
 */
public class Sorter{



    public static void main(String[] args) {
        if (args.length < 1){
            System.out.println("Not Enough Arguments");
            return;
        }
        SparkConf conf = new SparkConf().setAppName(Params.sparkAppName).setMaster(Params.sparkMaster);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filename_in = args[0];
        //String filename_out = args[1];
        long startTime1=0, endTime1=0;
        startTime1 =  System.currentTimeMillis();
        Processing processing = new Processing();
        processing.find_change_screenname(sc , filename_in);
        List<Tuple2<String, TreeSet<IdDate>>> sus = processing.find_handover(sc , filename_in);
        //processing.process_handovers(sus);
        MyUtils.find_loop(filename_in+"_changeURL.csv");
        MyUtils.find_loop(filename_in+"_handover.csv");
        //MyUtils.testfile();
        endTime1 = System.currentTimeMillis();
        System.out.println("It took" + String.valueOf((endTime1 - startTime1)/1000));// + " seconds to sort " + String.valueOf(local_sorted.size()) + " tweets");

    }
}
