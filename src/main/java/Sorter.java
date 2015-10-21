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

        Processing processing = new Processing();
        SparkConf conf = new SparkConf().setAppName(Params.sparkAppName).setMaster(Params.sparkMaster);
        JavaSparkContext sc = new JavaSparkContext(conf);
        long startTime1=0, endTime1=0;
        startTime1 =  System.currentTimeMillis();


        if(args[0].equals("nameuids")){
            String filename_in = args[1];//csv file
            String dir_out = args[2];//parts dir
            processing.find_name_uids(sc, filename_in, dir_out);

        }
        else if(args[0].equals("uidnames")){
            String filename_in = args[1];//csv file
            String dir_out = args[2];//parts dir
            processing.find_uid_names(sc, filename_in, dir_out);

        }
        else if(args[0].equals("merge")){
            String in_path = args[1]+"/part*,"+args[2]+"/part*";//parts dir
            String out_path = args[3];//prts dir
            processing.merge_name_uid(sc, in_path, out_path);

        }
        else if(args[0].equals("handover")){
            String dir_in = args[1];//parts dir
            String file_out = args[2];//csv file
            processing.find_handover(dir_in, file_out);

        }
        else if(args[0].equals("changeURL")){
            String dir_in = args[1];//parts dir
            String file_out = args[2];//csv file
            processing.find_changeURL(dir_in, file_out);

        }
        else if(args[0].equals("loop_handover")){
            String file_in = args[1];//file in csv
            String file_out = args[2];//file out csv
            processing.find_loop_handover(file_in, file_out);

        }
        else if(args[0].equals("loop_changeURL")){
            String file_in = args[1];//file in csv
            String file_out = args[2];//file out csv
            processing.find_loop_changeURL(file_in, file_out);

        }


        endTime1 = System.currentTimeMillis();
        System.out.println("It took " + String.valueOf((endTime1 - startTime1)/1000) + " seconds");

    }
}
