import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

/**
 * Created by hossein on 10/5/15.
 */
public class Processing implements Serializable{

    public Processing(){}

    public void find_uid_names(JavaSparkContext sc , String filename, String dir_out){
        JavaRDD<String> lines = sc.textFile(filename);

        JavaPairRDD<String,TreeSet<URLdate>> name_uid = lines.mapToPair(new PairFunction<String,String,TreeSet<URLdate>>() {
            public Tuple2< String,TreeSet<URLdate>> call(String s) {
                String[] elems = s.split(",");
                //String name = elems[1];
                URLdate t = new URLdate(elems[1] , Long.parseLong(elems[0]), Integer.parseInt(elems[3]) , Integer.parseInt(elems[5]));
                TreeSet<URLdate> ts = new TreeSet<URLdate>();
                ts.add(t);
                return new Tuple2<String,TreeSet<URLdate>>(elems[2], ts);
            }
        });
        JavaPairRDD<String,TreeSet<URLdate>> uid_name_count = name_uid.reduceByKey(new Function2<TreeSet<URLdate>,TreeSet<URLdate>,TreeSet<URLdate>>() {
            public TreeSet<URLdate> call(TreeSet<URLdate> s1 , TreeSet<URLdate> s2) {
                TreeSet<URLdate> out = new TreeSet<URLdate>(new MyDateCompURL());
                for (URLdate s : s1){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getName().equals(out.last().getName()))
                        out.add(s);
                }
                for (URLdate s : s2){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getName().equals(out.last().getName()))
                        out.add(s);
                }
                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,TreeSet<URLdate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, TreeSet<URLdate>>, Boolean>() {
//            public Boolean call(Tuple2<String, TreeSet<URLdate>> in) throws Exception {
//                Boolean out = false;
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, TreeSet<URLdate>>> sus = suspicious.collect();
        //MyUtils.write_to_file_URL(sus, filename+"_changeURL.csv");
        uid_name_count.saveAsTextFile(dir_out);
        System.out.println(String.valueOf(userNum) + " users");

    }



    public void find_name_uids(JavaSparkContext sc , String filename_in, String dir_out){
        JavaRDD<String> lines = sc.textFile(filename_in);

        JavaPairRDD<String,TreeSet<IdDate>> name_uid = lines.mapToPair(new PairFunction<String,String,TreeSet<IdDate>>() {
            public Tuple2< String,TreeSet<IdDate>> call(String s) {
                String[] elems = s.split(",");
                IdDate t = new IdDate(elems[2] , Long.parseLong(elems[0]), Integer.parseInt(elems[3]) , Integer.parseInt(elems[5]));
                TreeSet<IdDate> ts = new TreeSet<IdDate>();
                ts.add(t);
                return new Tuple2<String,TreeSet<IdDate>>(elems[1], ts);
            }
        });
        JavaPairRDD<String,TreeSet<IdDate>> uid_name_count = name_uid.reduceByKey(new Function2<TreeSet<IdDate>,TreeSet<IdDate>,TreeSet<IdDate>>() {
            public TreeSet<IdDate> call(TreeSet<IdDate> s1 , TreeSet<IdDate> s2) {
                TreeSet<IdDate> out = new TreeSet<IdDate>(new MyDateCompId());
                for (IdDate s : s1){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getId().equals(out.last().getId()))
                        out.add(s);
                }
                for (IdDate s : s2){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getId().equals(out.last().getId()))
                        out.add(s);
                }
                return out;
            }
        });

        long userNum = uid_name_count.count();
        JavaPairRDD<String,TreeSet<IdDate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, TreeSet<IdDate>>, Boolean>() {
            public Boolean call(Tuple2<String, TreeSet<IdDate>> in) throws Exception {
                if (in._2().size()>1)
                    return true;
                return false;
            }
        });

        //List<Tuple2<String, TreeSet<IdDate>>> sus = suspicious.collect();
        //List<Tuple2<String, TreeSet<IdDate>>> sus = uid_name_count.collect();
        //MyUtils.write_to_file_Id(sus, filename_in+"_handover.csv");
        uid_name_count.saveAsTextFile(dir_out);
        System.out.println(String.valueOf(userNum) + " users");
        //return sus;

    }


    public void merge_name_uid(JavaSparkContext sc , String paths_to_parts, String out_path){

        JavaRDD<String> lines = sc.textFile(paths_to_parts);
        JavaPairRDD<String,TreeSet<IdDate>> name_uid = lines.mapToPair(new PairFunction<String,String,TreeSet<IdDate>>() {
            public Tuple2< String,TreeSet<IdDate>> call(String s) {
                s = s.replaceAll("[\\[\\]() ]","");
                String[] elems = s.split(",");
                TreeSet<IdDate> ts = new TreeSet<IdDate>();
                for (int i =0 ; i<(elems.length-1)/4 ; i++){
                    IdDate t = new IdDate(elems[i*4+1] , Long.parseLong(elems[i*4+2]), Integer.parseInt(elems[i*4+3]) , Integer.parseInt(elems[i*4+4]));
                    ts.add(t);
                }
                return new Tuple2<String,TreeSet<IdDate>>(elems[0], ts);
            }
        });
        JavaPairRDD<String,TreeSet<IdDate>> uid_name_count = name_uid.reduceByKey(new Function2<TreeSet<IdDate>,TreeSet<IdDate>,TreeSet<IdDate>>() {
            public TreeSet<IdDate> call(TreeSet<IdDate> s1 , TreeSet<IdDate> s2) {
                TreeSet<IdDate> out = new TreeSet<IdDate>(new MyDateCompId());
                for (IdDate s : s1){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getId().equals(out.last().getId()))
                        out.add(s);
                }
                for (IdDate s : s2){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getId().equals(out.last().getId()))
                        out.add(s);
                }
                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,TreeSet<IdDate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, TreeSet<IdDate>>, Boolean>() {
//            public Boolean call(Tuple2<String, TreeSet<IdDate>> in) throws Exception {
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, TreeSet<IdDate>>> sus = suspicious.collect();
        //List<Tuple2<String, TreeSet<IdDate>>> sus = uid_name_count.collect();
        //MyUtils.write_to_file_Id(sus, filename_in+"_handover.csv");
        uid_name_count.saveAsTextFile(out_path);
        System.out.println(String.valueOf(userNum) + " users");

    }

    public void merge_uid_name(JavaSparkContext sc , String paths_to_parts, String out_path){

        JavaRDD<String> lines = sc.textFile(paths_to_parts);
        JavaPairRDD<String,TreeSet<URLdate>> name_uid = lines.mapToPair(new PairFunction<String,String,TreeSet<URLdate>>() {
            public Tuple2< String,TreeSet<URLdate>> call(String s) {
                s = s.replaceAll("[\\[\\]() ]","");
                String[] elems = s.split(",");
                TreeSet<URLdate> ts = new TreeSet<URLdate>();
                for (int i =0 ; i<(elems.length-1)/4 ; i++){
                    URLdate t = new URLdate(elems[i*4+1] , Long.parseLong(elems[i*4+2]), Integer.parseInt(elems[i*4+3]) , Integer.parseInt(elems[i*4+4]));
                    ts.add(t);
                }
                return new Tuple2<String,TreeSet<URLdate>>(elems[0], ts);
            }
        });
        JavaPairRDD<String,TreeSet<URLdate>> uid_name_count = name_uid.reduceByKey(new Function2<TreeSet<URLdate>,TreeSet<URLdate>,TreeSet<URLdate>>() {
            public TreeSet<URLdate> call(TreeSet<URLdate> s1 , TreeSet<URLdate> s2) {
                TreeSet<URLdate> out = new TreeSet<URLdate>(new MyDateCompURL());
                for (URLdate s : s1){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getName().equals(out.last().getName()))
                        out.add(s);
                }
                for (URLdate s : s2){
                    //if (!out.contains(s))
                    if (out.isEmpty() || !s.getName().equals(out.last().getName()))
                        out.add(s);
                }
                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,TreeSet<URLdate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, TreeSet<URLdate>>, Boolean>() {
//            public Boolean call(Tuple2<String, TreeSet<URLdate>> in) throws Exception {
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, TreeSet<URLdate>>> sus = suspicious.collect();
        //List<Tuple2<String, TreeSet<URLdate>>> sus = uid_name_count.collect();
        //MyUtils.write_to_file_Id(sus, filename_in+"_handover.csv");
        uid_name_count.saveAsTextFile(out_path);
        System.out.println(String.valueOf(userNum) + " users");

    }

    public void find_handover (String dir_in, String file_out){
        FileOutputStream wrt;
        String[] inFiles = MyUtils.get_list_of_files(dir_in);
        try {
            wrt = new FileOutputStream(file_out);
            for (int i=0 ; i<inFiles.length ; i++){
                BufferedReader br = new BufferedReader(new FileReader(dir_in+"/"+inFiles[i]));
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.split(",").length>5){
                        wrt.write((line.replaceAll("[\\[\\]() ]", "") + "\n").getBytes());
                    }
                }
            }



        }catch(Exception e){
            System.out.println("Error: " + e.getMessage());
        }



    }
    public void find_changeURL (String dir_in, String file_out){
        FileOutputStream wrt;
        String[] inFiles = MyUtils.get_list_of_files(dir_in);
        try {
            wrt = new FileOutputStream(file_out);
            for (int i=0 ; i<inFiles.length ; i++){
                BufferedReader br = new BufferedReader(new FileReader(dir_in+"/"+inFiles[i]));
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.split(",").length>5){
                        wrt.write((line.replaceAll("[\\[\\]() ]", "") + "\n").getBytes());
                    }
                }
            }



        }catch(Exception e){
            System.out.println("Error: " + e.getMessage());
        }

    }

    /*  The code to find loops in handovers*/
    public void find_loop_handover(String filename_in, String filename_out){
        FileOutputStream wrt;

        try{
            wrt = new FileOutputStream(filename_out);
            List<String> names = new ArrayList<String>();
            BufferedReader br = new BufferedReader(new FileReader(filename_in));
            String line;
            while ((line = br.readLine()) != null) {
                names.clear();
                String[] tokens = line.split(",");
                for (int i = 0 ; i<(tokens.length-1)/4 ; i++){
                    if (names.contains(tokens[i*4+1])){
                        wrt.write((line+"\n").getBytes());
                        break;
                    }
                    names.add(tokens[i*4+1]);
                }
            }
        }catch(Exception e){
            System.out.println("Error in reading the file: find_loop: " + e.getMessage());
        }


    }

    /*  The code to find loops in handovers*/
    public void find_loop_changeURL(String filename_in, String filename_out){
        FileOutputStream wrt;

        try{
            wrt = new FileOutputStream(filename_out);
            List<String> names = new ArrayList<String>();
            BufferedReader br = new BufferedReader(new FileReader(filename_in));
            String line;
            while ((line = br.readLine()) != null) {
                names.clear();
                String[] tokens = line.split(",");
                for (int i = 0 ; i<(tokens.length-1)/4 ; i++){
                    if (names.contains(tokens[i*4+1])){
                        wrt.write((line + "\n").getBytes());
                        break;
                    }
                    names.add(tokens[i*4+1]);
                }
            }
        }catch(Exception e){
            System.out.println("Error in reading the file: find_loop: " + e.getMessage());
        }


    }


    public static List<Tuple2<Long,String>> sort_tweet_by_time(String fileName){
        SparkConf conf = new SparkConf().setAppName(Params.sparkAppName).setMaster(Params.sparkMaster);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(fileName);
        JavaPairRDD<Long,String> twt_ts = lines.mapToPair(new PairFunction<String, Long,String>() {
            public Tuple2< Long,String> call(String s) {
                String[] elems = s.split(",");
                return new Tuple2<Long,String>(Long.parseLong(elems[0].substring(2)), elems[1]+","+elems[2]);
            }
        });
        JavaPairRDD<Long,String> sorted_twt_ts = twt_ts.sortByKey(true);
        List<Tuple2<Long,String>> local_sorted = sorted_twt_ts.collect();
        return local_sorted;
    }


    public static void process_handovers(List<Tuple2<String, TreeSet<IdDate>>> sus){
        List<Integer> follower_diff = new ArrayList<Integer>();
        List<Integer> tweet_diff = new ArrayList<Integer>();
        for (int i=0; i<sus.size() ; i++){
            for (int j=1 ; j<sus.get(i)._2().size() ; j++){
                Iterator itr = sus.get(i)._2().iterator();
                IdDate base = (IdDate)itr.next();
                while(itr.hasNext()){
                    IdDate tail = (IdDate)itr.next();
                    follower_diff.add(tail.getFollower()-base.getFollower());
                    tweet_diff.add(tail.getTweets()-base.getTweets());
                    base = tail;
                }
            }

        }
        MyUtils.write_to_file_int(follower_diff, "follower_diff", ',');
        MyUtils.write_to_file_int(tweet_diff,"tweet_diff" , ',');
    }




    ///////////////////////////////////////////////////////Internal CLASSES///////////////////////////////////////////
    class MyDateCompId implements Serializable,Comparator<IdDate> {

        public int compare(IdDate e1, IdDate e2) {
            if(e1.getId().equals(e2.getId())){
                return 0;
            }
            if(e1.getDate() > e2.getDate()){
                return 1;
            } else {
                return -1;
            }
        }
    }

    class MyDateCompURL implements Serializable,Comparator<URLdate> {

        public int compare(URLdate e1, URLdate e2) {
            if(e1.getName().equals(e2.getName())){
                return 0;
            }
            if(e1.getDate() > e2.getDate()){
                return 1;
            } else {
                return -1;
            }
        }
    }


}
