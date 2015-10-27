import com.google.common.collect.Iterables;
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

        JavaPairRDD<String,List<URLdate>> name_uid = lines.mapToPair(new PairFunction<String,String,List<URLdate>>() {
            public Tuple2< String,List<URLdate>> call(String s) {
                String[] elems = s.split(",");
                //String name = elems[1];
                URLdate t = new URLdate(elems[1] , Long.parseLong(elems[0]), Integer.parseInt(elems[3]) , Integer.parseInt(elems[5]));
                List<URLdate> ts = new ArrayList<URLdate>();
                ts.add(t);
                return new Tuple2<String,List<URLdate>>(elems[2], ts);
            }
        });
        JavaPairRDD<String,List<URLdate>> uid_name_count = name_uid.reduceByKey(new Function2<List<URLdate>,List<URLdate>,List<URLdate>>() {
            public List<URLdate> call(List<URLdate> s1 , List<URLdate> s2) {
                List<URLdate> out = new ArrayList<URLdate>();
                int i1 = 0;
                int i2 = 0;
                while (i1<s1.size() && i2<s2.size()){
                    if (s1.get(i1).compareTo(s2.get(i2))==-1){
                        if(out.isEmpty() || !s1.get(i1).equals(out.get(out.size()-1)))
                            out.add(s1.get(i1));
                        i1++;
                    }
                    else{
                        if(out.isEmpty() || !s2.get(i2).equals(out.get(out.size()-1)))
                            out.add(s2.get(i2));
                        i2++;
                    }
                }
                while (i1<s1.size()){
                    if(!s1.get(i1).equals(out.get(out.size()-1)))
                        out.add(s1.get(i1));
                    i1++;
                }
                while (i2<s2.size()){
                    if(!s2.get(i2).equals(out.get(out.size()-1)))
                        out.add(s2.get(i2));
                    i2++;
                }

                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,List<URLdate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, List<URLdate>>, Boolean>() {
//            public Boolean call(Tuple2<String, List<URLdate>> in) throws Exception {
//                Boolean out = false;
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, List<URLdate>>> sus = suspicious.collect();
        //MyUtils.write_to_file_URL(sus, filename+"_changeURL.csv");
        uid_name_count.saveAsTextFile(dir_out);
        System.out.println(String.valueOf(userNum) + " users");

    }



    public void find_name_uids(JavaSparkContext sc , String filename_in, String dir_out){
        JavaRDD<String> lines = sc.textFile(filename_in);

        JavaPairRDD<String,List<IdDate>> name_uid = lines.mapToPair(new PairFunction<String,String,List<IdDate>>() {
            public Tuple2< String,List<IdDate>> call(String s) {
                String[] elems = s.split(",");
                IdDate t = new IdDate(elems[2] , Long.parseLong(elems[0]), Integer.parseInt(elems[3]) , Integer.parseInt(elems[5]));
                List<IdDate> ts = new ArrayList<IdDate>();
                ts.add(t);
                return new Tuple2<String,List<IdDate>>(elems[1], ts);
            }
        });
        JavaPairRDD<String,List<IdDate>> uid_name_count = name_uid.reduceByKey(new Function2<List<IdDate>,List<IdDate>,List<IdDate>>() {
            public List<IdDate> call(List<IdDate> s1 , List<IdDate> s2) {
                List<IdDate> out = new ArrayList<IdDate>();
                int i1 = 0;
                int i2 = 0;
                while (i1<s1.size() && i2<s2.size()){
                    if (s1.get(i1).compareTo(s2.get(i2))==-1){
                        if(out.isEmpty() || !s1.get(i1).equals(out.get(out.size()-1)))
                            out.add(s1.get(i1));
                        i1++;
                    }
                    else{
                        if(out.isEmpty() || !s2.get(i2).equals(out.get(out.size()-1)))
                            out.add(s2.get(i2));
                        i2++;
                    }
                }
                while (i1<s1.size()){
                    if(!s1.get(i1).equals(out.get(out.size()-1)))
                        out.add(s1.get(i1));
                    i1++;
                }
                while (i2<s2.size()){
                    if(!s2.get(i2).equals(out.get(out.size()-1)))
                        out.add(s2.get(i2));
                    i2++;
                }

                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,List<IdDate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, List<IdDate>>, Boolean>() {
//            public Boolean call(Tuple2<String, List<IdDate>> in) throws Exception {
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, List<IdDate>>> sus = suspicious.collect();
        //List<Tuple2<String, List<IdDate>>> sus = uid_name_count.collect();
        //MyUtils.write_to_file_Id(sus, filename_in+"_handover.csv");
        uid_name_count.saveAsTextFile(dir_out);
        System.out.println(String.valueOf(userNum) + " users");
        //return sus;

    }


    public void merge_name_uid(JavaSparkContext sc , String paths_to_parts, String out_path){

        JavaRDD<String> lines = sc.textFile(paths_to_parts);
        JavaPairRDD<String,List<IdDate>> name_uid = lines.mapToPair(new PairFunction<String,String,List<IdDate>>() {
            public Tuple2< String,List<IdDate>> call(String s) {
                s = s.replaceAll("[\\[\\]() ]","");
                String[] elems = s.split(",");
                List<IdDate> ts = new ArrayList<IdDate>();
                for (int i =0 ; i<(elems.length-1)/4 ; i++){
                    IdDate t = new IdDate(elems[i*4+1] , Long.parseLong(elems[i*4+2]), Integer.parseInt(elems[i*4+3]) , Integer.parseInt(elems[i*4+4]));
                    ts.add(t);
                }
                return new Tuple2<String,List<IdDate>>(elems[0], ts);
            }
        });
        JavaPairRDD<String,List<IdDate>> uid_name_count = name_uid.reduceByKey(new Function2<List<IdDate>,List<IdDate>,List<IdDate>>() {
            public List<IdDate> call(List<IdDate> s1 , List<IdDate> s2) {
                List<IdDate> out = new ArrayList<IdDate>();
                int i1 = 0;
                int i2 = 0;
                while (i1<s1.size() && i2<s2.size()){
                    if (s1.get(i1).compareTo(s2.get(i2))==-1){
                        if(out.isEmpty() || !s1.get(i1).equals(out.get(out.size()-1)))
                            out.add(s1.get(i1));
                        i1++;
                    }
                    else{
                        if(out.isEmpty() || !s2.get(i2).equals(out.get(out.size()-1)))
                            out.add(s2.get(i2));
                        i2++;
                    }
                }
                while (i1<s1.size()){
                    if(!s1.get(i1).equals(out.get(out.size()-1)))
                        out.add(s1.get(i1));
                    i1++;
                }
                while (i2<s2.size()){
                    if(!s2.get(i2).equals(out.get(out.size()-1)))
                        out.add(s2.get(i2));
                    i2++;
                }

                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,List<IdDate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, List<IdDate>>, Boolean>() {
//            public Boolean call(Tuple2<String, List<IdDate>> in) throws Exception {
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, List<IdDate>>> sus = suspicious.collect();
        //List<Tuple2<String, List<IdDate>>> sus = uid_name_count.collect();
        //MyUtils.write_to_file_Id(sus, filename_in+"_handover.csv");
        uid_name_count.saveAsTextFile(out_path);
        System.out.println(String.valueOf(userNum) + " users");

    }

    public void merge_uid_name(JavaSparkContext sc , String paths_to_parts, String out_path){

        JavaRDD<String> lines = sc.textFile(paths_to_parts);
        JavaPairRDD<String,List<URLdate>> name_uid = lines.mapToPair(new PairFunction<String,String,List<URLdate>>() {
            public Tuple2< String,List<URLdate>> call(String s) {
                s = s.replaceAll("[\\[\\]() ]","");
                String[] elems = s.split(",");
                List<URLdate> ts = new ArrayList<URLdate>();
                for (int i =0 ; i<(elems.length-1)/4 ; i++){
                    URLdate t = new URLdate(elems[i*4+1] , Long.parseLong(elems[i*4+2]), Integer.parseInt(elems[i*4+3]) , Integer.parseInt(elems[i*4+4]));
                    ts.add(t);
                }
                return new Tuple2<String,List<URLdate>>(elems[0], ts);
            }
        });
        JavaPairRDD<String,List<URLdate>> uid_name_count = name_uid.reduceByKey(new Function2<List<URLdate>,List<URLdate>,List<URLdate>>() {
            public List<URLdate> call(List<URLdate> s1 , List<URLdate> s2) {
                List<URLdate> out = new ArrayList<URLdate>();
                int i1 = 0;
                int i2 = 0;
                while (i1<s1.size() && i2<s2.size()){
                    if (s1.get(i1).compareTo(s2.get(i2))==-1){
                        if(out.isEmpty() || !s1.get(i1).equals(out.get(out.size()-1)))
                            out.add(s1.get(i1));
                        i1++;
                    }
                    else{
                        if(out.isEmpty() || !s2.get(i2).equals(out.get(out.size()-1)))
                            out.add(s2.get(i2));
                        i2++;
                    }
                }
                while (i1<s1.size()){
                    if(!s1.get(i1).equals(out.get(out.size()-1)))
                        out.add(s1.get(i1));
                    i1++;
                }
                while (i2<s2.size()){
                    if(!s2.get(i2).equals(out.get(out.size()-1)))
                        out.add(s2.get(i2));
                    i2++;
                }

                return out;
            }
        });

        long userNum = uid_name_count.count();
//        JavaPairRDD<String,List<URLdate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, List<URLdate>>, Boolean>() {
//            public Boolean call(Tuple2<String, List<URLdate>> in) throws Exception {
//                if (in._2().size()>1)
//                    return true;
//                return false;
//            }
//        });

        //List<Tuple2<String, List<URLdate>>> sus = suspicious.collect();
        //List<Tuple2<String, List<URLdate>>> sus = uid_name_count.collect();
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


    public static void process_handovers(List<Tuple2<String, List<IdDate>>> sus){
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
