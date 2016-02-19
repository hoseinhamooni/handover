import com.google.common.collect.Iterables;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.*;
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

    public static void create_min_max_handover (JavaSparkContext sc, String handover_file, String ni_dir, String out_file){
        List<String> wanted = MyUtils.get_csv_column(handover_file , 0);
        final Broadcast<List<String>> br_wanted = sc.broadcast(wanted);

        JavaRDD<String> lines = sc.textFile(ni_dir);//+"/*.ni");
        JavaRDD<String> sus = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                String[] tokens = s.split(",");
                if (br_wanted.value().contains(tokens[1]))
                    return true;
                return false;
            }

        });

        JavaPairRDD<String,Tuple2<ShTweet,ShTweet>> key_shtweet = sus.mapToPair(new PairFunction<String, String, Tuple2<ShTweet,ShTweet>>() {
            public Tuple2<String, Tuple2<ShTweet,ShTweet>> call(String s) {
                String[] elems = s.split(",");
                String key = elems[1]+","+elems[2]; //URL + UID
                ShTweet t = new ShTweet(Long.parseLong(elems[0]), Integer.parseInt(elems[3]),Integer.parseInt(elems[4]),Integer.parseInt(elems[5]));
                return new Tuple2<String, Tuple2<ShTweet,ShTweet>>(key, new Tuple2<ShTweet, ShTweet>(t,t));
            }
        });
        JavaPairRDD<String,Tuple2<ShTweet,ShTweet>> key_shtweet_reduced = key_shtweet.reduceByKey(new Function2<Tuple2<ShTweet, ShTweet>, Tuple2<ShTweet, ShTweet>, Tuple2<ShTweet, ShTweet>>() {
            public Tuple2<ShTweet, ShTweet> call(Tuple2<ShTweet, ShTweet> t1, Tuple2<ShTweet, ShTweet> t2) throws Exception {
                ShTweet min;
                ShTweet max;
                if (t1._1.compareTo(t2._1)<0)
                    min = t1._1;
                else
                    min = t2._1;

                if (t1._2.compareTo(t2._2)>0)
                    max = t1._2;
                else
                    max = t2._2;

                return new Tuple2<ShTweet, ShTweet>(min,max);
            }
        });

        List<Tuple2<String,Tuple2<ShTweet,ShTweet>>> info = key_shtweet_reduced.collect();

        FileOutputStream wrt;
        HashMap<String , Tuple2<ShTweet,ShTweet>> map = new HashMap<String, Tuple2<ShTweet, ShTweet>>();

        try {
            //wrt = new FileOutputStream(out_file);

            for (Tuple2<String, Tuple2<ShTweet,ShTweet>> t : info){
                map.put(t._1 , t._2);
                //wrt.write((t._1+","+t._2._1.toString()+","+t._2._2.toString()+'\n').getBytes());
            }
        }
        catch (Exception e){
            System.out.println("Error in writing min max file");
        }


        String key="";
        try{
            File file = new File("mymap");
            FileOutputStream fmap = new FileOutputStream(file);
            for (String k : map.keySet()){
                fmap.write((k+","+map.get(k)._1.toString()+","+map.get(k)._2.toString()+"\n").getBytes());
            }
            fmap.close();


            wrt = new FileOutputStream(out_file);
            BufferedReader br = new BufferedReader(new FileReader(handover_file));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                String url = tokens[0];
                wrt.write((url+",").getBytes());
                for (int i=0 ; i<(tokens.length-1)/4 ; i++){
                    key = url + "," + tokens[i*4+1];
                    wrt.write((tokens[i*4+1]+","+map.get(key)._1.toString()+","+map.get(key)._2.toString()+",").getBytes());
                }
                wrt.write('\n');
            }
        }catch(Exception e){
            System.out.println("Error on key: " + key + e.getMessage());
        }





    }

    public static void create_min_max_handover_nobug (JavaSparkContext sc, String handover_file, String ni_dir, String out_file){
        List<String> wanted = MyUtils.get_csv_column(handover_file , 0);
        final Broadcast<List<String>> br_wanted = sc.broadcast(wanted);

        JavaRDD<String> lines = sc.textFile(ni_dir);//+"/*.ni");
        JavaRDD<String> sus = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                String[] tokens = s.split(",");
                if (br_wanted.value().contains(tokens[1]))
                    return true;
                return false;
            }

        });

        JavaPairRDD<String,List<IdDate>> key_shtweet = sus.mapToPair(new PairFunction<String, String, List<IdDate>>() {
            public Tuple2<String, List<IdDate>> call(String s) {
                String[] elems = s.split(",");
                String key = elems[1]; //URL
                IdDate t = new IdDate(elems[2], Long.parseLong(elems[0]));
                List<IdDate> tmp = new ArrayList<IdDate>();
                tmp.add(t);
                return new Tuple2<String, List<IdDate>>(key, tmp);
            }
        });
        JavaPairRDD<String,List<IdDate>> key_shtweet_reduced = key_shtweet.reduceByKey(new Function2<List<IdDate>, List<IdDate>, List<IdDate>>() {
            public List<IdDate> call(List<IdDate> t1, List<IdDate> t2) throws Exception {

                int i1 = 0;
                int i2 = 0;
                List<IdDate> out = new ArrayList<IdDate>();

                while (i1 < t1.size() && i2 < t2.size()) {
                    if (t1.get(i1).compareTo(t2.get(i2)) == -1) {
                        out.add(t1.get(i1));
                        i1++;
                    } else {
                        out.add(t2.get(i2));
                        i2++;
                    }
                }
                while (i1 < t1.size()) {
                    out.add(t1.get(i1));
                    i1++;
                }
                while (i2 < t2.size()) {
                    out.add(t2.get(i2));
                    i2++;
                }
                return out;
            }
        });

        //Write a map to compact the List<IdDate>
        JavaPairRDD<String,List<IdDate>> compact_list = key_shtweet_reduced.mapValues(new Function<List<IdDate>, List<IdDate>>() {
            public List<IdDate> call(List<IdDate> t) throws Exception {
                List<IdDate> out = new ArrayList<IdDate>();
                int i=0;

                while(i<t.size()){
                    String huid = t.get(i).getId();
                    long min = t.get(i).getDate();
                    long max = t.get(i).getDate();
                    while (i<t.size() && huid.equals(t.get(i).getId())){
                        max = t.get(i).getDate();
                        i++;
                    }
                    IdDate tmp = new IdDate(huid, min, (int)(max-min), 0); //date=from     follower = to-from
                    out.add(tmp);
                }
                return out;
            }
        });

        List<Tuple2<String,List<IdDate>>> info = compact_list.collect();

        FileOutputStream wrt;

        try {
            wrt = new FileOutputStream(out_file);

            for (Tuple2<String, List<IdDate>> t : info){
                wrt.write((t._1+",").getBytes());
                for (IdDate x : t._2) {
                    wrt.write((x.getId()+ "," + String.valueOf(x.getDate()) + "," + String.valueOf(x.getFollower()+x.getDate()) + ",").getBytes());
                }
                wrt.write('\n');
            }
        }
        catch (Exception e){
            System.out.println("Error in writing min max file");
        }


    }

/*
    public static void create_time_series (JavaSparkContext sc, String wanted_file, String ni_dir, String out_file){
        List<String> wanted = MyUtils.get_csv_column(wanted_file , 0);
        final Broadcast<List<String>> br_wanted = sc.broadcast(wanted);

        JavaRDD<String> lines = sc.textFile(ni_dir);//+"/*.ni");
        JavaRDD<String> sus = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                String[] tokens = s.split(",");
                if (br_wanted.value().contains(tokens[1]))
                    return true;
                return false;
            }

        });

        JavaPairRDD<String,List<String>> key_shtweet = sus.mapToPair(new PairFunction<String, String, Tuple2<ShTweet,ShTweet>>() {
            public Tuple2<String, Tuple2<ShTweet,ShTweet>> call(String s) {
                String[] elems = s.split(",");
                String key = elems[1]+","+elems[2]; //URL + UID
                ShTweet t = new ShTweet(Long.parseLong(elems[0]), Integer.parseInt(elems[3]),Integer.parseInt(elems[4]),Integer.parseInt(elems[5]));
                return new Tuple2<String, Tuple2<ShTweet,ShTweet>>(key, new Tuple2<ShTweet, ShTweet>(t,t));
            }
        });
        JavaPairRDD<String,Tuple2<ShTweet,ShTweet>> key_shtweet_reduced = key_shtweet.reduceByKey(new Function2<Tuple2<ShTweet, ShTweet>, Tuple2<ShTweet, ShTweet>, Tuple2<ShTweet, ShTweet>>() {
            public Tuple2<ShTweet, ShTweet> call(Tuple2<ShTweet, ShTweet> t1, Tuple2<ShTweet, ShTweet> t2) throws Exception {
                ShTweet min;
                ShTweet max;
                if (t1._1.compareTo(t2._1)<0)
                    min = t1._1;
                else
                    min = t2._1;

                if (t1._2.compareTo(t2._2)>0)
                    max = t1._2;
                else
                    max = t2._2;

                return new Tuple2<ShTweet, ShTweet>(min,max);
            }
        });

        List<Tuple2<String,Tuple2<ShTweet,ShTweet>>> info = key_shtweet_reduced.collect();

        FileOutputStream wrt;
        HashMap<String , Tuple2<ShTweet,ShTweet>> map = new HashMap<String, Tuple2<ShTweet, ShTweet>>();

        try {
            //wrt = new FileOutputStream(out_file);

            for (Tuple2<String, Tuple2<ShTweet,ShTweet>> t : info){
                map.put(t._1 , t._2);
                //wrt.write((t._1+","+t._2._1.toString()+","+t._2._2.toString()+'\n').getBytes());
            }
        }
        catch (Exception e){
            System.out.println("Error in writing min max file");
        }


        String key="";
        try{
            File file = new File("mymap");
            FileOutputStream fmap = new FileOutputStream(file);
            for (String k : map.keySet()){
                fmap.write((k+","+map.get(k)._1.toString()+","+map.get(k)._2.toString()+"\n").getBytes());
            }
            fmap.close();


            wrt = new FileOutputStream(out_file);
            BufferedReader br = new BufferedReader(new FileReader(handover_file));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                String url = tokens[0];
                wrt.write((url+",").getBytes());
                for (int i=0 ; i<(tokens.length-1)/4 ; i++){
                    key = url + "," + tokens[i*4+1];
                    wrt.write((tokens[i*4+1]+","+map.get(key)._1.toString()+","+map.get(key)._2.toString()+",").getBytes());
                }
                wrt.write('\n');
            }
        }catch(Exception e){
            System.out.println("Error on key: " + key + e.getMessage());
        }





    }
*/

    public static void create_min_max_changeURL (JavaSparkContext sc, String user_id_file, String ni_dir, String out_file){
        List<String> wanted = MyUtils.get_csv_column(user_id_file , 0);
        final Broadcast<List<String>> br_wanted = sc.broadcast(wanted);

        JavaRDD<String> lines = sc.textFile(ni_dir);//+"/*.ni");
        JavaRDD<String> sus = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                String[] tokens = s.split(",");
                if (br_wanted.value().contains(tokens[2]))//user id of handover people
                    return true;
                return false;
            }

        });

        JavaPairRDD<String,Tuple2<ShTweet,ShTweet>> key_shtweet = sus.mapToPair(new PairFunction<String, String, Tuple2<ShTweet,ShTweet>>() {
            public Tuple2<String, Tuple2<ShTweet,ShTweet>> call(String s) {
                String[] elems = s.split(",");
                String key = elems[1]+","+elems[2]; //URL + UID
                ShTweet t = new ShTweet(Long.parseLong(elems[0]), Integer.parseInt(elems[3]),Integer.parseInt(elems[4]),Integer.parseInt(elems[5]));
                return new Tuple2<String, Tuple2<ShTweet,ShTweet>>(key, new Tuple2<ShTweet, ShTweet>(t,t));
            }
        });
        JavaPairRDD<String,Tuple2<ShTweet,ShTweet>> key_shtweet_reduced = key_shtweet.reduceByKey(new Function2<Tuple2<ShTweet, ShTweet>, Tuple2<ShTweet, ShTweet>, Tuple2<ShTweet, ShTweet>>() {
            public Tuple2<ShTweet, ShTweet> call(Tuple2<ShTweet, ShTweet> t1, Tuple2<ShTweet, ShTweet> t2) throws Exception {
                ShTweet min;
                ShTweet max;
                if (t1._1.compareTo(t2._1)<0)
                    min = t1._1;
                else
                    min = t2._1;

                if (t1._2.compareTo(t2._2)>0)
                    max = t1._2;
                else
                    max = t2._2;

                return new Tuple2<ShTweet, ShTweet>(min,max);
            }
        });

        List<Tuple2<String,Tuple2<ShTweet,ShTweet>>> info = key_shtweet_reduced.collect();

        FileOutputStream wrt;
        HashMap<String , Tuple2<ShTweet,ShTweet>> map = new HashMap<String, Tuple2<ShTweet, ShTweet>>();

        try {
            //wrt = new FileOutputStream(out_file);

            for (Tuple2<String, Tuple2<ShTweet,ShTweet>> t : info){
                map.put(t._1 , t._2);
                //wrt.write((t._1+","+t._2._1.toString()+","+t._2._2.toString()+'\n').getBytes());
            }
        }
        catch (Exception e){
            System.out.println("Error in writing min max file");
        }


        String key="";
        try{
            File file = new File("mymap_changeURL");
            FileOutputStream fmap = new FileOutputStream(file);
            for (String k : map.keySet()){
                fmap.write((k+","+map.get(k)._1.toString()+","+map.get(k)._2.toString()+"\n").getBytes());
            }
            fmap.close();


            wrt = new FileOutputStream(out_file);
            BufferedReader br = new BufferedReader(new FileReader(user_id_file));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                String uid = tokens[0].trim();
                wrt.write((uid+",").getBytes());
                for (int i=0 ; i<(tokens.length-1)/4 ; i++){
                    key = tokens[i*4+1] + ","+uid;
                    wrt.write((tokens[i*4+1]+","+map.get(key)._1.toString()+","+map.get(key)._2.toString()+",").getBytes());
                }
                wrt.write('\n');
            }
        }catch(Exception e){
            System.out.println("Error on key: " + key + e.getMessage());
        }





    }

    
    
     public static void all_steps (JavaSparkContext sc, String file_in, String file_out){
        //List<String> wanted = MyUtils.get_csv_column(handover_file , 0);
        //final Broadcast<List<String>> br_wanted = sc.broadcast(wanted);

        JavaRDD<String> lines = sc.textFile(file_in);
        //JavaRDD<String> sus = lines.filter(new Function<String, Boolean>() {
          //  public Boolean call(String s) throws Exception {
        //        String[] tokens = s.split(",");
        //        if (br_wanted.value().contains(tokens[1]))
        //            return true;
        //        return false;
        //    }

        //});

        JavaPairRDD<String,List<IdDate>> key_shtweet = lines.mapToPair(new PairFunction<String, String, List<IdDate>>() {
            public Tuple2<String, List<IdDate>> call(String s) {
                String[] elems = s.split(",");
                String key = elems[1]; //URL
                IdDate t = new IdDate(elems[2], Long.parseLong(elems[0]));
                List<IdDate> tmp = new ArrayList<IdDate>();
                tmp.add(t);
                return new Tuple2<String, List<IdDate>>(key, tmp);
            }
        });
        JavaPairRDD<String,List<IdDate>> key_shtweet_reduced = key_shtweet.reduceByKey(new Function2<List<IdDate>, List<IdDate>, List<IdDate>>() {
            public List<IdDate> call(List<IdDate> t1, List<IdDate> t2) throws Exception {

                int i1 = 0;
                int i2 = 0;
                List<IdDate> out = new ArrayList<IdDate>();

                while (i1 < t1.size() && i2 < t2.size()) {
                    if (t1.get(i1).compareTo(t2.get(i2)) == -1) {
                        out.add(t1.get(i1));
                        i1++;
                    } else {
                        out.add(t2.get(i2));
                        i2++;
                    }
                }
                while (i1 < t1.size()) {
                    out.add(t1.get(i1));
                    i1++;
                }
                while (i2 < t2.size()) {
                    out.add(t2.get(i2));
                    i2++;
                }
                return out;
            }
        });

        //Write a map to compact the List<IdDate>
        JavaPairRDD<String,List<IdDate>> compact_list = key_shtweet_reduced.mapValues(new Function<List<IdDate>, List<IdDate>>() {
            public List<IdDate> call(List<IdDate> t) throws Exception {
                List<IdDate> out = new ArrayList<IdDate>();
                int i=0;

                while(i<t.size()){
                    String huid = t.get(i).getId();
                    long min = t.get(i).getDate();
                    long max = t.get(i).getDate();
                    while (i<t.size() && huid.equals(t.get(i).getId())){
                        max = t.get(i).getDate();
                        i++;
                    }
                    IdDate tmp = new IdDate(huid, min, (int)(max-min), 0); //date=from     follower = to-from
                    out.add(tmp);
                }
                return out;
            }
        });


        JavaPairRDD<String,List<IdDate>> final_sus = compact_list.filter(new Function<Tuple2<String,List<IdDate>>, Boolean>() {
          public Boolean call(Tuple2<String,List<IdDate>> s) throws Exception {
                if(s._2().size()>1)
                    return true;
                return false;
            }

        });

        List<Tuple2<String,List<IdDate>>> info = final_sus.collect();

        FileOutputStream wrt;

        try {
            wrt = new FileOutputStream(file_out);

            for (Tuple2<String, List<IdDate>> t : info){
                wrt.write((t._1+",").getBytes());
                for (IdDate x : t._2) {
                    wrt.write((x.getId()+ "," + String.valueOf(x.getDate()) + "," + String.valueOf(x.getFollower()+x.getDate()) + ",").getBytes());
                }
                wrt.write('\n');
            }
        }
        catch (Exception e){
            System.out.println("Error in writing min max file");
        }

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
