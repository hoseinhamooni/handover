import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by hossein on 10/5/15.
 */
public class Processing implements Serializable{

    public Processing(){}

    public void find_change_screenname(String filename){
        SparkConf conf = new SparkConf().setAppName(Params.sparkAppName).setMaster(Params.sparkMaster);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(filename);

        JavaPairRDD<String,TreeSet<URLdate>> name_uid = lines.mapToPair(new PairFunction<String,String,TreeSet<URLdate>>() {
            public Tuple2< String,TreeSet<URLdate>> call(String s) {
                String[] elems = s.split(",");
                //String name = elems[1];
                URLdate t = new URLdate(elems[1] , Long.parseLong(elems[0]));
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
        JavaPairRDD<String,TreeSet<URLdate>> suspicious = uid_name_count.filter(new Function<Tuple2<String, TreeSet<URLdate>>, Boolean>() {
            public Boolean call(Tuple2<String, TreeSet<URLdate>> in) throws Exception {
                Boolean out = false;
                if (in._2().size()>1)
                    return true;
                return false;
            }
        });

        List<Tuple2<String, TreeSet<URLdate>>> sus = suspicious.collect();
        MyUtils.write_to_file_URL(sus, "changeURL.csv");
        System.out.println(String.valueOf(userNum) + " users");

    }



    public List<Tuple2<String, TreeSet<IdDate>>> find_handover(String filename_in , String filename_out){
        SparkConf conf = new SparkConf().setAppName(Params.sparkAppName).setMaster(Params.sparkMaster);
        JavaSparkContext sc = new JavaSparkContext(conf);
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

        List<Tuple2<String, TreeSet<IdDate>>> sus = suspicious.collect();
        MyUtils.write_to_file_Id(sus, filename_out);
        System.out.println(String.valueOf(userNum) + " users");
        return sus;

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
