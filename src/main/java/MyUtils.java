import scala.Tuple2;

import java.io.FileOutputStream;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by hossein on 10/5/15.
 */
public final class MyUtils {


    public static void write_to_file_URL(List<Tuple2<String, TreeSet<URLdate>>> in , String filename){
        try {
            FileOutputStream wrt = new FileOutputStream(filename);
            for (int i=0 ; i<in.size() ; i++){
                wrt.write((in.get(i)._1 + ":").getBytes());
                for (URLdate s : in.get(i)._2()){
                    wrt.write((s.toString() + ":").getBytes());
                }
                wrt.write('\n');
            }
        }catch(Exception e){
            System.out.print("Error");
        }
    }

    public static void write_to_file_Id(List<Tuple2<String, TreeSet<IdDate>>> in , String filename){
        try {
            FileOutputStream wrt = new FileOutputStream(filename);
            for (int i=0 ; i<in.size() ; i++){
                wrt.write((in.get(i)._1 + ":").getBytes());
                for (IdDate s : in.get(i)._2()){
                    wrt.write((s.toString() + ":").getBytes());
                }
                wrt.write('\n');
            }
        }catch(Exception e){
            System.out.print("Error");
        }
    }

    public static void write_to_file_int(List<Integer> in , String filename, char delim){
        try {
            FileOutputStream wrt = new FileOutputStream(filename);
            for (int i=0 ; i<in.size()-1 ; i++){
                wrt.write((in.get(i).toString()+delim).getBytes());
            }
            wrt.write((in.get(in.size() - 1).toString()).getBytes());
        }catch(Exception e){
            System.out.print("Error");
        }
    }
}
