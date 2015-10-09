import org.omg.IOP.Encoding;
import scala.Tuple2;

import javax.sound.sampled.AudioFormat;
import java.io.*;
import java.util.ArrayList;
import java.util.Date;
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

    public static void write_to_file_string(List<String> in , String filename, char delim){
        try {
            FileOutputStream wrt = new FileOutputStream(filename);
            for (int i=0 ; i<in.size()-1 ; i++){
                wrt.write((in.get(i)+delim).getBytes());
            }
            wrt.write((in.get(in.size() - 1)).getBytes());
        }catch(Exception e){
            System.out.print("Error");
        }
    }

    /*  The code to find loops in changeURLs or handovers*/
    public static List<String> find_loop(String filename_in , String filename_out){
        List<String> out = new ArrayList<String>();
        try{
            List<String> names = new ArrayList<String>();
            BufferedReader br = new BufferedReader(new FileReader(filename_in));
            String line;
            while ((line = br.readLine()) != null) {
                names.clear();
                String[] tokens = line.split(":");
                for (int i = 1 ; i<tokens.length ; i++){
                    String[] itr = tokens[i].split(",");
                    if (names.contains(itr[0])){
                        //System.out.println(tokens[0]);
                        out.add(replaceTimestampWithDate(line));
                    }
                    names.add(itr[0]);
                }
            }
        }catch(Exception e){
            System.out.println("Error in reading the file: find_loop: " + e.getMessage());
        }
        MyUtils.write_to_file_string(out,filename_out,'\n');
        return out;
    }



    public static String replaceTimestampWithDate (String in){

        String[] tokens = in.split(":");
        String out= tokens[0]+":";
        for (int i=1 ; i<tokens.length ; i++){
            String[] inner = tokens[i].split(",");
            Date date = new Date(Long.valueOf(inner[1]));
            out += inner[0] + "," + date.toString() + ":";
        }
        return out;

    }
    public static void testfile (){

        File file = new File("/home/hossein/Downloads/snippet1.txt");
        FileInputStream fin = null;

        try {
            // create FileInputStream object
            fin = new FileInputStream(file);
            byte fileContent[] = new byte[(int)file.length()];
            fin.read(fileContent);
            System.out.println(fileContent.length);

        }
        catch(Exception e){
            System.out.print("sdsadas");
        }
    }

}
