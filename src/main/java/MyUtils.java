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
            System.out.println("Error: " + e.getMessage());
        }
    }

    public static String[] get_list_of_files(String path){
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        List<String> out = new ArrayList<String>();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().indexOf("part")==0) {
                out.add(listOfFiles[i].getName());
            }
        }

        String[] output = new String[out.size()];
        out.toArray(output);
        return output;

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
        } catch(Exception e){
            System.out.println("Error: " + e.getMessage());
        }
    }

    public static void write_to_file_int(List<Integer> in , String filename, char delim) {
        try {
            FileOutputStream wrt = new FileOutputStream(filename);
            for (int i=0 ; i<in.size()-1 ; i++){
                wrt.write((in.get(i).toString()+delim).getBytes());
            }
            wrt.write((in.get(in.size() - 1).toString()).getBytes());
        }catch(Exception e){
            System.out.println("Error: " + e.getMessage());
        }
    }

    public static void write_to_file_string(List<String> in , String filename, char delim){
        try {
            FileOutputStream wrt = new FileOutputStream(filename);
            for (int i=0 ; i<in.size()-1 ; i++){
                wrt.write((in.get(i)+delim).getBytes());
            }
            wrt.write((in.get(in.size() - 1)).getBytes());
            wrt.close();
        }catch(Exception e){
            System.out.print("Error: " + e.getMessage());
        }

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

    public static List<String> get_csv_column (String csvFile, int col){
        List<String> out = new ArrayList<String>();
        try{
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                out.add(tokens[col]);
            }
        }catch(Exception e){
                System.out.println("Error: " + e.getMessage());
        }
        return out;
    }

}
