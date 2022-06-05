package common;


public class Utils {
    public static String toDecade(String year){
        if(year.length() == 1){
            return "0";
        }
        return year.substring(0, year.length()-1);
    }

    public static String cleanWord(String w){
        return w.replace("\"", "");
    }
}
