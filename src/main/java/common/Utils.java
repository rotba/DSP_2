package common;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    public static boolean filterWord(String w){
        if(w.length() == 0){
            return true;
        }else if(w.contains("_") || w.contains(".") || w.contains(",")|| w.contains(")") || w.contains("?")|| w.contains("-")){
            return true;
        }else{
            return false;
        }
    }

    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            int d = Integer.parseInt(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public static boolean finalFilter(String w){
        return w.length()<=1;
    }
}
