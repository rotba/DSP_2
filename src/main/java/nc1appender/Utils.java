package nc1appender;

import org.apache.log4j.Level;

import java.util.HashMap;

public class Utils {
    public static HashMap<String, Long> toMap(String s){
        HashMap<String,Long> map = new HashMap<>();
        String[] lines = s.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String[] line = lines[i].split("\\s+");
            if(line.length<2){
                continue;
            }
            map.put(line[0], Long.parseLong(line[1]));
        }
        return map;
    }
}
