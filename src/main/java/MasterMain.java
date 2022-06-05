import c2appender.Main;
import common.Props;
import org.apache.log4j.Logger;

public class MasterMain {
    public static final Logger logger = Logger.getLogger(MasterMain.class);
    public static void main(String[] args) throws Exception {
        String job = args[0];
        String debugMode = args[1];
        String local = args[2];
        int bigramThresh = Integer.parseInt(args[3]);
        Props.DEBUG_MODE = debugMode.equals("1")? true: false;
        Props.LOCAL = local.equals("1")? true: false;
        Props.min2gCountThresh =bigramThresh;
        logger.info(String.format("DEBUG_MODE=%b, LOCAL=%b, min2gCountThresh=%d", Props.DEBUG_MODE, Props.LOCAL, Props.min2gCountThresh));
        String[] subarray = new String[args.length - 4];
        System.arraycopy(args, 4, subarray, 0, subarray.length);
        if (job.equals("NC1AP")) {
            nc1appender.Main.main(subarray);
        }else if(job.equals("C2AP")){
            c2appender.Main.main(subarray);
        }else if(job.equals("LKL")){
            likelihood.Main.main(subarray);
        }else if(job.equals("OGWC")){
            onegramwordcount.Main.main(subarray);
        }else if(job.equals("TGWC")){
            throw new RuntimeException("NOOOOOOOOOOOOO");
//            twogramworccount.Main.main(subarray);
        }else if(job.equals("TGWCComb")){
            tgwccomb.Main.main(subarray);
        }else if(job.equals("DECC")){
            decadecount.Main.main(subarray);
        }
    }
}
