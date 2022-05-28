
public class MasterMain {
    public static final boolean DEBUG_MODE = true;
    public static void main(String[] args) throws Exception {
        String job = args[0];
        String[] subarray = new String[args.length - 1];
        System.arraycopy(args, 1, subarray, 0, subarray.length);
        if (job.equals("NC1AP")) {
            nc1appender.Main.main(subarray);
        }else if(job.equals("C2CLC")){
            c2appender.Main.main(subarray);
        }else if(job.equals("LKL")){
            likelihood.Main.main(subarray);
        }
    }
}
