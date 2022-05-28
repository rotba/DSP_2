import nc1appender.Main;

public class MasterMain {
    public static void main(String[] args) throws Exception {
        String job = args[0];
        String[] subarray = new String[args.length - 1];
        System.arraycopy(args, 1, subarray, 0, subarray.length);
        if (job.equals("NC1AP")) {
            Main.main(subarray);
        }
    }
}
