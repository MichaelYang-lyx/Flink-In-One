package tpc_query.DataStream.DataContent;

public class Nation implements IDataContent {
    private int N_NATIONKEY;
    private String N_NAME;
    private int N_REGIONKEY;
    private String N_COMMENT;

    public Nation() {
    }

    public Nation(String[] string) {
        this.N_NATIONKEY = Integer.parseInt(string[0]);
        this.N_NAME = string[1];
        this.N_REGIONKEY = Integer.parseInt(string[2]);
        this.N_COMMENT = string[3];
    }

    // getters and setters...

    public String toString() {
        return "Nation [N_NATIONKEY=" + N_NATIONKEY + ", N_NAME=" + N_NAME + ", N_REGIONKEY=" + N_REGIONKEY
                + ", N_COMMENT=" + N_COMMENT + "]";
    }
}