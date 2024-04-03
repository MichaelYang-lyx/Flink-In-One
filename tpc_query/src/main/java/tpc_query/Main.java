package tpc_query;

import tpc_query.Update.*;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        System.out.println("Hello world!");
        IUpdate insert = new Insert();
        insert.run();
    }
}