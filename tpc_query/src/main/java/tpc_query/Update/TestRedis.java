package tpc_query.Update;

import java.io.*;

import redis.clients.jedis.Jedis;

public class TestRedis {
    public static void main(String[] args) {
        // 连接本地的 Redis 服务
        Jedis jedis = new Jedis("localhost", 6380);
        jedis.flushAll();
        System.out.println("连接成功");

        // 设置 redis 字符串数据
        // jedis.set("mykey", "Hello, Redis!");

        // 获取存储的数据并输出
        System.out.println("redis 存储的字符串为: " + jedis.get("mykey"));
        jedis.close();
        Process process;
        try {
            process = Runtime.getRuntime().exec("ping www.delftstack.com");
            printResults(process);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static void printResults(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }
}
