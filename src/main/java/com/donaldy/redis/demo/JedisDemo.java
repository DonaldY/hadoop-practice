package com.donaldy.redis.demo;

import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @author donald
 * @date 2020/09/09
 */
public class JedisDemo {

    @Test
    public void testConn(){
        // 与Redis建立连接 IP+port
        Jedis redis = new Jedis("192.168.127.128", 6379);
        // 在Redis中写字符串 key value
        redis.set("jedis:name:1","jd-zhangfei");
        // 获得Redis中字符串的值
        System.out.println(redis.get("jedis:name:1"));
        // 在Redis中写list
        redis.lpush("jedis:list:1","1","2","3","4","5");
        // 获得list的长度
        System.out.println(redis.llen("jedis:list:1"));
    }

    @Test
    public void testCluster() {

        JedisPoolConfig config = new JedisPoolConfig();

        Set<HostAndPort> jedisClusterNode = new HashSet<>();
        jedisClusterNode.add(new HostAndPort("172.16.64.21", 7001));
        jedisClusterNode.add(new HostAndPort("172.16.64.21", 7002));
        jedisClusterNode.add(new HostAndPort("172.16.64.21", 7003));
        jedisClusterNode.add(new HostAndPort("172.16.64.21", 7004));
        jedisClusterNode.add(new HostAndPort("172.16.64.21", 7005));
        jedisClusterNode.add(new HostAndPort("172.16.64.21", 7006));
        JedisCluster jcd = new JedisCluster(jedisClusterNode, config);
        jcd.set("name:001","donald");

        String value = jcd.get("name:001");

        System.out.println(value);
    }
}
