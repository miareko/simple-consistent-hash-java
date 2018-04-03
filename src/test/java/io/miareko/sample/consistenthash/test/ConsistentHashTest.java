package io.miareko.sample.consistenthash.test;

import io.miareko.sample.consistenthash.LoadBalancer;
import io.miareko.sample.consistenthash.Client;
import io.miareko.sample.consistenthash.Server;

public class ConsistentHashTest {

    public static void main(String[] args) {
        LoadBalancer lb = LoadBalancer.getInstance();

        Server server1 = new Server("192.168.1.1");
        Server server2 = new Server("192.168.1.2");
        Server server3 = new Server("192.168.1.3");

        lb.addNode(server1);
        lb.addNode(server2);
        lb.addNode(server3);

        for (int i = 0; i < 100; i++) {
            Client client = new Client(String.format("127.0.0.%s", i));
            lb.route(client);
        }

        lb.removeNode(server1);

        for (int i = 0; i < 100; i++) {
            Client client = new Client(String.format("127.0.0.%s", i));
            lb.route(client);
        }
    }
}
