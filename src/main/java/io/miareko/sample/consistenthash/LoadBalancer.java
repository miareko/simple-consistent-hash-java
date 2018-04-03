package io.miareko.sample.consistenthash;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public final class LoadBalancer {

    private static LoadBalancer lb = null;

    private static final long R = 1L << 33;

    private SortedMap<Long, Server> ring = new TreeMap<>();
    private Map<Server, Set<Long>> virtual = new HashMap<>();

    public static LoadBalancer getInstance() {
        if (lb == null) {
            lb = new LoadBalancer();
        }
        return lb;
    }

    public void addNode(Server server) {
        addVirtualNodes(server);
    }

    private void addVirtualNodes(Server server) {
        Set<Long> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            String name = server.getName() + "virtual" + i;
            long pos = hash(name) % R;
            ring.put(pos, server);
            set.add(pos);
        }
        virtual.put(server, set);
    }

    public void removeNode(Server server) {
        Set<Long> set = virtual.get(server);
        if (set != null && !set.isEmpty()) {
            set.forEach(p -> ring.remove(p));
        }
    }

    public Server route(Client client) {
        if (ring.isEmpty()) {
            return null;
        }
        long pos = hash(client.getName()) % R;
        Server server = null;
        if (ring.containsKey(pos)) {
            server = ring.get(pos);
        } else {
            Set<Long> set = ring.keySet();
            for (Long p : set) {
                if (pos <= p) {
                    server = ring.get(p);
                    break;
                }
            }
            if (server == null) {
                server = ring.get(set.stream().findFirst());
            }
        }
        System.out.println(String.format("client [%s] route to server [%s]", client.getName(), server.getName()));
        return server;
    }

    public static long hash(String key) {
        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
        int seed = 0x1234ABCD;

        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (buf.remaining() * m);

        long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(
                    ByteOrder.LITTLE_ENDIAN);
            // for big-endian version, do this first:
            // finish.position(8-buf.remaining());
            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);
        return h;
    }

}
