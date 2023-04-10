package app_kvServer;

import java.lang.Integer;

public class ClientNode {
    private String address;
    private int port;
        
    public ClientNode(String serialized) throws Exception {
        String[] parts = serialized.split(":");

        if (parts.length != 2) throw new Exception("Invalid client socket server serialization string");

        this.address = parts[0];
        this.port = Integer.parseInt(parts[1]);
    }

    public String getAddress() {
        return this.address;
    }

    public int getPort() {
        return this.port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        if (obj == null || getClass() != obj.getClass()) return false;

        ClientNode other = (ClientNode) obj;

        return this.address.equals(other.getAddress()) && this.port == other.getPort();
    }

    @Override
    public int hashCode() {
        return String.format("%s:%d", this.address, this.port).hashCode();
    }
}
