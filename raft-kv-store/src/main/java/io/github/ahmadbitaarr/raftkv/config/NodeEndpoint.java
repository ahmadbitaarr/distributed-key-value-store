package io.github.ahmadbitaarr.raftkv.config;

import io.github.ahmadbitaarr.raftkv.rpc.RpcLimits;

/** Advertised destination, never a listening interface. No DNS resolution at parse time. */
public record NodeEndpoint(String host, int port) {
    public NodeEndpoint {
        RpcLimits.validText(host, RpcLimits.MAX_HOST_BYTES, "endpoint host", true);
        if (!host.matches("[A-Za-z0-9][A-Za-z0-9.-]*") || host.contains("..")
                || host.endsWith(".") || host.equals("0.0.0.0") || host.equals("255.255.255.255")
                || host.equalsIgnoreCase("localhost.localdomain.invalid")) {
            throw new IllegalArgumentException("invalid advertised host: " + host);
        }
        if (host.matches("[0-9.]+")) {
            String[] octets = host.split("\\.", -1);
            if (octets.length != 4) throw new IllegalArgumentException("invalid IPv4 address");
            for (String octet : octets) {
                try {
                    int part = Integer.parseInt(octet);
                    if (part < 0 || part > 255) throw new IllegalArgumentException("invalid IPv4 octet");
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("invalid IPv4 octet", e);
                }
            }
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1..65535");
        }
    }
}
