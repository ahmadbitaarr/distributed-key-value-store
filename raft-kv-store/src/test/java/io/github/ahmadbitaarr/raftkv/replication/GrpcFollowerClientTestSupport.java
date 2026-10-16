package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import java.util.function.Function;

/** Test-source-only access to the preexisting package-private runtime endpoint resolver. */
public final class GrpcFollowerClientTestSupport {
    private GrpcFollowerClientTestSupport() { }
    public static GrpcFollowerClient withEndpointResolver(ClusterConfig config,
            Function<String,NodeEndpoint> resolver) {
        return new GrpcFollowerClient(config,resolver);
    }
}
