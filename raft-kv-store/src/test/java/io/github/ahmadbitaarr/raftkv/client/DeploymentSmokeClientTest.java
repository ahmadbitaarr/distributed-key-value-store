package io.github.ahmadbitaarr.raftkv.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DeploymentSmokeClientTest {
    @Test void emptyPutValueRemainsExplicitAndIdStable() {
        var opts=DeploymentSmokeClient.parse(new String[]{"put","--key=zero","--request-id=stable","--value-base64="});
        assertEquals("stable",opts.requestId());
        assertArrayEquals(new byte[0],opts.value());
        assertNull(opts.expectValue());
        assertNotSame(opts.value(),opts.value()); // Defensive copies.
    }
    @Test void expectedCodeAndIndexComparePrecisely() {
        var o=DeploymentSmokeClient.parse(new String[]{"get","--key=k","--expect=RESULT_CODE_OK","--expect-index=2","--expect-value-base64=AAE="});
        assertTrue(DeploymentSmokeClient.matches("OK",2L,new byte[]{0,1},o));
        assertFalse(DeploymentSmokeClient.matches("OK",3L,new byte[]{0,1},o));
        assertFalse(DeploymentSmokeClient.matches("NOT_FOUND",2L,new byte[]{0,1},o));
        assertFalse(DeploymentSmokeClient.matches("OK",2L,new byte[]{0,2},o));
    }
    @Test void rejectsMissingValueAndMalformedArgumentsAndEscapesJson() {
        assertThrows(IllegalArgumentException.class,()->DeploymentSmokeClient.parse(new String[]{"put","--key=k"}));
        assertThrows(IllegalArgumentException.class,()->DeploymentSmokeClient.parse(new String[]{"put","--key=k","--value-base64=!!"}));
        assertThrows(IllegalArgumentException.class,()->DeploymentSmokeClient.parse(new String[]{"get","--key=k","--key=other"}));
        assertEquals("\"a\\\"\\n\"",DeploymentSmokeClient.quote("a\"\n"));
    }
}
