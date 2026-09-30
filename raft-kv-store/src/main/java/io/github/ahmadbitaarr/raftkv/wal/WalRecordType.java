package io.github.ahmadbitaarr.raftkv.wal;

public enum WalRecordType {
    ENTRY((byte) 0x01),
    COMMIT((byte) 0x02);

    private final byte code;

    WalRecordType(byte code) {
        this.code = code;
    }

    byte code() {
        return code;
    }

    static WalRecordType fromCode(byte code) throws WalCorruptionException {
        for (WalRecordType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new WalCorruptionException("unknown WAL record type: " + Byte.toUnsignedInt(code));
    }
}
