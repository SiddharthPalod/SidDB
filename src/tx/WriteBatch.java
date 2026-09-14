package tx;

import java.util.ArrayList;
import java.util.List;

public class WriteBatch {

    public static class Op {
        private final String key;
        private final Object value;
        private final boolean isDelete;

        public Op(String key, Object value, boolean isDelete) {
            this.key = key;
            this.value = value;
            this.isDelete = isDelete;
        }

        public String getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public boolean isDelete() {
            return isDelete;
        }
    }

    private final List<Op> operations;

    public WriteBatch() {
        this.operations = new ArrayList<>();
    }

    public WriteBatch put(String key, Object value) {
        operations.add(new Op(key, value, false));
        return this;
    }

    public WriteBatch delete(String key) {
        operations.add(new Op(key, null, true));
        return this;
    }

    public List<Op> getOperations() {
        return operations;
    }

    public int size() {
        return operations.size();
    }

    public void clear() {
        operations.clear();
    }
}
