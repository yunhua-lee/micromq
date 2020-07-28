package io.micromq.log;

import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;

public class MQLog {
    private String message;
    private final Map<String, String> params = new HashMap<String, String>(4);

    public static MQLog build() {
        return new MQLog();
    }

    public static MQLog build(String msg) {
        return new MQLog(msg);
    }

    private MQLog() {

    }

    /**
     * Create a standard MQLog
     *
     * @param msg
     *            log message
     */
    public MQLog(String msg) {
        this.message = msg;
    }

    /**
     * Set log message and clear params
     *
     * @param msg
     * @return this
     */
    public MQLog setMessage(String msg) {
        Validate.notNull(msg);

        this.message = msg;
        params.clear();

        return this;
    }

    /**
     * Add log parameter. For example, we need to log user's name when we write
     * a login log. The user's name is log parameter.
     *
     * @param key
     *            parameter name
     * @param value
     *            parameter value
     * @return this
     */
    public <T> MQLog p(String key, T value) {
        Validate.notEmpty(key);

        params.put(key, String.valueOf(value));

        return this;
    }

    /**
     * Returns a string representing the MQLog
     */
    @Override
    public String toString() {
        return toJsonString();
    }

    @Deprecated
    public String toKeyValuePairString(){
        StringBuilder builder = new StringBuilder(128);

        builder.append("message: ");
        builder.append(this.message);
        if (params.isEmpty()) {
            return builder.toString();
        }

        builder.append(", ");
        builder.append("params: ");

        for (Map.Entry<String, String> entry : params.entrySet()) {
            builder.append(entry.getKey());
            builder.append("=");
            builder.append(entry.getValue());
            builder.append(",");
        }

        return builder.substring(0, builder.length() - 1);
    }

    public String toJsonString(){
        /*ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                    .writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return e.getMessage();
        }
         */

        return "";
    }
}
