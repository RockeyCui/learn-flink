package com.rock.flink19.lookup.function;

import com.github.benmanes.caffeine.cache.LoadingCache;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @author cuishilei
 * @date 2019/9/1
 */
public class RedisTableFunction extends AbstractTableFuc {
    private static final long serialVersionUID = -1408840130246375742L;

    private final String ip;
    private final int port;
    private final String database;
    private final String password;
    private final String tableName;

    private transient RedisClient redisClient;

    private transient StatefulRedisConnection<String, String> connection;

    private transient RedisStringCommands<String, String> commands;

    private RedisTableFunction(Builder builder) {
        super(builder.fieldNames, builder.fieldTypes, builder.joinKeyNames,
                builder.isCached, builder.cacheType, builder.cacheMaxSize, builder.cacheExpireMs,
                builder.maxRetryTimes);
        ip = builder.ip;
        port = builder.port;
        database = builder.database;
        password = builder.password;
        tableName = builder.tableName;
    }


    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initCache();
        redisClient = RedisClient.create("redis://" + ip + ":" + port + "/" + database);
        connection = redisClient.connect();
        commands = connection.sync();
    }

    @Override
    Row getRow(Object redisKeyPre) {
        Object[] filedValue = new Object[fieldNames.length];

        String[] redisKeys = FieldUtil.getRedisKeys((String) redisKeyPre, otherFieldNames);
        //填充字段值
        List<KeyValue<String, String>> mget = commands.mget(redisKeys);
        for (int i = 0; i < mget.size(); i++) {
            String value = mget.get(i).getValue();
            if (value == null) {
                throw new RuntimeException("table or column not found");
            }
            filedValue[otherFieldNamesIndexes[i]] = value;
        }
        //填充 key 值
        List<String> list = Arrays.asList(redisKeyPre.toString().split(":"));
        for (int i = 0; i < joinKeyIndexes.length; i++) {
            filedValue[joinKeyIndexes[i]] = list.get(list.indexOf(joinKeyNames[i]) + 1);
        }
        return Row.of(filedValue);
    }

    public void eval(Object... paramas) {
        StringBuilder keyPreBuilder = new StringBuilder(tableName);
        for (int i = 0; i < paramas.length; i++) {
            keyPreBuilder.append(":").append(joinKeyNames[i]).append(":").append(paramas[i]);
        }
        String keyPre = keyPreBuilder.toString();
        if (isCached) {
            if ("ALL".equals(cacheType) && cache instanceof LoadingCache) {
                LoadingCache<String, Row> loadingCache = (LoadingCache<String, Row>) cache;
                collect(loadingCache.get(keyPre));
            } else {
                Row row = cache.get(keyPre, this::getRow);
                collect(row);
            }
        } else {
            Row row = getRow(keyPre);
            collect(row);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
        if (cache != null) {
            cache = null;
        }
    }

    public static final class Builder {
        private String ip;
        private int port;
        private String database;
        private String password;
        private String tableName;
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;
        private String[] joinKeyNames;
        private boolean isCached;
        private String cacheType;
        private long cacheMaxSize;
        private long cacheExpireMs;
        private int maxRetryTimes;

        private Builder() {
        }

        public Builder withIp(String val) {
            ip = val;
            return this;
        }

        public Builder withPort(int val) {
            port = val;
            return this;
        }

        public Builder withDatabase(String val) {
            database = val;
            return this;
        }

        public Builder withPassword(String val) {
            password = val;
            return this;
        }

        public Builder withTableName(String val) {
            tableName = val;
            return this;
        }

        public Builder withFieldNames(String[] val) {
            fieldNames = val;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] val) {
            fieldTypes = val;
            return this;
        }

        public Builder withJoinKeyNames(String[] val) {
            joinKeyNames = val;
            return this;
        }

        public Builder withIsCached(boolean val) {
            isCached = val;
            return this;
        }

        public Builder withCacheType(String val) {
            cacheType = val;
            return this;
        }

        public Builder withCacheMaxSize(long val) {
            cacheMaxSize = val;
            return this;
        }

        public Builder withCacheExpireMs(long val) {
            cacheExpireMs = val;
            return this;
        }

        public Builder withMaxRetryTimes(int val) {
            maxRetryTimes = val;
            return this;
        }

        public RedisTableFunction build() {
            return new RedisTableFunction(this);
        }
    }
}
