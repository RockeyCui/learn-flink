package com.rock.flink19.lookup.function;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author cuishilei
 * @date 2019/9/1
 */
public class RedisAsyncTableFunction extends AbstractAsyncTableFuc {
    private static final long serialVersionUID = -1408840130246375742L;

    private final String ip;
    private final int port;
    private final String database;
    private final String password;
    private final String tableName;

    private transient RedisClient redisClient;

    private transient StatefulRedisConnection<String, String> connection;

    private transient RedisAsyncCommands<String, String> asyncCommands;

    private RedisAsyncTableFunction(Builder builder) {
        super(builder.fieldNames, builder.fieldTypes, builder.joinKeyNames,
                builder.isCached, "", builder.cacheMaxSize, builder.cacheExpireMs,
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
        asyncCommands = connection.async();
    }

    @Override
    Row getRow(Object... data) {
        List<KeyValue<String, String>> keyValues = (List<KeyValue<String, String>>) data[0];
        String keyPre = (String) data[1];
        Object[] filedValue = new Object[fieldNames.length];
        for (int i = 0; i < keyValues.size(); i++) {
            String value = keyValues.get(i).getValue();
            if (value == null) {
                throw new RuntimeException("table or column not found");
            }
            filedValue[otherFieldNamesIndexes[i]] = value;
        }
        List<String> list = Arrays.asList(keyPre.split(":"));
        for (int i = 0; i < joinKeyIndexes.length; i++) {
            filedValue[joinKeyIndexes[i]] = list.get(list.indexOf(joinKeyNames[i]) + 1);
        }
        return Row.of(filedValue);
    }

    public void eval(CompletableFuture<Collection<Row>> future, Object... paramas) {
        StringBuilder keyPreBuilder = new StringBuilder(tableName);
        for (int i = 0; i < paramas.length; i++) {
            keyPreBuilder.append(":").append(joinKeyNames[i]).append(":").append(paramas[i]);
        }
        String keyPre = keyPreBuilder.toString();
        if (isCached) {
            CompletableFuture<Row> get = asyncCache.getIfPresent(keyPre);
            if (get == null) {
                String[] redisKeys = FieldUtil.getRedisKeys(keyPre, otherFieldNames);
                RedisFuture<List<KeyValue<String, String>>> redisFuture = asyncCommands.mget(redisKeys);
                String finalKeyPre = keyPre;
                redisFuture.thenAccept(keyValues -> {
                    Row row = getRow(keyValues, finalKeyPre);
                    future.complete(Collections.singleton(row));
                    asyncCache.put(finalKeyPre, CompletableFuture.completedFuture(row));
                });
            } else {
                get.thenAccept(value -> {
                    future.complete(Collections.singleton(value));
                });
            }
        } else {
            String[] redisKeys = FieldUtil.getRedisKeys(keyPre, otherFieldNames);
            RedisFuture<List<KeyValue<String, String>>> redisFuture = asyncCommands.mget(redisKeys);
            String finalKeyPre = keyPre;
            redisFuture.thenAccept(keyValues -> {
                Row row = getRow(keyValues, finalKeyPre);
                future.complete(Collections.singleton(row));
            });
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
        if (asyncCache != null) {
            asyncCache = null;
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

        public RedisAsyncTableFunction build() {
            return new RedisAsyncTableFunction(this);
        }
    }
}
