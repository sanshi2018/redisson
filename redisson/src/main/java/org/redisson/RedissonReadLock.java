/**
 * Copyright (c) 2013-2024 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonReadLock extends RedissonLock implements RLock {

    protected RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return commandExecutor.syncedEval(getRawName(), LongCodec.INSTANCE, command,
                                // 获取当前这把锁的mode值
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                "if (mode == false) then " +
                                        // 添加读锁8
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                        // 给当前线程加锁，如果是读锁，会给所有线程都添加重入次数，如果是写锁，只会记一个线程的重入次数
                                        // 这里只关注线程，不关心是读锁，写锁
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                        // 读写超时锁拼接上1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                        // 读写超时锁设置过期时间
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +

                                        // 如果是读锁 OR 是写锁 + 是当前进程
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                        // 重入++ ，如果是读锁，会给所有线程都添加重入次数，如果是写锁，只会记一个线程的重入次数
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                        // 读写超时锁拼上重入次数【ind】
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                  "redis.call('set', key, 1); " +
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                        // 锁剩余时间
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                        // 重新设置ttl
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",
                        // KEY[1] - lock name
                        // KEY[2] - {lock name}:threadId:rwlock_timeout
                        // ARGV[1] - 锁过期时间
                        // ARGV[2] - UUID:threadId
                        // ARGV[3] - UUID:threadId:write
                        Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
                        unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId, String requestId, int timeout) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteSyncedAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
          "local val = redis.call('get', KEYS[5]); " +
                "if val ~= false then " +
                    "return tonumber(val);" +
                "end; " +

                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    "redis.call(ARGV[3], KEYS[2], ARGV[1]); " +
                    "redis.call('set', KEYS[5], 1, 'px', ARGV[4]); " +
                    "return 1; " +
                "end; " +
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                    
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " + 
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " + 
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " + 
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +
                            
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "redis.call('set', KEYS[5], 0, 'px', ARGV[4]); " +
                        "return 0; " +
                    "end;" + 
                        
                    "if mode == 'write' then " +
                        "redis.call('set', KEYS[5], 0, 'px', ARGV[4]); " +
                        "return 0;" +
                    "end; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "redis.call(ARGV[3], KEYS[2], ARGV[1]); " +
                "redis.call('set', KEYS[5], 1, 'px', ARGV[4]); " +
                "return 1; ",
                Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix, getUnlockLatchName(requestId)),
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId), getSubscribeService().getPublishCommand(), timeout);
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }
    
    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteSyncedAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " + 
                                "for i=counter, 1, -1 do " + 
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.syncedEvalWithRetry(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call(ARGV[2], KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.asList(getRawName(), getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, getSubscribeService().getPublishCommand());
    }

    @Override
    public boolean isLocked() {
        RFuture<Boolean> future = commandExecutor.evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
          "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == 'read') or (mode == 'write' and redis.call('hlen', KEYS[1]) > 2) then " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.asList(getRawName()));

        return get(future);
    }

}
