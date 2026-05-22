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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.client.protocol.decoder.MapValueDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.misc.CompletableFutureWrapper;
import org.redisson.misc.WrappedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;

/**
 * Base class for implementing distributed locks
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {

        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;

        private final WrappedLock lock = new WrappedLock();

        public ExpirationEntry() {
            super();
        }

        public void addThreadId(long threadId) {
            lock.execute(() -> {
                threadIds.compute(threadId, (t, counter) -> {
                    counter = Optional.ofNullable(counter).orElse(0);
                    counter++;
                    return counter;
                });
            });
        }
        public boolean hasNoThreads() {
            return lock.execute(() -> threadIds.isEmpty());
        }
        public Long getFirstThreadId() {
            return lock.execute(() -> {
                if (threadIds.isEmpty()) {
                    return null;
                }
                return threadIds.keySet().iterator().next();
            });
        }
        public void removeThreadId(long threadId) {
            lock.execute(() -> {
                threadIds.compute(threadId, (t, counter) -> {
                    if (counter == null) {
                        return null;
                    }
                    counter--;
                    if (counter == 0) {
                        return null;
                    }
                    return counter;
                });
            });
        }

        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseLock.class);
    // TODO 重点 serverId+lockName  TO ExpirationEntry
    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    // TODO 重点 表明一个客户端的uuid
    final String id;
    final String entryName;

    public RedissonBaseLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        // TODO 重点 表明一个客户端的uuid
        this.id = getServiceManager().getId();
        // TODO 重点 默认30s
        this.internalLockLeaseTime = getServiceManager().getCfg().getLockWatchdogTimeout();
        // TODO 重点 serverId+lockName
        this.entryName = id + ":" + name;
    }

    protected String getEntryName() {
        return entryName;
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    private void renewExpiration() {
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        
        Timeout task = getServiceManager().newTimeout(new TimerTask() {
            // TODO 重点 讲续期任务委托给netty的HashedWheelTimer 的timer去执行
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }
                
                CompletionStage<Boolean> future = renewExpirationAsync(threadId);
                future.whenComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't update lock {} expiration", getRawName(), e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }
                    
                    if (res) {
                        // reschedule itself
                        // TODO 重点 续期成功了，继续续期
                        renewExpiration();
                    } else {
                        // TODO 重点 续期失败了，说明锁已经过期了，取消续期任务
                        cancelExpirationRenewal(null);
                    }
                });
            }
            // TODO 重点 时间轮续期任务的执行周期是 internalLockLeaseTime / 3，默认10s
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);
        
        ee.setTimeout(task);
    }
    
    protected void scheduleExpirationRenewal(long threadId) {
        ExpirationEntry entry = new ExpirationEntry();
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        if (oldEntry != null) {
            // TODO 重点 以前已经加过锁了
            oldEntry.addThreadId(threadId);
        } else {
            // TODO 重点 以前没有加过锁，第一次加锁了
            entry.addThreadId(threadId);
            try {
                // TODO 重点 续约逻辑
                renewExpiration();
            } finally {
                if (Thread.currentThread().isInterrupted()) {
                    cancelExpirationRenewal(threadId);
                }
            }
        }
    }

    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteSyncedAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // TODO 重点 当前线程持有对于的key，就进行续期
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getRawName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    protected void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            // TODO 重点 取消当前线程的续约逻辑
            task.removeThreadId(threadId);
        }

        if (threadId == null || task.hasNoThreads()) {
            // TODO 重点 entry已经没有线程了，取消续约
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            // TODO 重点 取消续约逻辑
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected final <T> RFuture<T> evalWriteSyncedAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        return commandExecutor.syncedEval(key, codec, evalCommandType, script, keys, params);
    }

    protected void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        commandExecutor.get(acquireFailedAsync(waitTime, unit, threadId));
    }

    protected void trySuccessFalse(long currentThreadId, CompletableFuture<Boolean> result) {
        acquireFailedAsync(-1, null, currentThreadId).whenComplete((res, e) -> {
            if (e == null) {
                result.complete(false);
            } else {
                result.completeExceptionally(e);
            }
        });
    }

    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getRawName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", new MapValueDecoder(), new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, HGET, getRawName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return getServiceManager().execute(() -> unlockAsync0(threadId));
    }

    private RFuture<Void> unlockAsync0(long threadId) {
        CompletionStage<Boolean> future = unlockInnerAsync(threadId);
        CompletionStage<Void> f = future.handle((opStatus, e) -> {
            cancelExpirationRenewal(threadId);

            if (e != null) {
                if (e instanceof CompletionException) {
                    throw (CompletionException) e;
                }
                throw new CompletionException(e);
            }
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                throw new CompletionException(cause);
            }

            return null;
        });

        return new CompletableFutureWrapper<>(f);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }

//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    String getUnlockLatchName(String requestId) {
        return prefixName("redisson_unlock_latch", getRawName()) + ":" + requestId;
    }

    protected abstract RFuture<Boolean> unlockInnerAsync(long threadId, String requestId, int timeout);

    protected final RFuture<Boolean> unlockInnerAsync(long threadId) {
        String id = getServiceManager().generateId();
        MasterSlaveServersConfig config = getServiceManager().getConfig();
        int timeout = (config.getTimeout() + config.getRetryInterval()) * config.getRetryAttempts();
        timeout = Math.max(timeout, 1);
        RFuture<Boolean> r = unlockInnerAsync(threadId, id, timeout);
        CompletionStage<Boolean> ff = r.thenApply(v -> {
            CommandAsyncExecutor ce = commandExecutor;
            if (ce instanceof CommandBatchService) {
                ce = new CommandBatchService(commandExecutor);
            }
            ce.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.DEL, getUnlockLatchName(id));
            if (ce instanceof CommandBatchService) {
                ((CommandBatchService) ce).executeAsync();
            }
            return v;
        });
        return new CompletableFutureWrapper<>(ff);
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    protected final <T> CompletionStage<T> handleNoSync(long threadId, CompletionStage<T> ttlRemainingFuture) {
        return commandExecutor.handleNoSync(ttlRemainingFuture, () -> unlockInnerAsync(threadId));
    }

}
