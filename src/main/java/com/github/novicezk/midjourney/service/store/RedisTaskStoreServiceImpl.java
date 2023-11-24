package com.github.novicezk.midjourney.service.store;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import com.github.novicezk.midjourney.service.TaskStoreService;
import com.github.novicezk.midjourney.support.Task;
import com.github.novicezk.midjourney.support.TaskCondition;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RedisTaskStoreServiceImpl implements TaskStoreService {
    private static final String KEY_PREFIX = "mj-task-store::";
    private static final String ALL_KEY_PREFIX = "mj-task-store::keys";

    private final Duration timeout;
    private final RedisTemplate<String, Task> redisTemplate;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisTaskStoreServiceImpl(Duration timeout, RedisTemplate<String, Task> redisTemplate, StringRedisTemplate stringRedisTemplate) {
        this.timeout = timeout;
        this.redisTemplate = redisTemplate;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public void save(Task task) {
        String taskKey = getRedisKey(task.getId());
        this.redisTemplate.opsForValue().set(taskKey, task, this.timeout);
        stringRedisTemplate.opsForList().leftPush(ALL_KEY_PREFIX, taskKey);
    }

    @Override
    public void delete(String id) {
        String key = getRedisKey(id);

        this.redisTemplate.delete(key);
        stringRedisTemplate.opsForList().remove(ALL_KEY_PREFIX,1,key);
    }

    @Override
    public Task get(String id) {
        return this.redisTemplate.opsForValue().get(getRedisKey(id));
    }

    @Override
    public List<Task> list() {

        List<String> keys = stringRedisTemplate.opsForList().range(ALL_KEY_PREFIX, 0, 10000);

        //Set<String> keys = this.redisTemplate.keys(KEY_PREFIX + "*");

        if (CollUtil.isEmpty(keys)) {
            return ListUtil.empty();
        }
        //keys = keys.stream().limit(1000).collect(Collectors.toList());

        //Set<String> keys = this.redisTemplate.execute((RedisCallback<Set<String>>) connection -> {
        // Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().match(KEY_PREFIX + "*").count(1000).build());
        // return cursor.stream().map(String::new).collect(Collectors.toSet());
        //});
        //if (keys == null || keys.isEmpty()) {
        //    return Collections.emptyList();
        //}
        ValueOperations<String, Task> operations = this.redisTemplate.opsForValue();
        return keys.stream().map(operations::get).filter(Objects::nonNull).toList();
    }


    @Override
    public List<Task> list(TaskCondition condition) {
        return list().stream().filter(condition).toList();
    }

    @Override
    public Task findOne(TaskCondition condition) {
        return list().stream().filter(condition).findFirst().orElse(null);
    }

    private String getRedisKey(String id) {
        return KEY_PREFIX + id;
    }

    /**
     * 清理key列表中无效的key
     *
     * 每天0点执行
     */
    @Scheduled(cron = "1 0 0 * * ?")
    public void cleanInvalidKey() {
        List<String> keys = stringRedisTemplate.opsForList().range(ALL_KEY_PREFIX, 0, 10000);
        if (keys==null){
            return;
        }
        for (String key : keys) {
            Long counted = stringRedisTemplate.countExistingKeys(Arrays.asList(key));
            if (counted != null && counted == 0) {
                stringRedisTemplate.opsForList().remove(ALL_KEY_PREFIX,1,key);
            }
        }
    }

}
