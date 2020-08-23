package io.xxx.sync.core

import com.baomidou.mybatisplus.annotation.TableId
import com.baomidou.mybatisplus.annotation.TableName
import com.baomidou.mybatisplus.core.toolkit.IdWorker
import io.xxx.sync.config.ProxyJob
import org.quartz.*
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

@TableName("job_property")
data class JobProperty(@TableId var beanName: String,
                       var description: String,
                       var enabled: Boolean,
                       var address: String,
                       var cron: String) {

    val jobKey
        get() = JobKey(beanName + "Job")

    val jobDetail
        get() = JobBuilder.newJob(ProxyJob::class.java)
                .withIdentity(jobKey)
                .withDescription(description)
                .storeDurably()
                .build()!!

    val trigger
        get() = if (!CronExpression.isValidExpression(cron)) {
            log.warn("Job[{},{}] cron expression [{}] is not valid.",
                    beanName, description, cron)
            null
        } else {
            TriggerBuilder.newTrigger()
                    .withIdentity(TriggerKey(beanName + "Trigger"))
                    .withDescription(description)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build()
        }

    override fun equals(other: Any?): Boolean {
        return if (other == null || other !is JobProperty) {
            false
        } else {
            beanName == other.beanName
                    && description == other.description
                    && enabled == other.enabled
                    && address == other.address
                    && cron == other.cron
        }
    }

    override fun hashCode(): Int {
        var result = beanName.hashCode()
        result = 31 * result + description.hashCode()
        result = 31 * result + enabled.hashCode()
        result = 31 * result + address.hashCode()
        result = 31 * result + cron.hashCode()
        return result
    }

    companion object {
        private val log = LoggerFactory.getLogger(JobProperty::class.java)
    }
}

@TableName("sync_property")
data class SyncProperty(var id: Long,
                        var type: Byte,
                        var shopCode: String,
                        var shopName: String,
                        var originTime: LocalDateTime,
                        var startPage: Int = 1,
                        var delay: Int = 60,
                        var timeInterval: Int = 60,
                        var host: String?,
                        var countPath: String?,
                        var dataPath: String?,
                        var countJsonPath: String?,
                        var dataJsonPath: String?,
                        var snJsonPath: String?,
                        var rsnJsonPath: String?,
                        var createdJsonPath: String?,
                        var modifiedJsonPath: String?,
                        var beanName: String?,
                        var beanClass: String?,
                        var enabled: Boolean,
                        var fired: Boolean,
                        var compositional: Boolean) {
    val countUrl
        get() = if (host == null) {
            throw RuntimeException("Host is empty.")
        } else {
            host + countPath
        }

    val dataUrl
        get() = if (host == null) {
            throw RuntimeException("Host is empty.")
        } else {
            host + countPath
        }

    fun beanClass(): Class<*> {
        return Class.forName(beanClass)
    }

    fun beanName(): String {
        return if (beanName == null)
            Class.forName(beanClass).simpleName.decapitalize() + shopCode
        else beanName!!
    }
}

@TableName("sync_schedule")
data class SyncSchedule(var id: Long,
                        var propertyId: Long,
                        var startTime: LocalDateTime,
                        var endTime: LocalDateTime,
                        var priority: Int = 0,
                        var completed: Boolean = false,
                        var count: Long = 0,
                        var pullMillis: Long = 0,
                        var saveMillis: Long = 0,
                        var totalMillis: Long = 0) {

    val spendTime
        get() = if (totalMillis > 1000) (totalMillis / 1000).toString() + "s"
        else totalMillis.toString() + "ms"

    constructor(id: Long,
                propertyId: Long,
                startTime: LocalDateTime,
                endTime: LocalDateTime)
            : this(id, propertyId, startTime, endTime, 0,
            false, 0, 0, 0, 0)
}

/**
 * 数据包装类型
 */
@TableName("sync_document")
data class SyncDocument(var id: Long?,
                        var propertyId: Long?,
                        var shopCode: String?,
                        var shopName: String?,
                        var sn: String,
                        var rsn: String?,
                        var data: String,
                        var created: LocalDateTime,
                        var modified: LocalDateTime,
                        var syncCreated: LocalDateTime?,
                        var syncModified: LocalDateTime?) {

    init {
        val now = LocalDateTime.now()
        if (syncCreated == null) {
            syncCreated = now
        }
        if (syncModified == null) {
            syncModified = now
        }
    }

    constructor(sn: String,
                data: String,
                created: LocalDateTime,
                modified: LocalDateTime)
            : this(IdWorker.getId(), null, null, null, sn, null,
            data, created, modified, null, null)

    constructor(sn: String,
                rsn: String,
                data: String,
                created: LocalDateTime,
                modified: LocalDateTime)
            : this(IdWorker.getId(), null, null, null, sn, rsn,
            data, created, modified, null, null)
}