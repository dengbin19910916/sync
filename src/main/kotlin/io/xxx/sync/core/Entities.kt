package io.xxx.sync.core

import com.baomidou.mybatisplus.annotation.TableField
import com.baomidou.mybatisplus.annotation.TableId
import com.baomidou.mybatisplus.annotation.TableName
import com.baomidou.mybatisplus.core.toolkit.IdWorker
import java.lang.RuntimeException
import java.time.LocalDateTime

@TableName("job_property")
data class JobProperty(@TableId var name: String,
                       var description: String,
                       var enabled: Boolean,
                       var address: String,
                       var beanName: String,
                       var cron: String,
                       var sign: String?)

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
    val countUrl: String
        get() {
            if (host == null) {
                throw RuntimeException("Host is empty.")
            }
            return host + countPath
        }

    val dataUrl: String
        get() {
            if (host == null) {
                throw RuntimeException("Host is empty.")
            }
            return host + countPath
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

    @TableField(exist = false)
    val spendTime: String = if (totalMillis > 1000) (totalMillis / 1000).toString() + "s" else totalMillis.toString() + "ms"
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