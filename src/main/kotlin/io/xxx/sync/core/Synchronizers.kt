package io.xxx.sync.core

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.baomidou.mybatisplus.core.toolkit.IdWorker
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.util.StopWatch
import java.time.format.DateTimeFormatter

/**
 * 同步数据并更新[SyncSchedule]
 */
abstract class AbstractSynchronizer(protected var property: SyncProperty) : Job {

    override fun execute(context: JobExecutionContext) {
        pullAndSave()
    }

    private fun pullAndSave() {
        fun pullAndSave0(parameter: Any? = null) {
            getUncompletedSchedules().forEach { schedule ->
                pullAndSave(schedule, parameter)
                updateSchedule(schedule)
            }
        }

        if (getParameters().isEmpty()) {
            pullAndSave0()
        } else {
            getParameters().forEach { parameter ->
                pullAndSave0(parameter)
            }
        }
    }

    open fun getParameters(): List<Any> = emptyList()

    private fun getUncompletedSchedules(): List<SyncSchedule> {
        val wrapper = QueryWrapper<SyncSchedule>()
        wrapper.eq("property_id", property.id)
                .eq("completed", 0)
                .last("limit 120")
        return scheduleMapper.selectList(wrapper)
    }

    abstract fun pullAndSave(schedule: SyncSchedule, parameter: Any?)

    private fun updateSchedule(schedule: SyncSchedule) {
        schedule.completed = true
        scheduleMapper.updateById(schedule)
    }

    companion object {
        @Autowired
        private lateinit var scheduleMapper: SyncScheduleMapper

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")!!
    }
}

abstract class DocumentSynchronizer(property: SyncProperty) : AbstractSynchronizer(property) {

    open fun saveData(shopCode: String, schedule: SyncSchedule, parameter: Any?, document: SyncDocument) {
        document.id = IdWorker.getId()
        document.propertyId = property.id
        document.shopCode = shopCode
        document.shopName = property.shopName
        documentMapper.insert(document)
    }

    companion object {
        @Autowired
        private lateinit var documentMapper: SyncDocumentMapper
    }
}

/**
 * 通过分页的方式同步数据。
 */
abstract class PageDocumentSynchronizer(property: SyncProperty) : DocumentSynchronizer(property) {

    /**
     * 默认每页100条数据。
     */
    open val pageSize = 100

    /**
     * 返回数据总数。
     */
    abstract fun getCount(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long?

    /**
     * 返回数据对象，需要将原始数据包装成[SyncDocument]
     */
    abstract fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument>

    override fun pullAndSave(schedule: SyncSchedule, parameter: Any?) {
        fun getCount0(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long {
            val stopWatch = StopWatch()
            stopWatch.start()
            var count = getCount(shopCode, schedule, parameter)
            if (count == null) {
                count = 0L
            }
            stopWatch.stop()
            schedule.pullMillis += stopWatch.totalTimeMillis
            schedule.totalCount = count
            return count
        }

        fun getData0(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument> {
            val stopWatch = StopWatch()
            stopWatch.start()
            val data = getData(shopCode, schedule, parameter, pageNo)
            stopWatch.stop()
            schedule.pullMillis += stopWatch.totalTimeMillis
            return data
        }

        fun saveData0(shopCode: String, schedule: SyncSchedule, parameter: Any?, document: SyncDocument) {
            val stopWatch = StopWatch()
            stopWatch.start()
            saveData(shopCode, schedule, parameter, document)
            stopWatch.stop()
            schedule.saveMillis += stopWatch.totalTimeMillis
        }

        val shopCodes = property.shopCode.split(",")
        val targetShopCode = shopCodes[0]
        shopCodes.forEach { shopCode ->
            var pages = getCount0(shopCode, schedule, parameter) / pageSize
            while (pages-- > 0) {
                val data = getData0(shopCode, schedule, parameter, pages)
                if (!data.isEmpty()) {
                    data.parallelStream().forEach { saveData0(targetShopCode, schedule, parameter, it) }
                }
            }
        }
    }
}