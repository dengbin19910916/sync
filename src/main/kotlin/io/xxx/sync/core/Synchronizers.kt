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

    /**
     * 拉取并保存数据
     */
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

    /**
     * 扩展参数由子类提供并共享给父类方法
     */
    open fun getParameters(): List<Any> = emptyList()

    private fun getUncompletedSchedules(): List<SyncSchedule> {
        val wrapper = QueryWrapper<SyncSchedule>()
        wrapper.eq("property_id", property.id)
                .eq("completed", 0)
                .orderByDesc("priority")
                .orderByAsc("start_time")
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
 * 通过分页的方式同步数据
 */
abstract class PageDocumentSynchronizer(property: SyncProperty) : DocumentSynchronizer(property) {

    /**
     * 默认数据起始页码为1
     */
    open val startPage by lazy {
        property.startPage
    }

    /**
     * 默认每页100条数据
     */
    open val pageSize = 100

    /**
     * 返回数据总数
     */
    abstract fun getCount(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long?

    /**
     * 返回数据对象，需要将原始数据包装成[SyncDocument]
     */
    abstract fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument>

    override fun pullAndSave(schedule: SyncSchedule, parameter: Any?) {
        fun <T> execute(action: () -> T): Pair<Long, T> {
            val stopWatch = StopWatch()
            stopWatch.start()
            val result = action()
            stopWatch.stop()
            return Pair(stopWatch.totalTimeMillis, result)
        }

        val shopCodes = property.shopCode.split(",")
        val targetShopCode = shopCodes[0]
        shopCodes.forEach { shopCode ->
            val (getCountTime, count) = execute {
                getCount(shopCode, schedule, parameter)
            }
            schedule.pullMillis += getCountTime
            var pages = if (count == null) 0 else count / pageSize + startPage
            while (pages-- > 0) {
                val (getDataTime, data) = execute {
                    getData(shopCode, schedule, parameter, pages + startPage)
                }
                schedule.pullMillis += getDataTime
                if (!data.isEmpty()) {
                    data.parallelStream().forEach {
                        val (saveDataTime, _) = execute {
                            saveData(targetShopCode, schedule, parameter, it)
                        }
                        schedule.saveMillis += saveDataTime
                    }
                }
            }
        }
    }
}