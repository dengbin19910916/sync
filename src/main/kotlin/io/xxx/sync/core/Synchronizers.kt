package io.xxx.sync.core

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.util.StopWatch
import org.springframework.web.client.RestTemplate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 同步数据并更新[SyncSchedule]
 */
abstract class AbstractSynchronizer(protected var property: SyncProperty) : Job {

    @Autowired
    private lateinit var scheduleMapper: SyncScheduleMapper

    @Autowired
    private lateinit var restTemplateBuilder: RestTemplateBuilder

    protected val restTemplate: RestTemplate by lazy {
        restTemplateBuilder.build()
    }

    override fun execute(context: JobExecutionContext) {
        pullAndSave()
    }

    /**
     * 拉取并保存数据
     */
    private fun pullAndSave() {
        fun pullAndSave0(parameter: Any? = null) {
            getUncompletedSchedules().forEach { schedule ->
                val stopWatch = StopWatch()
                if (log.isDebugEnabled) {
                    log.debug("Synchronizer[{}][{}, {}] started.", schedule.id,
                            schedule.startTime.format(formatter), schedule.endTime.format(formatter))
                }
                pullAndSave(schedule, parameter)
                updateSchedule(schedule)
                stopWatch.stop()
                if (log.isDebugEnabled) {
                    val spendTime = if (stopWatch.totalTimeMillis >= 1000)
                        (stopWatch.totalTimeMillis / 1000).toString() + "s"
                    else
                        stopWatch.totalTimeMillis.toString() + "ms"
                    log.debug("Synchronizer[{}][{}, {}][{}, {}] completed.", schedule.id,
                            schedule.startTime.format(formatter), schedule.endTime.format(formatter),
                    schedule.count, spendTime)
                }
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
        private var log = LoggerFactory.getLogger(AbstractSynchronizer::class.java)
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")!!
    }
}

abstract class DocumentSynchronizer(property: SyncProperty) : AbstractSynchronizer(property) {

    @Autowired
    private lateinit var documentMapper: SyncDocumentMapper

    open fun saveData(shopCode: String, schedule: SyncSchedule, parameter: Any?, document: SyncDocument) {
        document.shopCode = shopCode
        val wrapper = QueryWrapper<SyncDocument>()
                .eq("property_id", document.propertyId)
                .eq("sn", document.sn)
        val oldDocument = documentMapper.selectOne(wrapper)
        val now = LocalDateTime.now()
        if (oldDocument == null) {
            if (document.syncCreated == null) {
                document.syncCreated = now
            }
            if (document.syncModified == null) {
                document.syncModified = now
            }
            documentMapper.insert(document)
        } else {
            if (document.syncModified == null) {
                document.syncModified = now
            }
            if (document.modified.isAfter(oldDocument.modified)) {
                document.id = oldDocument.id
                documentMapper.updateById(document)
            }
        }
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

        val stopWatch = StopWatch()
        stopWatch.start()

        val shopCodes = property.shopCode.split(",")
        val targetShopCode = shopCodes[0]
        shopCodes.forEach { shopCode ->
            val (getCountTime, count) = execute {
                getCount(shopCode, schedule, parameter)
            }
            schedule.pullMillis += getCountTime
            var pages = if (count == null) 0 else (count / pageSize + if (count % pageSize == 0L) 0 else 1)
            while (pages-- > 0) {
                val (getDataTime, data) = execute {
                    getData(shopCode, schedule, parameter, pages + startPage)
                            .onEach {
                                it.propertyId = property.id
                                it.shopCode = property.shopCode
                                it.shopName = property.shopName
                            }
                }
                schedule.pullMillis += getDataTime
                schedule.count += data.size
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

        stopWatch.stop()
        schedule.totalMillis += stopWatch.totalTimeMillis
    }
}