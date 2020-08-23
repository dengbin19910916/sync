package io.xxx.sync.core

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSONPath
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.http.RequestEntity
import org.springframework.util.ObjectUtils
import org.springframework.util.StopWatch
import org.springframework.web.client.RestTemplate
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 同步数据并更新[SyncSchedule]
 */
@Suppress("SpringJavaAutowiredMembersInspection")
abstract class AbstractSynchronizer(protected var property: SyncProperty) : Job {

    @Autowired
    private lateinit var propertyMapper: SyncPropertyMapper

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
        fun debug(id: Any, schedule: SyncSchedule) {
            if (log.isDebugEnabled) {
                log.debug("Synchronizer[{}][{}, {}][{}, {}] completed.", id,
                        schedule.startTime.format(formatter), schedule.endTime.format(formatter),
                        schedule.count, schedule.spendTime)
            }
        }

        fun updateSchedule(schedule: SyncSchedule) {
            schedule.completed = true
            scheduleMapper.updateById(schedule)
        }

        fun composePullAndSave(parameter: Any? = null) {
            val uncompletedSchedules = getUncompletedSchedules()
            val minStartTime = uncompletedSchedules.minByOrNull { it.startTime }?.startTime
            val maxEndTime = uncompletedSchedules.maxByOrNull { it.endTime }?.endTime

            if (minStartTime != null && maxEndTime != null) {
                val defaultId = 1000000000000000000
                val schedule = SyncSchedule(defaultId, property.id, minStartTime, maxEndTime,
                        0, false, 0, 0, 0, 0)
                pullAndSave(schedule, parameter)
                uncompletedSchedules.forEach { updateSchedule(it) }
                debug(defaultId, schedule)
            }
        }

        fun singlePullAndSave(parameter: Any? = null) {
            getUncompletedSchedules().forEach { schedule ->
                pullAndSave(schedule, parameter)
                updateSchedule(schedule)
                debug(schedule.id, schedule)
            }
        }

        fun pullAndSave0(parameter: Any? = null) {
            if (property.compositional) {
                composePullAndSave(parameter)
            } else {
                singlePullAndSave(parameter)
            }
        }

        if (!property.fired) {
            return
        }
        val property = propertyMapper.selectById(property.id) // refresh property

        if (property == null) {
            if (log.isWarnEnabled) {
                log.warn("Property {} is not found, job is stopped.", this.property.id)
            }
            return
        } else {
            this.property = property
        }
        if (parameters.isEmpty()) {
            pullAndSave0()
        } else {
            parameters.forEach { parameter ->
                pullAndSave0(parameter)
            }
        }
    }

    /**
     * 扩展参数由子类提供并共享给父类方法
     */
    open val parameters: List<Any> = emptyList()

    private fun getUncompletedSchedules(): List<SyncSchedule> {
        val wrapper = QueryWrapper<SyncSchedule>()
        wrapper.eq("property_id", property.id)
                .eq("completed", 0)
                .orderByDesc("priority")
                .orderByAsc("start_time")
                .last("limit 120")
        return scheduleMapper.selectList(wrapper)
    }

    /**
     * 拉取并保存数据
     */
    abstract fun pullAndSave(schedule: SyncSchedule, parameter: Any?)

    companion object {
        private var log = LoggerFactory.getLogger(AbstractSynchronizer::class.java)
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")!!
    }
}

@Suppress("SpringJavaAutowiredMembersInspection")
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
    open fun getCount(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long? {
        val headers = HttpHeaders()
        headers["Content-Type"] = listOf(MediaType.APPLICATION_JSON_VALUE)
        val requestEntity = RequestEntity<Any>(headers, HttpMethod.GET, URI.create(property.countUrl))
        return if (ObjectUtils.isEmpty(property.countJsonPath)) {
            restTemplate.exchange(requestEntity, Long::class.java).body
        } else {
            val response = restTemplate.exchange(requestEntity, JSONObject::class.java).body
            if (response != null) {
                JSONPath.eval(response, property.countJsonPath).toString().toLong()
            } else {
                0
            }
        }
    }

    /**
     * 返回数据对象，需要将原始数据包装成[SyncDocument]
     */
    open fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument> {
        fun buildDocument(it: JSONObject): SyncDocument {
            return if (property.type == 1.toByte()) {
                SyncDocument(JSONPath.eval(it, property.snJsonPath).toString(), JSON.toJSONString(it),
                        LocalDateTime.now(), LocalDateTime.now())
            } else {
                SyncDocument(JSONPath.eval(it, property.snJsonPath).toString(),
                        JSONPath.eval(it, property.rsnJsonPath).toString(), JSON.toJSONString(it),
                        LocalDateTime.now(), LocalDateTime.now())
            }
        }

        val headers = HttpHeaders()
        headers["Content-Type"] = listOf(MediaType.APPLICATION_JSON_VALUE)
        val requestEntity = RequestEntity<Any>(headers, HttpMethod.GET, URI.create(property.dataUrl))
        return if (ObjectUtils.isEmpty(property.dataJsonPath)) {
            val response = restTemplate.exchange(requestEntity,
                    object : ParameterizedTypeReference<List<JSONObject>>() {}).body
            response?.map { buildDocument(it) } ?: emptyList()
        } else {
            val response = restTemplate.exchange(requestEntity, JSONObject::class.java).body
            if (response != null) {
                val jsonArray = JSONPath.eval(response, property.dataJsonPath) as JSONArray
                jsonArray.map { buildDocument(it as JSONObject) }
            } else {
                emptyList()
            }
        }
    }

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
            val (getCountTime, count) = execute { getCount(shopCode, schedule, parameter) }
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