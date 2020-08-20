package io.xxx.sync.core

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.jd.open.api.sdk.DefaultJdClient
import com.jd.open.api.sdk.request.supplier.DropshipDpsSearchRequest
import com.jd.open.api.sdk.request.supplier.DropshipDpsSearchpreRequest
import com.jd.open.api.sdk.response.supplier.DropshipDpsSearchResponse
import com.jd.open.api.sdk.response.supplier.DropshipDpsSearchpreResponse
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import kotlin.reflect.jvm.jvmName

/**
 * 京东自营订单
 */
@Suppress("unused")
class JdDpsOrderSynchronizer(property: SyncProperty) : PageDocumentSynchronizer(property) {

    override fun getCount(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long? {
        val response = getResponse(shopCode, schedule, 1)
        return response.searchResult.recordCount?.toLong()
    }

    override fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument> {
        val response = getResponse(shopCode, schedule, pageNo.toInt())
        return response.searchResult.resultDtoList?.map {
            SyncDocument(it.customOrderId.toString(), JSON.toJSONString(it),
                    it.createDate.toLocalDateTime(), it.orderCreateDate.toLocalDateTime())
        }!!

    }

    private fun getResponse(shopCode: String, schedule: SyncSchedule, pageNumber: Int): DropshipDpsSearchResponse {
        val request = DropshipDpsSearchRequest()
        request.pageSize = pageSize
        request.page = pageNumber
        request.beginDate = schedule.startTime.toDate()
        request.endDate = schedule.endTime.toDate()
        val url = "http://114.67.201.245:3389/api/execute?shopCode=${shopCode}&requestClass=${request::class.jvmName}"
        val httpEntity = HttpEntity<Any>(request, null)
        val jsonObject = restTemplate.exchange(url, HttpMethod.POST, httpEntity, JSONObject::class.java)
                .body!!
        return jsonObject.toJavaObject(request.responseClass)
    }

    companion object {
        fun LocalDateTime.toDate(): Date {
            return Date.from(this.atZone(ZoneId.systemDefault()).toInstant())
        }

        fun Date.toLocalDateTime(): LocalDateTime {
            return LocalDateTime.ofInstant(this.toInstant(), ZoneId.systemDefault())
        }
    }
}

/**
 * 京东自营售后单
 */
@Suppress("unused")
class JdDpsRefundSynchronizer(property: SyncProperty) : PageDocumentSynchronizer(property) {

    override val pageSize: Int get() = 50

    override fun getCount(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long? {
        val response = getResponse(schedule, 1)
        return response.searchPreResult.recordCount?.toLong()
    }

    override fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument> {
        val response = getResponse(schedule, pageNo.toInt())
        return response.searchPreResult.resultDtoList?.map {
            val modifiedDate: LocalDateTime = if (it.modifiedDate == null) it.roApplyDate.toLocalDateTime()
            else it.modifiedDate.toLocalDateTime()
            SyncDocument(it.roPreNo.toString(), it.customOrderId.toString(), JSON.toJSONString(it),
                    it.roApplyDate.toLocalDateTime(), modifiedDate)
        }!!
    }

    private fun getResponse(schedule: SyncSchedule, pageNo: Int): DropshipDpsSearchpreResponse {
        val request = DropshipDpsSearchpreRequest()
        request.pageSize = pageSize
        request.page = pageNo
        request.beginDate = schedule.startTime.toDate()
        request.endDate = schedule.endTime.toDate()
        return jdClient.execute(request)
    }

    companion object {
        private val jdClient = DefaultJdClient(
                "http://api.jd.com/routerjson", "e1085458fffe44e988231584556bcbe4ztew",
                "C435BE5E5410BBE2B8CC5D57B96F2A87", "87fdccacafd2407eb84d2f52d81e6959")

        fun LocalDateTime.toDate(): Date {
            return Date.from(this.atZone(ZoneId.systemDefault()).toInstant())!!
        }

        fun Date.toLocalDateTime(): LocalDateTime {
            return LocalDateTime.ofInstant(this.toInstant(), ZoneId.systemDefault())
        }
    }
}