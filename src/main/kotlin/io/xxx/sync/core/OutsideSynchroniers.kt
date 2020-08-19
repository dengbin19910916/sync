package io.xxx.sync.core

import com.alibaba.fastjson.JSON
import com.jd.open.api.sdk.DefaultJdClient
import com.jd.open.api.sdk.request.supplier.DropshipDpsSearchRequest
import com.jd.open.api.sdk.request.supplier.DropshipDpsSearchpreRequest
import com.jd.open.api.sdk.response.supplier.DropshipDpsSearchResponse
import com.jd.open.api.sdk.response.supplier.DropshipDpsSearchpreResponse
import com.jd.security.tdeclient.SecretJdClient
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*

/**
 * 京东自营订单
 */
@Suppress("unused")
class JdDpsOrderSynchronizer(property: SyncProperty) : PageDocumentSynchronizer(property) {

    override fun getCount(shopCode: String, schedule: SyncSchedule, parameter: Any?): Long? {
        val response = getResponse(schedule, 1)
        return response.searchResult.recordCount as Long?
    }

    override fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument> {
        val response = getResponse(schedule, pageNo.toInt())
        return response.searchResult.resultDtoList
                .map {
                    SyncDocument(it.customOrderId.toString(), JSON.toJSONString(it),
                            it.createDate.toLocalDateTime(), it.orderTime.toLocalDateTime())
                }.toList()

    }

    private fun getResponse(schedule: SyncSchedule, pageNumber: Int): DropshipDpsSearchResponse {
        val request = DropshipDpsSearchRequest()
        request.pageSize = pageSize
        request.page = pageNumber
        request.beginDate = schedule.startTime.toDate()
        request.endDate = schedule.endTime.toDate()
        return jdClient.execute(request)
    }

    companion object {
        private val jdClient = DefaultJdClient("", "", "", "")
        private val tdeClient = SecretJdClient.getInstance("", "", "", "")!!

        fun LocalDateTime.toDate(): Date {
            return Date.from(this.atZone(ZoneId.systemDefault()).toInstant())!!
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
        return response.searchPreResult.recordCount as Long?
    }

    override fun getData(shopCode: String, schedule: SyncSchedule, parameter: Any?, pageNo: Long): Collection<SyncDocument> {
        val response = getResponse(schedule, pageNo.toInt())
        return response.searchPreResult.resultDtoList
                .map {
                    val modifiedDate: LocalDateTime = if (it.modifiedDate == null) it.roApplyDate.toLocalDateTime()
                    else it.modifiedDate.toLocalDateTime()
                    SyncDocument(it.customOrderId.toString(), "", JSON.toJSONString(it),
                            it.roApplyDate.toLocalDateTime(), modifiedDate)
                }.toList()
    }

    override fun getParameters(): List<Any> = listOf(1L, 11L)

    private fun getResponse(schedule: SyncSchedule, pageNo: Int): DropshipDpsSearchpreResponse {
        val request = DropshipDpsSearchpreRequest()
        request.pageSize = pageSize
        request.page = pageNo
        request.beginDate = schedule.startTime.toDate()
        request.endDate = schedule.endTime.toDate()
        return jdClient.execute(request)
    }

    companion object {
        val jdClient = DefaultJdClient("", "", "", "")
        val tdeClient = SecretJdClient.getInstance("", "", "", "")!!

        fun LocalDateTime.toDate(): Date {
            return Date.from(this.atZone(ZoneId.systemDefault()).toInstant())!!
        }

        fun Date.toLocalDateTime(): LocalDateTime {
            return LocalDateTime.ofInstant(this.toInstant(), ZoneId.systemDefault())
        }
    }
}