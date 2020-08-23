package io.xxx.sync.core

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.baomidou.mybatisplus.core.toolkit.IdWorker
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime

@Component
class ScheduleCalendar : Job {

    @Autowired
    private lateinit var propertyMapper: SyncPropertyMapper

    @Autowired
    private lateinit var scheduleMapper: SyncScheduleMapper

    override fun execute(context: JobExecutionContext) {
        val wrapper = QueryWrapper<SyncProperty>()
        wrapper.eq("enabled", 1)
        val properties: List<SyncProperty> = propertyMapper.selectList(wrapper)
        properties.forEach { property -> this.saveUncompletedSchedules(property) }
    }

    private fun saveUncompletedSchedules(property: SyncProperty) {
        val wrapper = QueryWrapper<SyncSchedule>()
        wrapper.eq("property_id", property.id)
        wrapper.orderByDesc("end_time")
        wrapper.last("limit 1")
        val lastSchedule = scheduleMapper.selectOne(wrapper)
        val startTime = lastSchedule?.endTime ?: property.originTime
        val now = LocalDateTime.now()
        val maxEndTime = now.minusSeconds(property.delay.toLong())
        val duration = Duration.between(startTime, maxEndTime)
        val betweenSeconds = duration.seconds
        val count = (betweenSeconds / property.timeInterval)

        val schedules = mutableListOf<SyncSchedule>()
        for (i in 0 until count) {
            val newStartTime = startTime.plusSeconds(property.timeInterval * i)
            val schedule = buildSchedule(property, newStartTime)
            schedules.add(schedule)
        }
        if (schedules.isNotEmpty()) {
            scheduleMapper.insertAll(schedules)
        }
    }

    private fun buildSchedule(property: SyncProperty, startTime: LocalDateTime): SyncSchedule {
        val endTime = startTime.plusSeconds(property.timeInterval.toLong())
        return SyncSchedule(IdWorker.getId(), property.id, startTime, endTime,
                0, false, 0, 0, 0, 0)
    }
}