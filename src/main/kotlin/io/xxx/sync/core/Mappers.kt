package io.xxx.sync.core

import com.baomidou.mybatisplus.core.mapper.BaseMapper
import org.apache.ibatis.annotations.Mapper
import org.apache.ibatis.annotations.Param
import org.springframework.stereotype.Repository

@Mapper
@Repository
interface JobPropertyMapper : BaseMapper<JobProperty>

@Mapper
@Repository
interface SyncPropertyMapper : BaseMapper<SyncProperty>

@Mapper
@Repository
interface SyncScheduleMapper : BaseMapper<SyncSchedule> {

    fun insertAll(@Param("schedules") schedules: Collection<SyncSchedule>)
}

@Mapper
@Repository
interface SyncDocumentMapper : BaseMapper<SyncDocument>