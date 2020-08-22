package io.xxx.sync.config

import io.xxx.sync.core.JobProperty
import io.xxx.sync.core.JobPropertyMapper
import io.xxx.sync.core.SyncPropertyMapper
import lombok.extern.slf4j.Slf4j
import org.quartz.*
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.support.BeanDefinitionBuilder
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.GenericApplicationContext
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.net.InetAddress

@Slf4j
@Configuration
class SyncConfig : ApplicationRunner, ApplicationContextAware {

    @Autowired
    private lateinit var propertyMapper: SyncPropertyMapper

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Synchronized
    override fun run(args: ApplicationArguments) {
        val properties = propertyMapper.selectList(null)
        for (property in properties) {
            try {
                val clazz = property.beanClass()
                val beanName = property.beanName()
                val builder = BeanDefinitionBuilder.genericBeanDefinition(clazz)
                        .addConstructorArgValue(property)
                if (!applicationContext.isBeanNameInUse(beanName)) {
                    applicationContext.registerBeanDefinition(beanName, builder.beanDefinition)
                }
            } catch (e: Exception) {
                if (log.isWarnEnabled) {
                    log.warn("Create synchronizer bean[${property.id}] failed.", e)
                }
            }
        }

        if (log.isInfoEnabled) {
            log.info("Synchronizer init completed.")
        }
    }

    override fun setApplicationContext(applicationContext: ApplicationContext) {
        this.applicationContext = applicationContext as GenericApplicationContext
    }

    companion object {

        val log: Logger = LoggerFactory.getLogger(SyncConfig::class.java)
    }
}

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
class ProxyJob : Job {

    override fun execute(context: JobExecutionContext) {
        val jobProperty = context.mergedJobDataMap["jobProperty"] as JobProperty
        val applicationContext = context.mergedJobDataMap["applicationContext"] as ApplicationContext
        val synchronizer = applicationContext.getBean(jobProperty.beanName, Job::class.java)
        synchronizer.execute(context)
    }
}

@Component
class JobManager {

    @Autowired
    private lateinit var jobPropertyMapper: JobPropertyMapper

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    init {
        scheduler.start()
    }

    @Scheduled(cron = "*/3 * * * * ?")
    fun loadJobs() {
        val jobProperties = jobPropertyMapper.selectList(null)
        if (jobProperties.isEmpty()) {
            return
        }

        for (jobProperty in jobProperties) {
            val cachedJobProperty = JOB_PROPERTY_CACHE[jobProperty.name]
            if (cachedJobProperty == null) {
                scheduleJob(jobProperty)
            } else {
                if (jobProperty != cachedJobProperty) {
                    val jobKey = jobProperty.jobKey
                    if (scheduler.checkExists(jobKey)) {
                        scheduler.deleteJob(jobKey)
                    }
                    scheduleJob(jobProperty)
                }
            }
            JOB_PROPERTY_CACHE[jobProperty.name] = jobProperty
        }

        val jobNames = jobProperties.map { it.name }.toSet()
        JOB_PROPERTY_CACHE.forEach { (jobName, _) ->
            if (!jobNames.contains(jobName)) {
                val jobKey = jobName.getJobKey()
                scheduler.deleteJob(jobKey)
                JOB_PROPERTY_CACHE.remove(jobKey.name)
            }
        }
    }

    private fun scheduleJob(jobProperty: JobProperty) {
        val address = InetAddress.getLocalHost().hostAddress
        if (address == jobProperty.address) {
            val jobDetail = getJobDetail(jobProperty)
            if (jobProperty.enabled) {
                if (!scheduler.checkExists(jobDetail.key)) {
                    val trigger = getTrigger(jobProperty)
                    if (trigger != null) {
                        scheduler.scheduleJob(jobDetail, trigger)
                    }
                }
            } else {
                if (scheduler.checkExists(jobDetail.key)) {
                    scheduler.deleteJob(jobDetail.key)
                }
            }
        }
    }

    private fun getJobDetail(jobProperty: JobProperty): JobDetail {
        val jobDataMap = JobDataMap()
        jobDataMap["jobProperty"] = jobProperty
        jobDataMap["applicationContext"] = applicationContext
        return JobBuilder.newJob(ProxyJob::class.java)
                .withIdentity(jobProperty.jobKey)
                .withDescription(jobProperty.description)
                .usingJobData(jobDataMap)
                .storeDurably()
                .build()
    }

    private fun getTrigger(jobProperty: JobProperty): Trigger? {
        if (!CronExpression.isValidExpression(jobProperty.cron)) {
            log.warn("Job[{},{}] cron expression [{}] is not valid.",
                    jobProperty.name, jobProperty.description, jobProperty.cron)
            return null
        }
        return TriggerBuilder.newTrigger()
                .withIdentity(jobProperty.triggerKey)
                .withDescription(jobProperty.description)
                .withSchedule(CronScheduleBuilder.cronSchedule(jobProperty.cron))
                .build()
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(JobManager::class.java)
        private val JOB_PROPERTY_CACHE = mutableMapOf<String, JobProperty>()
        private val scheduler: Scheduler = StdSchedulerFactory().scheduler

        fun String.getJobKey(): JobKey {
            return JobKey(this + "Job")
        }
    }
}