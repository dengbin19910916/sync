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
    private lateinit var applicationContext: GenericApplicationContext

    override fun run(args: ApplicationArguments) {
        synchronized(this) {
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

    @Scheduled(cron = "*/3 * * * * ?")
    fun loadJobs() {
        val jobProperties = jobPropertyMapper.selectList(null)
        if (jobProperties.isEmpty()) {
            return
        }

        for (jobProperty in jobProperties) {
            val cachedJobProperty = JOB_PROPERTY_CACHE[jobProperty.beanName]
            if (cachedJobProperty == null) {
                scheduleJob(jobProperty)
            } else {
                if (jobProperty != cachedJobProperty) {
                    val jobKey = jobProperty.jobKey
                    if (scheduler.checkExists(jobKey)) {
                        scheduler.deleteJob(jobKey)
                        if (log.isInfoEnabled) {
                            log.info("Job[{}, {}] is deleted.", jobProperty.beanName, cachedJobProperty.description)
                        }
                    }
                    scheduleJob(jobProperty)
                }
            }
            JOB_PROPERTY_CACHE[jobProperty.beanName] = jobProperty
        }

        val jobNames = jobProperties.map { it.beanName }.toSet()
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
            val jobDetail = jobProperty.jobDetail
            val jobDataMap = jobDetail.jobDataMap
            jobDataMap["jobProperty"] = jobProperty
            jobDataMap["applicationContext"] = applicationContext
            if (jobProperty.enabled) {
                if (!scheduler.checkExists(jobDetail.key)) {
                    val trigger = jobProperty.trigger
                    if (trigger != null) {
                        scheduler.scheduleJob(jobDetail, trigger)
                        if (log.isInfoEnabled) {
                            log.info("Job[{}, {}] is started.", jobProperty.beanName, jobProperty.description)
                        }
                    }
                }
            } else {
                if (scheduler.checkExists(jobDetail.key)) {
                    scheduler.deleteJob(jobDetail.key)
                    if (log.isInfoEnabled) {
                        log.info("Job[{}, {}] is stopped.", jobProperty.beanName, jobProperty.description)
                    }
                }
            }
        }
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(JobManager::class.java)
        private val JOB_PROPERTY_CACHE = mutableMapOf<String, JobProperty>()
        private val scheduler: Scheduler = StdSchedulerFactory().scheduler

        init {
            scheduler.start()
        }

        fun String.getJobKey(): JobKey {
            return JobKey(this + "Job")
        }
    }
}