package io.xxx.sync.config

import io.xxx.sync.core.*
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
import org.springframework.util.ObjectUtils
import java.net.InetAddress
import java.util.stream.Collectors

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
                log.warn("Create synchronizer bean[${property.id}] failed.", e)
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
            val sign = jobProperty.sign()
            if (!JOB_PROPERTY_MAP.containsKey(jobProperty.name)) {
                if (ObjectUtils.isEmpty(jobProperty.sign) || jobProperty.sign != sign) {
                    updateJobProperty(jobProperty, sign)
                }
                scheduleJob(jobProperty)
            } else {
                if (jobProperty.sign == null || jobProperty.sign != sign) {
                    updateJobProperty(jobProperty, sign)
                    val jobKey = JobKey(jobProperty.beanName + "Job")
                    if (scheduler.checkExists(jobKey)) {
                        scheduler.deleteJob(jobKey)
                    }
                    scheduleJob(jobProperty)
                }
            }
            JOB_PROPERTY_MAP[jobProperty.name] = jobProperty
        }

        val jobNames = jobProperties.stream()
                .map { it.name }
                .collect(Collectors.toSet())
        JOB_PROPERTY_MAP.forEach { (k, _) ->
            JOB_PROPERTY_MAP.remove(k + "Job")
            if (!jobNames.contains(k)) {
                scheduler.deleteJob(JobKey(k + "Job"))
            }
        }
    }

    private fun updateJobProperty(jobProperty: JobProperty, sign: String) {
        jobProperty.sign = sign
        jobPropertyMapper.updateById(jobProperty)
    }

    private fun scheduleJob(jobProperty: JobProperty) {
        val address = InetAddress.getLocalHost().hostAddress
        if (address == jobProperty.address) {
            val jobDetail = getJobDetail(jobProperty)
            if (jobProperty.enabled) {
                if (!scheduler.checkExists(jobDetail.key)) {
                    scheduler.scheduleJob(jobDetail, getTrigger(jobProperty))
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
                .withIdentity(jobProperty.beanName + "Job")
                .withDescription(jobProperty.description)
                .usingJobData(jobDataMap)
                .storeDurably()
                .build()
    }

    private fun getTrigger(jobProperty: JobProperty): Trigger? {
        if (!CronExpression.isValidExpression(jobProperty.cron)) {
            log.error("Job[{},{}] cron expression [{}] is not valid.",
                    jobProperty.name, jobProperty.description, jobProperty.cron)
        }
        return TriggerBuilder.newTrigger()
                .withIdentity(jobProperty.beanName + "Trigger")
                .withDescription(jobProperty.description)
                .withSchedule(CronScheduleBuilder.cronSchedule(jobProperty.cron))
                .build()
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(JobManager::class.java)
        private val JOB_PROPERTY_MAP = mutableMapOf<String, JobProperty>()
        private val scheduler: Scheduler = StdSchedulerFactory().scheduler
    }
}