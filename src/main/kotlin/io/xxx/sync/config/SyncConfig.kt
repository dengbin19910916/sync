package io.xxx.sync.config

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
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
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.stream.Collectors

@Slf4j
@Configuration
class SyncConfig : ApplicationRunner {

    @Autowired
    private lateinit var propertyMapper: SyncPropertyMapper

    @Synchronized
    override fun run(args: ApplicationArguments) {
        val wrapper = QueryWrapper<SyncProperty>()
        wrapper.eq("enabled", true)
        val properties = propertyMapper.selectList(wrapper)
        for (property in properties) {
            val clazz = property.getBeanClass()
            val beanName = property.getBeanName(clazz)
            val builder = BeanDefinitionBuilder.genericBeanDefinition(clazz)
                    .addConstructorArgValue(property)
            if (!applicationContext.isBeanNameInUse(beanName)) {
                applicationContext.registerBeanDefinition(beanName, builder.beanDefinition)
            }
        }

        if (log.isInfoEnabled) {
            log.info("Synchronizer init completed.")
        }
    }

    companion object : ApplicationContextAware {

        val log: Logger = LoggerFactory.getLogger(SyncConfig::class.java)
        private lateinit var applicationContext: GenericApplicationContext

        override fun setApplicationContext(applicationContext: ApplicationContext) {
            this.applicationContext = applicationContext as GenericApplicationContext
        }
    }
}

fun SyncProperty.getBeanClass(): Class<*> {
    return Class.forName(this.beanClass)
}

fun SyncProperty.getBeanName(clazz: Class<*>): String {
    return if (this.beanName == null)
        clazz.simpleName.decapitalize() + this.shopCode
    else this.beanName!!
}

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
class ProxyJob : Job {

    override fun execute(context: JobExecutionContext) {
        val property = context.mergedJobDataMap["property"] as SyncProperty
        val applicationContext = context.mergedJobDataMap["applicationContext"] as ApplicationContext
        val clazz = property.getBeanClass()
        val beanName = property.getBeanName(clazz)
        val synchronizer = applicationContext.getBean(beanName, Job::class.java)
        synchronizer.execute(context)
    }
}

@Component
class JobManager {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(JobManager::class.java)
        private val JOB_PROPERTY_MAP = mutableMapOf<String, JobProperty>()
        private val scheduler: Scheduler = StdSchedulerFactory().scheduler

        @Autowired
        private lateinit var jobPropertyMapper: JobPropertyMapper

        @Autowired
        private lateinit var applicationContext: ApplicationContext
    }

    init {
        scheduler.start()
    }

    @Scheduled(cron = "*/5 * * * * ?")
    fun loadJobs() {
        val jobProperties = jobPropertyMapper.selectList(null)
        if (jobProperties.isEmpty()) {
            return
        }

        for (jobProperty in jobProperties) {
            val sign: String = jobProperty.getSign()
            if (ObjectUtils.isEmpty(jobProperty.sign)) {
                updateJobProperty(jobProperty, sign)
                scheduleJob(jobProperty)
            } else {
                if (!JOB_PROPERTY_MAP.containsKey(jobProperty.name)) {
                    if (ObjectUtils.isEmpty(jobProperty.sign) || !jobProperty.sign.equals(sign)) {
                        updateJobProperty(jobProperty, sign)
                        scheduleJob(jobProperty)
                    }
                } else {
                    if (jobProperty.sign != JOB_PROPERTY_MAP[jobProperty.name]?.sign) {
                        updateJobProperty(jobProperty, sign)
                        scheduler.deleteJob(JobKey(jobProperty.beanName + "Job"))
                        scheduleJob(jobProperty)
                    }
                }
            }
            JOB_PROPERTY_MAP[jobProperty.name] = jobProperty
        }

        val jobNames = jobProperties.stream()
                .map { it.name }
                .collect(Collectors.toSet())
        JOB_PROPERTY_MAP.forEach { (k, _) ->
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
        val jobDetail = getJobDetail(jobProperty)
        if (jobProperty.enabled) {
            if (!scheduler.checkExists(jobDetail.key)) {
                scheduler.scheduleJob(jobDetail, getTrigger(jobProperty))
            }
        } else {
            scheduler.deleteJob(jobDetail.key)
        }
    }

    @Throws(NoSuchAlgorithmException::class)
    private fun JobProperty.getSign(): String {
        val str = this::class.java.declaredFields
                .filter { it.name != "sign" }
                .sortedBy { it.name }
                .map { it.isAccessible = true;it.name + it.get(this) }
                .joinToString { it }

        val md5Instance = MessageDigest.getInstance("MD5")
        md5Instance.update(str.toByteArray(StandardCharsets.UTF_8))
        val digest = md5Instance.digest()
        return byte2Hex(digest)
    }

    private fun byte2Hex(bytes: ByteArray): String {
        val hexDigits = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
        val j = bytes.size
        val str = CharArray(j * 2)
        var k = 0
        for (byte0 in bytes) {
            str[k++] = hexDigits[byte0.toInt() ushr (4) and (0xf)]
            str[k++] = hexDigits[byte0.toInt() and (0xf)]
        }
        return String(str)
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
}