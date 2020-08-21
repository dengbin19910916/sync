package io.xxx.sync.config

import io.ebean.Database
import io.ebean.DatabaseFactory
import io.ebean.config.DatabaseConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
class EbeanConfig {

    @Bean
    fun database(): Database {
        val cfg = DatabaseConfig()

        val properties = Properties()
        properties["ebean.db.ddl.generate"] = "false"
        properties["ebean.db.ddl.run"] = "false"

        properties["datasource.db.username"] = "root"
        properties["datasource.db.password"] = "12345678"
        properties["datasource.db.databaseUrl"] = "jdbc:mysql://localhost:3306/tradedb?characterEncoding=utf-8&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai"
        properties["datasource.db.databaseDriver"] = "com.mysql.cj.jdbc.Driver"

        cfg.loadFromProperties(properties)
        return DatabaseFactory.create(cfg)
    }
}