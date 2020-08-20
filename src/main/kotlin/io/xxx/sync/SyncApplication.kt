package io.xxx.sync

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class SyncApplication

fun main(args: Array<String>) {
    runApplication<SyncApplication>(*args)
}
