package io.xxx.sync.web

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.client.RestTemplate

@RestController
@RequestMapping("/test")
class TestController {

    @Autowired
    private lateinit var restTemplateBuilder: RestTemplateBuilder

    fun test() {
        restTemplateBuilder.build()
    }
}