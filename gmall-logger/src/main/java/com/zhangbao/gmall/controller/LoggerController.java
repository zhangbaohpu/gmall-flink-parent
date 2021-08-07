package com.zhangbao.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhangbao
 * @date 2021/5/16 11:33
 **/
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;
    @RequestMapping("/applog")
    public String logger(String param){
        log.info(param);
        kafkaTemplate.send("ods_base_log",param);
        return param;
    }

}
