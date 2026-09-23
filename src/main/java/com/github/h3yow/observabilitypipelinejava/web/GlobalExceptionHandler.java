package com.github.h3yow.observabilitypipelinejava.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 兜底异常处理：任何未预期输入都返回可区分的结构化错误，不抛栈导致 500。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArg(IllegalArgumentException e) {
        log.warn("bad request: {}", e.getMessage());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "REJECTED");
        body.put("reason", "BAD_REQUEST");
        body.put("detail", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleOther(Exception e) {
        log.error("unexpected error", e);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "REJECTED");
        body.put("reason", "INTERNAL_ERROR");
        body.put("detail", String.valueOf(e.getMessage()));
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }
}
