package com.github.h3yow.observabilitypipelinejava.web;

import java.util.List;

public record BatchIngestRequest(List<MetricSampleRequest> samples) {
}
