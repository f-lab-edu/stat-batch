package ksh.statbatch.quiz.web.controller;

import ksh.statbatch.quiz.batch.launcher.JobRunner;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/batch")
@RequiredArgsConstructor
public class BatchJobController {

    private final JobRunner jobRunner;

    @PostMapping("/run")
    public String runJob(@RequestBody Map<String, String> body) {
        String aggregationDay = body.get("aggregationDay");
        jobRunner.runJob(aggregationDay);
        return "배치 성공";
    }

    @PostMapping("/run/chunk")
    public String runJobWithChunk(@RequestBody Map<String, String> body) {
        String aggregationDay = body.get("aggregationDay");
        String chunkSize = body.get("chunkSize");
        jobRunner.runJob(aggregationDay, chunkSize);
        return "배치 성공";
    }
}
