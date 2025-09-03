package ksh.statbatch.quiz.batch.controller;

import ksh.statbatch.quiz.batch.job.launcher.JobRunner;
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
}
