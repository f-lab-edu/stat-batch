package ksh.statbatch.quiz.batch.launcher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRunner {

    private final JobLauncher jobLauncher;
    private final Job job;

    public void runJob(String aggregationDay) {
        try {
            JobParameters params = new JobParametersBuilder()
                .addString("aggregationDay", aggregationDay, true)
                .addString("runId", System.currentTimeMillis() + "", true)
                .toJobParameters();

            log.info("배치 잡 시작, 집계 날짜: {}", aggregationDay);
            JobExecution execution = jobLauncher.run(job, params);
            log.info("배치 잡 실행 ID: {}, 상태: {}", execution.getId(), execution.getStatus());

        } catch (Exception e) {
            log.error("배치 잡 실행 실패", e);
        }
    }


    public void runJob(String aggregationDay, String chunkSize) {
        try {
            JobParameters params = new JobParametersBuilder()
                .addString("aggregationDay", aggregationDay, true)
                .addString("chunkSize", chunkSize, true)
                .addString("runId", System.currentTimeMillis() + "", true)
                .toJobParameters();

            log.info("배치 잡 시작, 집계 날짜: {}", aggregationDay);
            JobExecution execution = jobLauncher.run(job, params);
            log.info("배치 잡 실행 ID: {}, 상태: {}", execution.getId(), execution.getStatus());

        } catch (Exception e) {
            log.error("배치 잡 실행 실패", e);
        }
    }
}
