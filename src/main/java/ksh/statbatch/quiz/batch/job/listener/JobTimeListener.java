package ksh.statbatch.quiz.batch.job.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JobTimeListener implements JobExecutionListener {
    private static final String TIME_KEY = "time";

    @Override
    public void beforeJob(JobExecution jobExecution) {
        jobExecution.getExecutionContext().putLong(TIME_KEY, System.nanoTime());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long start = jobExecution.getExecutionContext().getLong(TIME_KEY);
        long end = System.nanoTime();
        long elapsedTime = (end - start) / 1_000_000;

        log.info(
            "배치 잡 '{}' 실행 완료: {} ms (상태={})",
            jobExecution.getJobInstance().getJobName(),
            elapsedTime,
            jobExecution.getStatus()
        );
    }
}
