package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.entity.QuizAttemptHistory;
import ksh.statbatch.quiz.entity.WrongQuizMonthlyStat;
import ksh.statbatch.quiz.repository.QuizAttemptHistoryRepository;
import ksh.statbatch.quiz.repository.WrongQuizMonthlyStatRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@SpringBatchTest
@SpringBootTest
class DailyAggregationInMemoryReaderTest {

    @Autowired
    JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    QuizAttemptHistoryRepository quizAttemptHistoryRepository;

    @Autowired
    WrongQuizMonthlyStatRepository wrongQuizMonthlyStatRepository;

    @AfterEach
    void tearDown() {
        quizAttemptHistoryRepository.deleteAllInBatch();
        wrongQuizMonthlyStatRepository.deleteAllInBatch();
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @DisplayName("일간 퀴즈 풀이 기록을 한번에 모두 읽어 집계한 후 월별 통계를 업데이트한다")
    @Test
    void test() throws Exception {
        //given
        QuizAttemptHistory history1OfSong1 = createQuizAttemptHistory(true, 1L, 1L, 1L);
        QuizAttemptHistory history2OfSong1 = createQuizAttemptHistory(false, 1L, 1L, 2L);
        QuizAttemptHistory history3OfSong1 = createQuizAttemptHistory(false, 2L, 1L, 1L);

        QuizAttemptHistory history1OfSong2 = createQuizAttemptHistory(false, 4L, 2L, 4L);
        QuizAttemptHistory history2OfSong2 = createQuizAttemptHistory(false, 1L, 2L, 5L);

        quizAttemptHistoryRepository.saveAll(List.of(history1OfSong1, history2OfSong1, history3OfSong1, history1OfSong2, history2OfSong2));

        JobParameters baseDateParam = new JobParametersBuilder()
            .addString("aggregationDay", LocalDate.now().toString())
            .toJobParameters();

        //when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(baseDateParam);

        //then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        LocalDate baseDate = LocalDate.now().withDayOfMonth(1);
        List<WrongQuizMonthlyStat> stats = wrongQuizMonthlyStatRepository.findByBaseDate(baseDate);
        assertThat(stats).hasSize(2)
            .extracting("songId", "wrongCount", "totalTries", "wrongRate")
            .containsExactlyInAnyOrder(
                tuple(1L, 2L, 3L, 0.6667),
                tuple(2L, 2L, 2L, 1.0)
            );
    }

    private QuizAttemptHistory createQuizAttemptHistory(
        boolean isCorrect,
        long memberId,
        long songId,
        long quizId
    ) {
        return QuizAttemptHistory.builder()
            .input("인풋")
            .isCorrect(isCorrect)
            .quizId(quizId)
            .memberId(memberId)
            .songId(songId)
            .build();
    }
}
