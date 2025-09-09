package ksh.statbatch.quiz.repository;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.dto.QuizResult;
import ksh.statbatch.quiz.dto.QuizResultWithoutId;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public interface QuizAttemptHistoryQueryRepository {

    List<QuizResultWithoutId> findQuizResultByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);

    List<QuizResult> findQuizResultByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime, long lastId, int pageSize);

    List<DailySongAggregation> aggregateDailyAttemptsBySong(LocalDate baseDate, LocalDateTime startTime, LocalDateTime endTime);

    List<DailySongAggregation> aggregateDailyAttemptsBySong(LocalDate baseDate, LocalDateTime startTime, LocalDateTime endTime, long lastSongId, int pageSize);
}
