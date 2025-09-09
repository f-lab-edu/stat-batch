package ksh.statbatch.quiz.repository;

import ksh.statbatch.quiz.dto.QuizResult;
import ksh.statbatch.quiz.dto.QuizResultWithoutId;

import java.time.LocalDateTime;
import java.util.List;

public interface QuizAttemptHistoryQueryRepository {

    List<QuizResultWithoutId> findQuizResultByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);

    List<QuizResult> findQuizResultByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime, long lastId, int pageSize);
}
