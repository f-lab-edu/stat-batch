package ksh.statbatch.quiz.repository;

import ksh.statbatch.quiz.entity.QuizAttemptHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface QuizAttemptHistoryRepository extends JpaRepository<QuizAttemptHistory, Long>, QuizAttemptHistoryQueryRepository {
}
