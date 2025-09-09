package ksh.statbatch.quiz.repository;

import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.dto.QuizResult;
import ksh.statbatch.quiz.dto.QuizResultWithoutId;
import lombok.RequiredArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static ksh.statbatch.quiz.entity.QQuizAttemptHistory.quizAttemptHistory;

@RequiredArgsConstructor
public class QuizAttemptHistoryQueryRepositoryImpl implements QuizAttemptHistoryQueryRepository {

    private final JPAQueryFactory queryFactory;

    @Override
    public List<QuizResultWithoutId> findQuizResultByCreatedAtBetween(
        LocalDateTime startTime,
        LocalDateTime endTime
    ) {
        var qah = quizAttemptHistory;

        return queryFactory
            .select(Projections.constructor(
                QuizResultWithoutId.class,
                qah.songId,
                qah.isCorrect
            ))
            .from(qah)
            .where(
                qah.createdAt.goe(startTime),
                qah.createdAt.lt(endTime),
                qah.isDeleted.isFalse()
            )
            .fetch();
    }

    @Override
    public List<QuizResult> findQuizResultByCreatedAtBetween(
        LocalDateTime startTime,
        LocalDateTime endTime,
        long lastId,
        int pageSize
    ) {
        var qah = quizAttemptHistory;

        return queryFactory
            .select(Projections.constructor(
                QuizResult.class,
                qah.id,
                qah.songId,
                qah.isCorrect
            ))
            .from(qah)
            .where(
                qah.createdAt.goe(startTime),
                qah.createdAt.lt(endTime),
                qah.isDeleted.isFalse(),
                qah.id.gt(lastId)
            )
            .orderBy(qah.id.asc())
            .limit(pageSize)
            .fetch();
    }

    @Override
    public List<DailySongAggregation> aggregateDailyAttemptsBySong(
        LocalDate baseDate,
        LocalDateTime startTime,
        LocalDateTime endTime
    ) {
        var qah = quizAttemptHistory;

        var wrongIncExpr = new CaseBuilder()
                .when(qah.isCorrect.isFalse()).then(1L)
                .otherwise(0L);

        return queryFactory
            .select(Projections.constructor(
                DailySongAggregation.class,
                Expressions.constant(baseDate),
                qah.songId,
                wrongIncExpr.sum(),
                qah.songId.count()
            ))
            .from(qah)
            .where(
                qah.createdAt.goe(startTime),
                qah.createdAt.lt(endTime),
                qah.isDeleted.isFalse()
            )
            .groupBy(qah.songId)
            .fetch();
    }

    @Override
    public List<DailySongAggregation> aggregateDailyAttemptsBySong(
        LocalDate baseDate,
        LocalDateTime startTime,
        LocalDateTime endTime,
        long lastSongId,
        int pageSize
    ) {
        var qah = quizAttemptHistory;

        var wrongIncExpr = new CaseBuilder()
            .when(qah.isCorrect.isFalse()).then(1L)
            .otherwise(0L);

        return queryFactory
            .select(Projections.constructor(
                DailySongAggregation.class,
                Expressions.constant(baseDate),
                qah.songId,
                wrongIncExpr.sum(),
                qah.songId.count()
            ))
            .from(qah)
            .where(
                qah.createdAt.goe(startTime),
                qah.createdAt.lt(endTime),
                qah.songId.gt(lastSongId),
                qah.isDeleted.isFalse()
            )
            .groupBy(qah.songId)
            .orderBy(qah.songId.asc())
            .limit(pageSize)
            .fetch();
    }
}
