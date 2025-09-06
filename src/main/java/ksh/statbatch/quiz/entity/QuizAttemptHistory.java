package ksh.statbatch.quiz.entity;

import jakarta.persistence.*;
import ksh.statbatch.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;

@Entity
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@SQLDelete(sql = "update quiz_attempt_history set is_deleted = true where id = ?")
@Where(clause = "is_deleted = false")
public class QuizAttemptHistory extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String input;

    @Column(nullable = false, columnDefinition = "boolean default false")
    private boolean isCorrect;

    @Column(nullable = false)
    private Long quizId;

    @Column(nullable = false)
    private Long memberId;

    @Column(nullable = false)
    private Long songId;

    @Column(nullable = false, columnDefinition = "boolean default false")
    private boolean isDeleted;
}
