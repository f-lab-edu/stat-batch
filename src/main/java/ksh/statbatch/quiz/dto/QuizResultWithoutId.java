package ksh.statbatch.quiz.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class QuizResultWithoutId {

    private long songId;
    private boolean isCorrect;
}
