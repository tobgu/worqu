package tasks

import "fmt"

type TaskNotFoundError struct {
	message string
}

func (e TaskNotFoundError) Error() string {
	return e.message
}

func NewTaskNotFoundError(format string, a ...any) TaskNotFoundError {
	return TaskNotFoundError{
		message: fmt.Sprintf(format, a...),
	}
}

type InputError struct {
	message string
}

func (e InputError) Error() string {
	return e.message
}

func NewInputError(format string, a ...any) InputError {
	return InputError{
		message: fmt.Sprintf(format, a...),
	}
}
