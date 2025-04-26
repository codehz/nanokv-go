package helper

import (
	"context"
	"fmt"
)

func RecoverCancelCause(cancel context.CancelCauseFunc, actions ...func()) {
	if e := recover(); e != nil {
		if err, ok := e.(error); ok {
			cancel(fmt.Errorf("recovered error %w", err))
		} else {
			cancel(fmt.Errorf("recovered panic %v", e))
		}
		for _, action := range actions {
			action()
		}
	}
}
