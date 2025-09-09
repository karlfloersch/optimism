package activities

import "context"

type Activity interface {
	StartActivity(ctx context.Context) error
	StopActivity(ctx context.Context) error
}
