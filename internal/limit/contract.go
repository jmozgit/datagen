package limit

import (
	"context"

	"github.com/jmozgit/datagen/internal/model"
)

type Collector interface {
	Collect(ctx context.Context, progress model.ProgressState)
}
