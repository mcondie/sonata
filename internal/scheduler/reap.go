package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"syscall"
	"time"

	"github.com/mcondie/sonata/internal/store"
)

// ReapOrphans handles the aftermath of a daemon that died without stopping
// its tasks: every delivery still claimed belongs to a process group nobody
// is watching. Kill the group (ESRCH means it died with the daemon) and
// return the delivery to the retry path. Runs at startup, before serving —
// this is why pgid is persisted at claim time.
func ReapOrphans(ctx context.Context, st *store.Store, log *slog.Logger) error {
	orphans, err := st.ClaimedDeliveries(ctx)
	if err != nil {
		return fmt.Errorf("list orphaned deliveries: %w", err)
	}
	for _, d := range orphans {
		if d.Pgid != nil {
			// The previous daemon is dead, so the group cannot be cooperating
			// with a graceful stop; SIGKILL, not SIGTERM.
			if err := syscall.Kill(-*d.Pgid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
				log.Warn("kill orphan process group", "delivery_id", d.ID,
					"pgid", *d.Pgid, "error", err)
			}
		}
		if err := st.ResetOrphan(ctx, d.ID, time.Now()); err != nil {
			return fmt.Errorf("reset orphan %s: %w", d.ID, err)
		}
		log.Info("reaped orphaned delivery", "delivery_id", d.ID,
			"action", d.ActionName)
	}
	return nil
}
