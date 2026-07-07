/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 */

package effects

// inMajorityPartition reports whether this node can trust its cluster view.
// Standalone (no broadcaster) is the whole cluster.
func (e *Engine) inMajorityPartition() bool {
	if e.broadcaster == nil {
		return true
	}
	return e.broadcaster.InMajorityPartition()
}

// clusterMaybeHasKey no longer uses advisory key filters. Without a
// DAG-derived proof of absence, every read miss must take the real
// subscription/bootstrap path.
func (e *Engine) clusterMaybeHasKey(key string) bool { return true }

// fetchHint orders unknown fetches through peers first. CDN remains a fallback
// in FetchFromAny; advisory filters no longer choose a preferred source.
func (e *Engine) fetchHint(key string) FetchHint { return PreferPeers }
