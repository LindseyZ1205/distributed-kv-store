package raft

// LogEntry is one entry in the Raft log. Data is an opaque command for the
// state machine, or empty for the no-op a new leader appends. Data is never
// modified once the entry exists, so entries can share it freely.
type LogEntry struct {
	Term int
	Data []byte
}

// raftLog hides the mapping between log indices and slice positions.
// entries[0] is a sentinel: its index is startIndex and its term is the
// term of the entry at startIndex. Without a snapshot it is the usual dummy
// entry at index 0; after compaction it stands for the last entry the
// snapshot covers, so consistency checks against that index still work.
type raftLog struct {
	startIndex int
	entries    []LogEntry
}

func makeLog(startIndex, startTerm int) raftLog {
	return raftLog{startIndex: startIndex, entries: []LogEntry{{Term: startTerm}}}
}

func (l *raftLog) lastIndex() int { return l.startIndex + len(l.entries) - 1 }

func (l *raftLog) lastTerm() int { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at index, which must be in
// [startIndex, lastIndex()].
func (l *raftLog) term(index int) int { return l.entries[index-l.startIndex].Term }

func (l *raftLog) entry(index int) LogEntry { return l.entries[index-l.startIndex] }

func (l *raftLog) append(entries ...LogEntry) { l.entries = append(l.entries, entries...) }

// from returns a copy of at most limit entries starting at index (all of
// them if limit <= 0). The copy matters: it is handed to RPCs that run
// without the lock held.
func (l *raftLog) from(index, limit int) []LogEntry {
	pos := index - l.startIndex
	end := len(l.entries)
	if limit > 0 && end-pos > limit {
		end = pos + limit
	}
	out := make([]LogEntry, end-pos)
	copy(out, l.entries[pos:end])
	return out
}

// truncateFrom discards the entries at indices >= index.
func (l *raftLog) truncateFrom(index int) { l.entries = l.entries[:index-l.startIndex] }

// compactTo discards every entry up to and including index, which becomes
// the new sentinel with the given term. Later entries are kept and copied
// to a fresh slice so the old backing array can be freed.
func (l *raftLog) compactTo(index, term int) {
	var rest []LogEntry
	if index < l.lastIndex() {
		rest = l.entries[index-l.startIndex+1:]
	}
	entries := make([]LogEntry, 1, 1+len(rest))
	entries[0] = LogEntry{Term: term}
	l.entries = append(entries, rest...)
	l.startIndex = index
}

// firstIndexOfTerm returns the smallest index <= hint holding the same
// term as the entry at hint. Followers use it for fast backup.
func (l *raftLog) firstIndexOfTerm(hint int) int {
	term := l.term(hint)
	index := hint
	for index-1 > l.startIndex && l.term(index-1) == term {
		index--
	}
	return index
}

// lastIndexOfTerm returns the largest index holding term, or -1 if there
// is none. Leaders use it for fast backup.
func (l *raftLog) lastIndexOfTerm(term int) int {
	for index := l.lastIndex(); index >= l.startIndex; index-- {
		t := l.term(index)
		if t == term {
			return index
		}
		if t < term {
			break
		}
	}
	return -1
}
