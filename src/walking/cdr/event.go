package cdr

type Event interface {
	NewEvent(line []byte, pathFile string, linCnt int) (evt *Event, row4err string)
	/**
	if input is over then evt is nil
	if no output then evts is nil
	 */
	// Reduce(evt *Event) (evts map[string][]*Event) // reflect.Value.Type().String() // reflect.ValueOf(evt)
	Reduce(evt *Event) (sel *Event, grp map[string]*Event)
	ToDsv() (data string)
	Sql() (ddl, load string)
}
