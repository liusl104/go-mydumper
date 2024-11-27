package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"math"
	"strings"
	"sync"
	"time"
)

func initialize_char_chunk(o *OptionEntries) {
	if o.global.starting_chunk_step_size > 0 {
		if o.Chunks.CharChunk == 0 {
			o.Chunks.CharChunk = o.Common.NumThreads
		}
		if o.Chunks.CharDeep == 0 {
			o.Chunks.CharDeep = o.Common.NumThreads
		}
	}
}

func print_hex(buffer string, size uint) string {
	var str strings.Builder
	var i uint
	for i = 0; i < size; i++ {
		str.WriteString(fmt.Sprintf("%02x", buffer[i]))
	}
	return str.String()
}

func new_char_step(o *OptionEntries, conn *client.Conn, row []mysql.FieldValue, lengths []*mysql.Field, mutex *sync.Mutex) *chunk_step {
	var cs = new(chunk_step)
	_ = conn
	cs.char_step = new(char_step)
	cs.char_step.step = o.global.starting_chunk_step_size
	cs.char_step.mutex = mutex

	cs.char_step.cmin_len = uint(lengths[0].ColumnLength) + 1 // lengths[2]
	cs.char_step.cmax_len = uint(lengths[1].ColumnLength) + 1 // lengths[3]

	cs.char_step.cmin_clen = uint(lengths[2].ColumnLength) // lengths[0] + 1
	cs.char_step.cmax_clen = uint(lengths[3].ColumnLength) // lengths[1] + 1

	cs.char_step.cmin = string(row[0].AsString()) // cs.char_step.cmin_len
	cs.char_step.cmin_escaped = mysql.Escape(string(row[0].AsString()))

	cs.char_step.cmax = string(row[1].AsString()) // g_new(char, cs.char_step.cmax_len)
	cs.char_step.cmax_escaped = mysql.Escape(string(row[1].AsString()))

	cs.char_step.previous = nil
	cs.char_step.estimated_remaining_steps = 1
	cs.char_step.status = 0
	return cs
}

func new_char_step_item(o *OptionEntries, conn *client.Conn, include_null bool, prefix string, field string, deep uint, number uint, row []mysql.FieldValue, lengths []*mysql.Field, next *chunk_step_item) *chunk_step_item {
	var csi = new(chunk_step_item)
	csi.number = uint64(number)
	csi.field = field
	csi.mutex = g_mutex_new()
	csi.chunk_step = new_char_step(o, conn, row, lengths, csi.mutex)
	csi.chunk_type = CHAR
	csi.chunk_functions.process = process_char_chunk
	csi.chunk_functions.get_next = get_next_char_chunk
	csi.next = next
	csi.field = field
	csi.status = UNASSIGNED
	csi.include_null = include_null
	csi.prefix = prefix
	csi.where = ""
	return csi
}

func next_chunk_in_char_step(cs *chunk_step) {
	cs.char_step.cmin_clen = cs.char_step.cursor_clen
	cs.char_step.cmin_len = cs.char_step.cursor_len
	cs.char_step.cmin = cs.char_step.cursor
	cs.char_step.cmin_escaped = cs.char_step.cursor_escaped
}

func split_char_step(o *OptionEntries, deep uint, number uint, previous_csi *chunk_step_item) *chunk_step_item {
	var csi = new(chunk_step_item)
	var cs = new(chunk_step)
	csi.prefix = csi.where
	csi.status = ASSIGNED
	cs.char_step.deep = deep
	csi.mutex = g_mutex_new()
	csi.number = uint64(number)
	cs.char_step.step = o.global.starting_chunk_step_size
	csi.field = previous_csi.field
	cs.char_step.previous = nil
	cs.char_step.status = 0
	csi.chunk_step = cs
	csi.chunk_type = CHAR
	csi.chunk_functions.process = process_char_chunk
	csi.where = ""
	csi.chunk_functions.get_next = get_next_char_chunk
	return csi
}

func free_char_step(cs *chunk_step) {
	cs = nil
}

func get_next_char_chunk(o *OptionEntries, dbt *db_table) *chunk_step_item {
	var csi *chunk_step_item
	for e := dbt.chunks.Front(); e != nil; e = e.Next() {
		csi = e.Value.(*chunk_step_item)
		if csi.mutex == nil {
			log.Infof("This should not happen")
			continue
		}

		csi.mutex.Lock()
		if csi.status == UNASSIGNED {
			csi.status = ASSIGNED
			csi.mutex.Unlock()
			return csi
		}

		if csi.chunk_step.char_step.deep <= o.Chunks.CharDeep && strings.Compare(csi.chunk_step.char_step.cmax, csi.chunk_step.char_step.cursor) != 0 && csi.chunk_step.char_step.status == 0 {
			var new_cs = split_char_step(o, csi.chunk_step.char_step.deep+1, uint(csi.number)+uint(math.Pow(2, float64(csi.deep))), csi)
			csi.chunk_step.char_step.deep++
			csi.chunk_step.char_step.status = 1
			new_cs.status = ASSIGNED
			csi.mutex.Unlock()
			return new_cs
		} else {
			csi.mutex.Unlock()
			//      g_message("Not able to split because %d > %d | %s == %s | %d != 0", cs.char_step.deep,num_threads, cs.char_step.cmax, cs.char_step.cursor, cs.char_step.status);
		}

	}
	return nil
}

func get_escaped_middle_char(conn *client.Conn, c1 []byte, c1len uint, c2 []byte, c2len uint, part uint) string {
	/*fmt.Printf("c1 <-- %s c1len %d\n", c1, c1len)
	fmt.Printf("c1 <-- %s c1len %d\n", c2, c2len)*/
	var cresultlen uint
	_ = conn
	if c1len < c2len {
		cresultlen = c1len
	} else {
		cresultlen = c2len
	}
	var cresult []byte = make([]byte, cresultlen)
	var i uint
	var cu1 byte = c1[0]
	var cu2 byte = c2[0]
	for i = 0; i < cresultlen && c1[i] == c2[i]; i++ {
		cresult[i] = c1[i]
	}
	cu1 = c1[i]
	cu2 = c2[i]
	if i < cresultlen && cu2 != cu1 {
		if cu2 > cu1 {
			// 58
			cresult[i] = cu1 + uint8(int(math.Abs(float64(cu2)-float64(cu1)))/int(part))
		} else {
			// 48 + abs(48 - 90) / 2 = 155 / 58
			cresult[i] = cu2 + uint8(int(math.Abs(float64(cu2)-float64(cu1)))/int(part))
		}
	}
	cu1 = c1[0]
	cu2 = c2[0]
	// cresult[cresultlen] = 0
	escapedresult := mysql.Escape(string(cresult))
	return escapedresult
}

func update_cursor(o *OptionEntries, conn *client.Conn, csi *chunk_step_item, dbt *db_table, tj *table_job) string {
	var query, cache string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var char_chunk_part uint
	if tj.char_chunk_part > 0 {
		char_chunk_part = tj.char_chunk_part
	} else {
		char_chunk_part = 1
	}
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var middle = get_escaped_middle_char(conn, []byte(csi.chunk_step.char_step.cmax), csi.chunk_step.char_step.cmax_clen, []byte(csi.chunk_step.char_step.cmin),
		csi.chunk_step.char_step.cmin_clen, char_chunk_part)
	query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE '%s' <= `%s` AND '%s' <= `%s` AND `%s` <= '%s' ORDER BY `%s` LIMIT 1", cache,
		csi.field, dbt.database.name, dbt.table, csi.chunk_step.char_step.cmin_escaped, csi.field, middle, csi.field, csi.field,
		csi.chunk_step.char_step.cmax_escaped, csi.field)
	minmax, _ = conn.Execute(query)
	if len(minmax.Values) == 0 {
		csi.chunk_step.char_step.cursor_clen = csi.chunk_step.char_step.cmax_clen
		csi.chunk_step.char_step.cursor_len = csi.chunk_step.char_step.cmax_len
		csi.chunk_step.char_step.cursor = csi.chunk_step.char_step.cmax
		csi.chunk_step.char_step.cursor_escaped = csi.chunk_step.char_step.cmax_escaped
		return ""
	}
	var lengths = minmax.Values[0]

	tj.char_chunk_part--
	if strings.Compare(string(row[0].AsString()), csi.chunk_step.char_step.cmax) != 0 && strings.Compare(string(row[0].AsString()), csi.chunk_step.char_step.cmin) != 0 {
		csi.chunk_step.char_step.cursor_clen = uint(len(lengths[0].AsString()))
		csi.chunk_step.char_step.cursor_len = uint(len(lengths[0].AsString())) + 1
		csi.chunk_step.char_step.cursor = string(row[0].AsString())
		csi.chunk_step.char_step.cursor_escaped = mysql.Escape(string(row[0].AsString()))
	} else {
		csi.chunk_step.char_step.cursor_clen = csi.chunk_step.char_step.cmax_clen
		csi.chunk_step.char_step.cursor_len = csi.chunk_step.char_step.cmax_len
		csi.chunk_step.char_step.cursor = csi.chunk_step.char_step.cmax
		csi.chunk_step.char_step.cursor_escaped = csi.chunk_step.char_step.cmax_escaped
	}
	return ""
}

func get_new_minmax(o *OptionEntries, td *thread_data, dbt *db_table, csi *chunk_step_item) bool {
	var query, cache, cursor, escaped string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var err error
	var previous = csi.chunk_step.char_step.previous
	var cursor_len uint
	// previous->char_step.cursor != NULL ? previous->char_step.cursor: previous->char_step.cmin,
	// previous->char_step.cursor != NULL ?previous->char_step.cursor_len:previous->char_step.cmin_clen
	if previous.char_step.cursor != "" {
		cursor = previous.char_step.cursor
		cursor_len = previous.char_step.cursor_len
	} else {
		cursor = previous.char_step.cmin
		cursor_len = previous.char_step.cmin_clen
	}
	log.Infof("get_new_minmax::")
	previous.char_step.mutex.Lock()
	var middle = get_escaped_middle_char(td.thrconn, []byte(previous.char_step.cmax), previous.char_step.cmax_clen, []byte(cursor), cursor_len, o.Chunks.CharChunk)
	_ = cursor_len
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}

	if previous.char_step.cursor_escaped != "" {
		escaped = previous.char_step.cursor_escaped
	} else {
		escaped = previous.char_step.cmin_escaped
	}
	// previous.char_step.cursor_escaped!=NULL?previous.char_step.cursor_escaped:previous.char_step.cmin_escaped

	query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE `%s` > (SELECT `%s` FROM `%s`.`%s` WHERE `%s` > '%s' ORDER BY `%s` LIMIT 1) AND '%s' < `%s` AND `%s` < '%s' ORDER BY `%s` LIMIT 1",
		cache, csi.field, dbt.database.name, dbt.table, dbt.primary_key, dbt.primary_key, dbt.database.name,
		dbt.table, dbt.primary_key, middle, dbt.primary_key, escaped, dbt.primary_key, dbt.primary_key, previous.char_step.cmax_escaped, dbt.primary_key)
	minmax, err = td.thrconn.Execute(query)
	if err != nil {
		log.Infof("No middle point")
		previous.char_step.mutex.Unlock()
		return false

	}
	if len(minmax.Values) == 0 {
		log.Infof("No middle point")
		previous.char_step.mutex.Unlock()
		return false
	}

	row = minmax.Values[0]
	// minmax.Fields[0].ColumnLength
	var lengths = row

	csi.chunk_step.char_step.cmax_clen = previous.char_step.cmax_clen
	csi.chunk_step.char_step.cmax_len = previous.char_step.cmax_len
	csi.chunk_step.char_step.cmax = previous.char_step.cmax
	csi.chunk_step.char_step.cmax_escaped = previous.char_step.cmax_escaped
	previous.char_step.cmax_clen = uint(len(lengths[0].AsString()))
	previous.char_step.cmax_len = uint(len(lengths[0].AsString())) + 1
	previous.char_step.cmax = string(row[0].AsString())
	previous.char_step.cmax_escaped = mysql.Escape(string(row[0].AsString()))
	previous.char_step.status = 0
	previous.char_step.mutex.Unlock()
	csi.chunk_step.char_step.cmin_clen = uint(len(lengths[0].AsString()))
	csi.chunk_step.char_step.cmin_len = uint(len(lengths[0].AsString())) + 1
	csi.chunk_step.char_step.cmin = string(row[0].AsString())
	csi.chunk_step.char_step.cmin_escaped = mysql.Escape(string(row[0].AsString()))
	return true
}

func process_char_chunk_step(o *OptionEntries, td *thread_data, tj *table_job, csi *chunk_step_item) bool {
	check_pause_resume(td)
	if o.global.shutdown_triggered {
		return true
	}
	csi.mutex.Lock()
	if csi.chunk_step.char_step.cmax != "" {
		update_cursor(o, td.thrconn, tj.chunk_step_item, tj.dbt, tj)
	}
	csi.mutex.Unlock()
	update_where_on_char_step(csi)
	if csi.next != nil {
		csi.next.chunk_functions.process(o, tj, csi.next)
	} else {
		tj.where = ""
		tj.where = csi.where
		var from = time.Now()
		write_table_job_into_file(o, tj)
		var to = time.Now()
		diff := to.Sub(from).Seconds()
		if diff > 2 {
			csi.chunk_step.char_step.step = csi.chunk_step.char_step.step / 2
			if csi.chunk_step.char_step.step < o.global.min_chunk_step_size {
				csi.chunk_step.char_step.step = o.global.min_chunk_step_size
			}
		} else if diff < 1 {
			csi.chunk_step.char_step.step = csi.chunk_step.char_step.step * 2
			if o.global.max_chunk_step_size != 0 {
				if csi.chunk_step.char_step.step > o.global.max_chunk_step_size {
					csi.chunk_step.char_step.step = o.global.max_chunk_step_size
				}
			}
		}
	}
	csi.mutex.Lock()
	next_chunk_in_char_step(csi.chunk_step)
	csi.mutex.Unlock()
	return false
}

func process_char_chunk(o *OptionEntries, tj *table_job, csi *chunk_step_item) {
	var td = tj.td
	var dbt = tj.dbt
	var cs = csi.chunk_step
	var previous = cs.char_step.previous
	var cont bool = false
	for cs.char_step.previous != nil || cs.char_step.cmax == cs.char_step.cursor {
		if cs.char_step.previous != nil {
			csi.mutex.Lock()
			cont = get_new_minmax(o, td, tj.dbt, csi)
			csi.mutex.Unlock()
			if cont == true {
				cs.char_step.previous = nil
				dbt.chunks_mutex.Lock()
				dbt.chunks.PushBack(cs)
				dbt.chunks_mutex.Unlock()
			} else {
				dbt.chunks_mutex.Lock()
				previous.char_step.status = 0
				dbt.chunks_mutex.Unlock()
				// previous.char_step.mutex.Unlock()
				return
			}
		} else {
			if strings.Compare(cs.char_step.cmax, cs.char_step.cursor) != 0 {
				if process_char_chunk_step(o, td, tj, csi) {
					log.Infof("Thread %d: Job has been cacelled", td.thread_id)
					return
				}
			} else {
				csi.mutex.Lock()
				cs.char_step.status = 2
				csi.mutex.Unlock()
				break
			}
		}
	}
	if strings.Compare(cs.char_step.cursor, cs.char_step.cmin) != 0 {
		if process_char_chunk_step(o, td, tj, csi) {
			log.Infof("Thread %d: Job has been cacelled", td.thread_id)
			return
		}
	}
	dbt.chunks_mutex.Lock()
	csi.mutex.Lock()
	csi.mutex.Unlock()
	dbt.chunks_mutex.Unlock()
}

func update_where_on_char_step(csi *chunk_step_item) {
	csi.where = ""
	var has_prefix bool = len(csi.prefix) > 0
	if has_prefix {
		csi.where += fmt.Sprintf("(%s AND ", csi.prefix)
	}
	csi.where += "("
	if csi.include_null {
		csi.where += fmt.Sprintf("`%s` IS NULL OR", csi.field)
	}
	var cs = csi.chunk_step.char_step
	if cs.cmax == "" {
		csi.where += fmt.Sprintf("`%s` >= '%s'", csi.field, cs.cmin_escaped)
	} else {
		var e string
		if cs.cursor == cs.cmax {
			e = "="
		}
		csi.where += fmt.Sprintf("'%s' <= `%s` AND `%s` <%s '%s'", cs.cmin_escaped, csi.field, csi.field, e, cs.cursor_escaped)
	}
	if has_prefix {
		csi.where += ")"
	}
	csi.where += ")"

}
