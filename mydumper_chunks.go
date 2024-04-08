package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

func initialize_chunk(o *OptionEntries) {
	o.global.give_me_another_innodb_chunk_step_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.give_me_another_non_innodb_chunk_step_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)

	if o.global.rows_per_file > 0 {
		if o.Chunks.CharChunk == 0 {
			o.Chunks.CharChunk = o.Common.NumThreads
		}
		if o.Chunks.CharDeep == 0 {
			o.Chunks.CharDeep = o.Common.NumThreads
		}
	}
}

func finalize_chunk(o *OptionEntries) {
	o.global.give_me_another_innodb_chunk_step_queue.unref()
	o.global.give_me_another_non_innodb_chunk_step_queue.unref()
}

func gint64_abs(a int64) int64 {
	if a > 0 {
		return a
	}
	return -a
}

func new_char_step(o *OptionEntries, conn *client.Conn, field string, deep uint, number uint, row []mysql.FieldValue, lengths []mysql.FieldValue) *chunk_step {
	var cs = new(chunk_step)
	_ = conn
	cs.char_step = new(char_step)
	cs.char_step.step = o.global.rows_per_file
	cs.char_step.cmin_clen = uint(len(lengths[2].AsString()))    // lengths[2]
	cs.char_step.cmin_len = uint(len(lengths[0].AsString())) + 1 // lengths[0] + 1
	cs.char_step.cmin = string(row[0].AsString())                // cs.char_step.cmin_len
	cs.char_step.cmin_escaped = mysql.Escape(string(row[0].AsString()))
	cs.char_step.cmax_clen = uint(len(lengths[3].AsString()))    // lengths[3]
	cs.char_step.cmax_len = uint(len(lengths[1].AsString())) + 1 // lengths[1] + 1
	cs.char_step.cmax = string(row[1].AsString())                // g_new(char, cs.char_step.cmax_len)
	cs.char_step.cmax_escaped = mysql.Escape(string(row[1].AsString()))
	//  g_message("new_char_step: cmin: `%s` | cmax: `%s`", cs.char_step.cmin, cs.char_step.cmax);
	cs.char_step.assigned = false
	cs.char_step.deep = deep
	cs.char_step.number = number
	cs.char_step.mutex = g_mutex_new()
	cs.char_step.field = field
	cs.char_step.previous = nil
	//  cs.char_step.list = list;
	cs.char_step.estimated_remaining_steps = 1
	cs.char_step.prefix = fmt.Sprintf("`%s` IS NULL OR `%s` = '%s' OR", field, field, cs.char_step.cmin_escaped)
	//  g_message("new_char_step: min: %s | max: %s ", cs.char_step.cmin_escaped, cs.char_step.cmax_escaped);
	cs.char_step.status = 0
	return cs
}

func next_chunk_in_char_step(cs *chunk_step) {
	cs.char_step.cmin_clen = cs.char_step.cursor_clen
	cs.char_step.cmin_len = cs.char_step.cursor_len
	cs.char_step.cmin = cs.char_step.cursor
	cs.char_step.cmin_escaped = cs.char_step.cursor_escaped
}

func split_char_step(o *OptionEntries, deep uint, number uint, previous_cs *chunk_step) *chunk_step {
	var cs = new(chunk_step)
	cs.char_step = new(char_step)
	cs.char_step.prefix = ""
	cs.char_step.assigned = true
	cs.char_step.deep = deep
	cs.char_step.number = number
	cs.char_step.mutex = g_mutex_new()
	cs.char_step.step = o.global.rows_per_file
	cs.char_step.field = previous_cs.char_step.field
	cs.char_step.previous = previous_cs
	cs.char_step.status = 0
	return cs
}

func new_integer_step(prefix string, field string, is_unsigned bool, types *int_types, deep uint, step uint64, number uint64, check_min bool, check_max bool) *chunk_step {
	var cs = new(chunk_step)
	cs.integer_step = new(integer_step)
	cs.integer_step.types = new(int_types)
	cs.integer_step.types.unsign = new(unsigned_int)
	cs.integer_step.types.sign = new(signed_int)
	cs.integer_step.is_unsigned = is_unsigned
	cs.integer_step.prefix = prefix
	cs.integer_step.step = step
	if cs.integer_step.is_unsigned {
		//    g_message("Is unsigned... ");
		cs.integer_step.types.unsign.min = types.unsign.min
		cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.min
		cs.integer_step.types.unsign.max = types.unsign.max
		cs.integer_step.estimated_remaining_steps = (cs.integer_step.types.unsign.max - cs.integer_step.types.unsign.min) / cs.integer_step.step
	} else {
		//    g_message("Is signed... ");
		cs.integer_step.types.sign.min = types.sign.min
		cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.min
		cs.integer_step.types.sign.max = types.sign.max
		cs.integer_step.estimated_remaining_steps = uint64(cs.integer_step.types.sign.max-cs.integer_step.types.sign.min) / cs.integer_step.step
	}
	cs.integer_step.deep = deep
	cs.integer_step.number = number
	cs.integer_step.field = field
	cs.integer_step.mutex = g_mutex_new()
	cs.integer_step.status = UNASSIGNED
	cs.integer_step.check_max = check_max
	cs.integer_step.check_min = check_min
	return cs
}

func new_real_partition_step(partition []string, deep, number uint) *chunk_step {
	var cs = new(chunk_step)
	cs.partition_step = new(partition_step)
	cs.partition_step.list = partition
	cs.partition_step.assigned = false
	cs.partition_step.mutex = g_mutex_new()
	cs.partition_step.deep = deep
	cs.partition_step.number = number
	return cs
}

func free_char_step(cs *chunk_step) {
	cs.char_step.mutex.Lock()
	cs.char_step.field = ""
	cs.char_step.prefix = ""
	cs.char_step.mutex.Unlock()
	cs.char_step.mutex = nil
}

func free_integer_step(cs *chunk_step) {
	if cs.integer_step.field != "" {
		cs.integer_step.field = ""
	}
	if cs.integer_step.prefix != "" {
		cs.integer_step.prefix = ""
	}

}

func get_next_integer_chunk(o *OptionEntries, dbt *db_table) *chunk_step {
	dbt.chunks_mutex.Lock()
	var cs *chunk_step
	var task any
	if dbt.chunks != nil {
		task = dbt.chunks_queue.try_pop()
		if task != nil {
			cs = task.(*chunk_step)
		} else {
			cs = nil
		}

		for cs != nil {
			cs.integer_step.mutex.Lock()
			if cs.integer_step.status == UNASSIGNED {
				//      g_message("Not assigned");
				cs.integer_step.status = ASSIGNED
				dbt.chunks_queue.push(cs)
				cs.integer_step.mutex.Unlock()
				dbt.chunks_mutex.Unlock()
				return cs
			}
			var ftype *int_types = new_int_types()
			if cs.integer_step.is_unsigned {
				if cs.integer_step.types.unsign.cursor < cs.integer_step.types.unsign.max && ((cs.integer_step.types.unsign.min != cs.integer_step.types.unsign.cursor && cs.integer_step.types.unsign.max-cs.integer_step.types.unsign.cursor > cs.integer_step.step) ||
					(cs.integer_step.types.unsign.min == cs.integer_step.types.unsign.cursor && cs.integer_step.types.unsign.max-cs.integer_step.types.unsign.cursor > 2*cs.integer_step.step)) {
					var new_minmax uint64
					ftype.unsign.max = cs.integer_step.types.unsign.max
					var new_cs *chunk_step
					if o.global.min_rows_per_file == o.global.rows_per_file && o.global.max_rows_per_file == o.global.rows_per_file {
						new_minmax = cs.integer_step.types.unsign.min + cs.integer_step.step*((cs.integer_step.types.unsign.max/cs.integer_step.step-cs.integer_step.types.unsign.min/cs.integer_step.step)/2)
						if new_minmax == cs.integer_step.types.unsign.cursor {
							new_minmax++
						}
						ftype.unsign.min = new_minmax
						new_cs = new_integer_step("", dbt.field, cs.integer_step.is_unsigned, ftype, cs.integer_step.deep+1, cs.integer_step.step, cs.integer_step.number, true, cs.integer_step.check_max)
					} else {
						new_minmax = cs.integer_step.types.unsign.cursor + (cs.integer_step.types.unsign.max-cs.integer_step.types.unsign.cursor)/2

						if new_minmax == cs.integer_step.types.unsign.cursor {
							new_minmax++
						}

						ftype.unsign.min = new_minmax
						new_cs = new_integer_step("", dbt.field, cs.integer_step.is_unsigned, ftype, cs.integer_step.deep+1, cs.integer_step.step, cs.integer_step.number+uint64(math.Pow(2, float64(cs.integer_step.deep))), true, cs.integer_step.check_max)
					}
					cs.integer_step.deep++
					cs.integer_step.check_max = true
					dbt.chunks = append(dbt.chunks, new_cs)
					cs.integer_step.types.unsign.max = new_minmax - 1
					new_cs.integer_step.status = ASSIGNED

					dbt.chunks_queue.push(cs)
					dbt.chunks_queue.push(new_cs)

					cs.integer_step.mutex.Unlock()
					dbt.chunks_mutex.Unlock()
					return new_cs
				} else {
					cs.integer_step.mutex.Unlock()
					if cs.integer_step.status == COMPLETED {
						free_integer_step(cs)
					}
				}
			} else {
				if cs.integer_step.types.sign.cursor < cs.integer_step.types.sign.max && ((cs.integer_step.types.sign.min != cs.integer_step.types.sign.cursor && gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.cursor) > int64(cs.integer_step.step)) ||
					(cs.integer_step.types.sign.min == cs.integer_step.types.sign.cursor && gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.cursor) > int64(2*cs.integer_step.step))) {
					var new_minmax int64
					ftype.sign.max = cs.integer_step.types.sign.max
					var new_cs *chunk_step
					if o.global.min_rows_per_file == o.global.rows_per_file && o.global.max_rows_per_file == o.global.rows_per_file {
						new_minmax = cs.integer_step.types.sign.cursor + int64(cs.integer_step.step)*(cs.integer_step.types.sign.max/int64(cs.integer_step.step)-cs.integer_step.types.sign.cursor/int64(cs.integer_step.step))/2
						if new_minmax == cs.integer_step.types.sign.cursor {
							new_minmax++
						}
						ftype.sign.min = new_minmax
						new_cs = new_integer_step("", dbt.field, cs.integer_step.is_unsigned, ftype, cs.integer_step.deep+1, cs.integer_step.step, cs.integer_step.number, true, cs.integer_step.check_max)
					} else {
						if gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.cursor) > int64(cs.integer_step.step) {
							new_minmax = cs.integer_step.types.sign.cursor + (cs.integer_step.types.sign.max-cs.integer_step.types.sign.cursor)/2
						} else {
							new_minmax = cs.integer_step.types.sign.cursor + 1
						}
						if new_minmax == cs.integer_step.types.sign.cursor {
							new_minmax++
						}

						ftype.sign.min = new_minmax

						new_cs = new_integer_step("", dbt.field, cs.integer_step.is_unsigned, ftype, cs.integer_step.deep+1, cs.integer_step.step, cs.integer_step.number+uint64(math.Pow(2, float64(cs.integer_step.deep))), true, cs.integer_step.check_max)
					}
					cs.integer_step.deep++
					cs.integer_step.check_max = true
					dbt.chunks = append(dbt.chunks, new_cs)
					cs.integer_step.types.sign.max = new_minmax - 1
					new_cs.integer_step.status = ASSIGNED
					dbt.chunks_queue.push(cs)
					dbt.chunks_queue.push(new_cs)
					cs.integer_step.mutex.Unlock()
					dbt.chunks_mutex.Unlock()
					return new_cs
				} else {
					cs.integer_step.mutex.Unlock()
					if cs.integer_step.status == COMPLETED {
						free_integer_step(cs)
					}
				}
			}
			task = dbt.chunks_queue.try_pop()
			if task != nil {
				cs = task.(*chunk_step)
			} else {
				cs = nil
			}
		}
	}
	dbt.chunks_mutex.Unlock()
	return nil
}

func get_next_char_chunk(o *OptionEntries, dbt *db_table) *chunk_step {
	dbt.chunks_mutex.Lock()
	var cs *chunk_step
	for _, chunk := range dbt.chunks {
		cs = chunk.(*chunk_step)
		if cs.char_step.mutex == nil {
			log.Infof("This should not happen")
			continue
		}

		cs.char_step.mutex.Lock()
		if !cs.char_step.assigned {
			cs.char_step.assigned = true
			cs.char_step.mutex.Unlock()
			dbt.chunks_mutex.Unlock()
			return cs
		}
		if cs.char_step.deep <= o.Chunks.CharDeep && strings.Compare(cs.char_step.cmax, cs.char_step.cursor) != 0 && cs.char_step.status == 0 {
			var new_cs = split_char_step(o, cs.char_step.deep+1, cs.char_step.number+uint(math.Pow(2, float64(cs.char_step.deep))), cs)
			cs.char_step.deep++
			cs.char_step.status = 1
			new_cs.char_step.assigned = true
			return new_cs
		} else {
			//      g_message("Not able to split because %d > %d | %s == %s | %d != 0", cs.char_step.deep,num_threads, cs.char_step.cmax, cs.char_step.cursor, cs.char_step.status);
		}
		cs.char_step.mutex.Unlock()
	}
	dbt.chunks_mutex.Unlock()
	return nil
}

func get_next_partition_chunk(dbt *db_table) *chunk_step {
	dbt.chunks_mutex.Lock()
	var cs *chunk_step
	for _, chunk := range dbt.chunks {
		cs = chunk.(*chunk_step)
		cs.partition_step.mutex.Lock()
		if !cs.partition_step.assigned {
			cs.partition_step.assigned = true
			cs.partition_step.mutex.Unlock()
			dbt.chunks_mutex.Unlock()
			return cs
		}

		if len(cs.partition_step.list) > 3 {
			var pos = uint(len(cs.partition_step.list)) / 2
			var new_list = cs.partition_step.list[pos:]
			var new_cs = new_real_partition_step(new_list, cs.partition_step.deep+1, cs.partition_step.number+uint(math.Pow(2, float64(cs.partition_step.deep))))
			cs.partition_step.deep++
			new_cs.partition_step.assigned = true
			dbt.chunks = append(dbt.chunks, new_cs)

			cs.partition_step.mutex.Unlock()
			dbt.chunks_mutex.Unlock()
			return new_cs
		}
		cs.partition_step.mutex.Unlock()
	}
	dbt.chunks_mutex.Unlock()
	return nil
}

func get_next_chunk(o *OptionEntries, dbt *db_table) *chunk_step {
	switch dbt.chunk_type {
	case CHAR:
		return get_next_char_chunk(o, dbt)
	case INTEGER:
		return get_next_integer_chunk(o, dbt)
	case PARTITION:
		return get_next_partition_chunk(dbt)
	default:

	}
	return nil
}

func get_partitions_for_table(o *OptionEntries, conn *client.Conn, database string, table string) []string {
	var partition_list []string
	var query = fmt.Sprintf("select PARTITION_NAME from information_schema.PARTITIONS where PARTITION_NAME is not null and TABLE_SCHEMA='%s' and TABLE_NAME='%s'", database, table)
	res, err := conn.Execute(query)
	if err != nil {
		log.Errorf("get partition name fail:%v", err)
		return partition_list
	}
	for _, row := range res.Values {
		if eval_partition_regex(o, string(row[0].AsString())) {
			partition_list = append(partition_list, string(row[0].AsString()))
		}
	}
	return partition_list
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
	for i = 0; i < cresultlen; i++ {
		cu1 = c1[i]
		cu2 = c2[i]
		if cu2 != cu1 {
			if cu2 > cu1 {
				// 58
				cresult[i] = cu1 + uint8(int(math.Abs(float64(cu2)-float64(cu1)))/int(part))
			} else {
				// 48 + abs(48 - 90) / 2 = 155 / 58
				cresult[i] = cu2 + uint8(int(math.Abs(float64(cu2)-float64(cu1)))/int(part))
			}
		} else {
			cresultlen = i
		}
	}
	cu1 = c1[0]
	cu2 = c2[0]
	// cresult[cresultlen] = 0
	escapedresult := mysql.Escape(string(cresult))
	return escapedresult
}

func update_integer_min(o *OptionEntries, conn *client.Conn, tj *table_job) {
	var cs = tj.chunk_step
	var query string
	var minmax *mysql.Result
	var cache string
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	if cs.integer_step.is_unsigned {
		query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE %s %d <= `%s` AND `%s` <= %d ORDER BY `%s` ASC LIMIT 1", cache, tj.dbt.field, tj.dbt.database.name, tj.dbt.table, cs.integer_step.prefix, cs.integer_step.types.unsign.min, tj.dbt.field, tj.dbt.field, cs.integer_step.types.unsign.max, tj.dbt.field)

	} else {
		query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE %s %d  <= `%s` AND `%s` <= %d  ORDER BY `%s` ASC LIMIT 1", cache, tj.dbt.field, tj.dbt.database.name, tj.dbt.table, cs.integer_step.prefix, cs.integer_step.types.sign.min, tj.dbt.field, tj.dbt.field, cs.integer_step.types.sign.max, tj.dbt.field)
	}
	minmax, _ = conn.Execute(query)
	if len(minmax.Values) == 0 {
		return
	}
	for _, row := range minmax.Values {
		if row[0].Value() == nil {
			return
		}
		if cs.integer_step.is_unsigned {
			var nmin = row[0].AsUint64()
			cs.integer_step.types.unsign.min = nmin
		} else {
			var nmin = row[0].AsInt64()
			cs.integer_step.types.sign.min = nmin
		}
	}
	return

}

func update_integer_max(o *OptionEntries, conn *client.Conn, tj *table_job) {
	var cs = tj.chunk_step
	var query, cache string
	var minmax *mysql.Result
	var row []mysql.FieldValue
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	if cs.integer_step.is_unsigned {
		query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE %d  <= `%s` AND `%s` <= %d  ORDER BY `%s` DESC LIMIT 1", cache, tj.dbt.field, tj.dbt.database.name, tj.dbt.table, cs.integer_step.types.unsign.min, tj.dbt.field, tj.dbt.field, cs.integer_step.types.unsign.max, tj.dbt.field)
	} else {
		query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE %d  <= `%s` AND `%s` <= %d  ORDER BY `%s` DESC LIMIT 1", cache, tj.dbt.field, tj.dbt.database.name, tj.dbt.table, cs.integer_step.types.sign.min, tj.dbt.field, tj.dbt.field, cs.integer_step.types.sign.max, tj.dbt.field)
	}
	minmax, _ = conn.Execute(query)
	if len(minmax.Values) == 0 {
		if cs.integer_step.is_unsigned {
			cs.integer_step.types.unsign.max = cs.integer_step.types.unsign.min
		} else {
			cs.integer_step.types.sign.max = cs.integer_step.types.sign.min
		}
		return
	}
	for _, row = range minmax.Values {
		if cs.integer_step.is_unsigned {
			var nmax = row[0].AsUint64()
			cs.integer_step.types.unsign.max = nmax
		} else {
			var nmax = row[0].AsInt64()
			cs.integer_step.types.sign.max = nmax
		}
	}
	return
}

func update_cursor(o *OptionEntries, conn *client.Conn, tj *table_job) string {
	var cs = tj.chunk_step
	var query, cache string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var char_chunk_part int
	if tj.char_chunk_part > 0 {
		char_chunk_part = tj.char_chunk_part
	} else {
		char_chunk_part = 1
	}
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var middle = get_escaped_middle_char(conn, []byte(cs.char_step.cmax), cs.char_step.cmax_clen, []byte(cs.char_step.cmin), cs.char_step.cmin_clen, uint(char_chunk_part))
	query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE '%s' <= `%s` AND '%s' <= `%s` AND `%s` <= '%s' ORDER BY `%s` LIMIT 1", cache, tj.dbt.field, tj.dbt.database.name, tj.dbt.table, cs.char_step.cmin_escaped, tj.dbt.field, middle, tj.dbt.field, tj.dbt.field, cs.char_step.cmax_escaped, tj.dbt.field)
	minmax, _ = conn.Execute(query)
	if len(minmax.Values) == 0 {
		cs.char_step.cursor_clen = cs.char_step.cmax_clen
		cs.char_step.cursor_len = cs.char_step.cmax_len
		cs.char_step.cursor = cs.char_step.cmax
		cs.char_step.cursor_escaped = cs.char_step.cmax_escaped
		return ""
	}
	var lengths = minmax.Values[0]
	for _, row = range minmax.Values {
		tj.char_chunk_part--
		if strings.Compare(string(row[0].AsString()), cs.char_step.cmax) != 0 && strings.Compare(string(row[0].AsString()), cs.char_step.cmin) != 0 {
			cs.char_step.cursor_clen = uint(len(lengths[0].AsString()))
			cs.char_step.cursor_len = uint(len(lengths[0].AsString())) + 1
			cs.char_step.cursor = string(row[0].AsString())
			cs.char_step.cursor_escaped = mysql.Escape(string(row[0].AsString()))
		} else {
			cs.char_step.cursor_clen = cs.char_step.cmax_clen
			cs.char_step.cursor_len = cs.char_step.cmax_len
			cs.char_step.cursor = cs.char_step.cmax
			cs.char_step.cursor_escaped = cs.char_step.cmax_escaped
		}
		return ""
	}
	return ""
}

func get_new_minmax(o *OptionEntries, td *thread_data, dbt *db_table, cs *chunk_step) bool {
	var query, cache, cursor, escaped string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var previous = cs.char_step.previous
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
	var middle = get_escaped_middle_char(td.thrconn, []byte(previous.char_step.cmax), previous.char_step.cmax_clen, []byte(cursor), cursor_len, o.Chunks.CharChunk)
	query = fmt.Sprintf("SELECT %s `%s` FROM `%s`.`%s` WHERE `%s` > (SELECT `%s` FROM `%s`.`%s` WHERE `%s` > '%s' ORDER BY `%s` LIMIT 1) AND '%s' < `%s` AND `%s` < '%s' ORDER BY `%s` LIMIT 1",
		cache, dbt.field, dbt.database.name, dbt.table, dbt.field, dbt.field, dbt.database.name, dbt.table, dbt.field, middle, dbt.field, escaped, dbt.field, dbt.field, previous.char_step.cmax_escaped, dbt.field)
	minmax, _ = td.thrconn.Execute(query)
	if len(minmax.Values) == 0 {
		return false
	}

	row = minmax.Values[0]
	var lengths = row
	cs.char_step.cmax_clen = previous.char_step.cmax_clen
	cs.char_step.cmax_len = previous.char_step.cmax_len
	cs.char_step.cmax = previous.char_step.cmax
	cs.char_step.cmax_escaped = previous.char_step.cmax_escaped
	previous.char_step.cmax_clen = uint(len(lengths[0].AsString()))
	previous.char_step.cmax_len = uint(len(lengths[0].AsString())) + 1
	previous.char_step.cmax = string(row[0].AsString())
	previous.char_step.cmax_escaped = mysql.Escape(string(row[0].AsString()))
	previous.char_step.status = 0
	cs.char_step.cmin_clen = uint(len(lengths[0].AsString()))
	cs.char_step.cmin_len = uint(len(lengths[0].AsString())) + 1
	cs.char_step.cmin = string(row[0].AsString())
	cs.char_step.cmin_escaped = mysql.Escape(string(row[0].AsString()))
	return true
}

func set_chunk_strategy_for_dbt(o *OptionEntries, conn *client.Conn, dbt *db_table) {
	var partitions []string
	if o.Chunks.SplitPartitions {
		partitions = get_partitions_for_table(o, conn, dbt.database.name, dbt.table)
	}
	if len(partitions) > 0 {
		dbt.chunks = append(dbt.chunks, new_real_partition_step(partitions, 0, 0))
		dbt.chunk_type = PARTITION
		return
	}
	if o.global.rows_per_file > 0 && dbt.rows_in_sts > o.global.min_rows_per_file {
		var query, cache, where string
		var minmax *mysql.Result
		var row []mysql.FieldValue
		if o.is_mysql_like() {
			cache = "/*!40001 SQL_NO_CACHE */"
		}
		if o.Filter.WhereOption != "" {
			where = "WHERE"
		}
		query = fmt.Sprintf("SELECT %s MIN(`%s`),MAX(`%s`),LEFT(MIN(`%s`),1),LEFT(MAX(`%s`),1) FROM `%s`.`%s` %s %s", cache, dbt.field, dbt.field, dbt.field, dbt.field, dbt.database.name, dbt.table, where, o.Filter.WhereOption)
		minmax, _ = conn.Execute(query)
		if len(minmax.Values) == 0 {
			dbt.chunk_type = NONE
			return
		}
		row = minmax.Values[0]
		var fields = minmax.Fields
		var abs, unmin, unmax uint64
		var nmin, nmax int64
		var prefix string
		var cs *chunk_step
		switch fields[0].Type {
		case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_SHORT:
			if o.global.min_rows_per_file == o.global.rows_per_file && o.global.max_rows_per_file == o.global.rows_per_file {
				dbt.chunk_filesize = 0
			}
			unmin = row[0].AsUint64()
			unmax = row[1].AsUint64() + 1
			nmin = row[0].AsInt64()
			nmax = row[1].AsInt64() + 1
			if intToBool(int(fields[0].Flag & mysql.UNSIGNED_FLAG)) {
				prefix = fmt.Sprintf("`%s` IS NULL OR", dbt.field)
				abs = uint64(gint64_abs(int64(unmax - unmin)))
			} else {
				prefix = fmt.Sprintf("`%s` IS NULL OR ", dbt.field)
				abs = uint64(gint64_abs(nmax - nmin))
			}
			if abs > o.global.min_rows_per_file {
				var ftype *int_types = new_int_types()

				if intToBool(int(fields[0].Flag & mysql.UNSIGNED_FLAG)) {
					ftype.unsign.min = unmin
					ftype.unsign.max = unmax
					cs = new_integer_step(prefix, dbt.field, intToBool(int(fields[0].Flag&mysql.UNSIGNED_FLAG)), ftype, 0, o.global.rows_per_file, 0, false, false)
				} else {
					ftype.sign.min = nmin
					ftype.sign.max = nmax
					cs = new_integer_step(prefix, dbt.field, intToBool(int(fields[0].Flag&mysql.UNSIGNED_FLAG)), ftype, 0, o.global.rows_per_file, 0, false, false)
				}

				dbt.chunks = append(dbt.chunks, cs)
				dbt.chunk_type = INTEGER
				dbt.chunks_queue.push(cs)
				dbt.estimated_remaining_steps = cs.integer_step.estimated_remaining_steps
			} else {
				dbt.chunk_type = NONE
			}
			return
		case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VAR_STRING:
			cs = new_char_step(o, conn, dbt.field, 0, 0, row, row)
			dbt.chunks = append(dbt.chunks, cs)
			dbt.chunk_type = CHAR
			dbt.chunks_queue.push(cs)
			return
		default:
			dbt.chunk_type = NONE
		}
	} else {
		dbt.chunk_type = NONE
	}
}

func get_field_for_dbt(conn *client.Conn, dbt *db_table, conf *configuration) string {
	var indexes *mysql.Result
	var row []mysql.FieldValue
	var field string
	var query string
	query = fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", dbt.database.name, dbt.table)
	indexes, _ = conn.Execute(query)
	for _, row = range indexes.Values {
		if string(row[2].AsString()) == "PRIMARY" && row[3].AsInt64() == 1 {
			field = string(row[4].AsString())
			break
		}
	}
	if field == "" {
		for _, row = range indexes.Values {
			if row[1].AsInt64() == 0 && row[3].AsInt64() == 1 {
				field = string(row[4].AsString())
				break
			}
		}
	}
	if field == "" && conf.use_any_index != "" {
		var max_cardinality, cardinality uint64
		for _, row = range indexes.Values {
			if row[3].AsInt64() == 1 {
				if row[6].Value() != nil {
					cardinality = row[6].AsUint64()
					if cardinality > max_cardinality {
						field = string(row[4].AsString())
						max_cardinality = cardinality
					}
				}
			}
		}
	}

	return field
}

func get_next_dbt_and_chunk(o *OptionEntries, dbt *db_table, cs *chunk_step, dbt_list []*db_table) (*db_table, *chunk_step, []*db_table, bool) {
	var lcs *chunk_step
	var d *db_table
	var are_there_jobs_defining bool
	for _, d = range dbt_list {
		if d.chunk_type != DEFINING {
			if d.chunk_type == NONE {
				dbt = d
				// dbt_list = g_list_remove(dbt_list, d)
				i := slices.Index(dbt_list, d)
				dbt_list = append(dbt_list[:i], dbt_list[i+1:]...)
				break
			}
			if d.chunk_type == UNDEFINED {
				dbt = d
				d.chunk_type = DEFINING
				are_there_jobs_defining = true
				break
			}
			lcs = get_next_chunk(o, d)
			if lcs != nil {
				cs = lcs
				dbt = d
				break
			} else {
				// dbt_list = g_list_remove(dbt_list, d)
				i := slices.Index(dbt_list, d)
				dbt_list = append(dbt_list[:i], dbt_list[i+1:]...)
				continue
			}
		} else {
			are_there_jobs_defining = true
		}
	}
	return dbt, cs, dbt_list, are_there_jobs_defining
}

func give_me_another_non_innodb_chunk_step(o *OptionEntries) {
	o.global.give_me_another_non_innodb_chunk_step_queue.push(1)
}

func give_me_another_innodb_chunk_step(o *OptionEntries) {
	o.global.give_me_another_innodb_chunk_step_queue.push(1)
}

func enqueue_shutdown_jobs(o *OptionEntries, queue *asyncQueue) {
	var n uint
	for n = 0; n < o.Common.NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		queue.push(j)
	}
}

func table_job_enqueue(o *OptionEntries, pop_queue *asyncQueue, conf *configuration, is_innodb bool) {
	var dbt *db_table
	var cs *chunk_step
	var are_there_jobs_defining bool
	var push_queue *asyncQueue
	// var table_list []*db_table
	if is_innodb {
		push_queue = conf.innodb_queue
	} else {
		push_queue = conf.non_innodb_queue
	}
	for {
		pop_queue.pop()
		if o.global.shutdown_triggered {
			return
		}
		dbt = nil
		cs = nil
		are_there_jobs_defining = false
		if is_innodb {
			// 解决slice传递指针问题
			dbt, cs, o.global.innodb_table, are_there_jobs_defining = get_next_dbt_and_chunk(o, dbt, cs, o.global.innodb_table)
		} else {
			dbt, cs, o.global.non_innodb_table, are_there_jobs_defining = get_next_dbt_and_chunk(o, dbt, cs, o.global.non_innodb_table)
		}

		if cs == nil && dbt == nil {
			if are_there_jobs_defining {
				log.Debugf("chunk_builder_thread: Are jobs defining... should we wait and try again later?")
				pop_queue.push(1)
				time.Sleep(1 * time.Microsecond)
				continue
			}
			log.Debugf("chunk_builder_thread: There were not job defined")
			break
		}
		log.Debugf("chunk_builder_thread: Job will be enqueued")
		switch dbt.chunk_type {
		case INTEGER:
			create_job_to_dump_chunk(o, dbt, "", cs.integer_step.number, dbt.primary_key, cs, push_queue, true)
		case CHAR:
			create_job_to_dump_chunk(o, dbt, "", uint64(cs.char_step.number), dbt.primary_key, cs, push_queue, false)
		case PARTITION:
			create_job_to_dump_chunk(o, dbt, "", uint64(cs.partition_step.number), dbt.primary_key, cs, push_queue, true)
		case NONE:
			create_job_to_dump_chunk(o, dbt, "", 0, dbt.primary_key, cs, push_queue, true)
		case DEFINING:
			create_job_to_determine_chunk_type(dbt, push_queue)
		default:
			log.Errorf("This should not happen")
		}
	}
	return
}

func chunk_builder_thread(o *OptionEntries, conf *configuration) {
	if o.global.chunk_builder == nil {
		o.global.chunk_builder = new(sync.WaitGroup)
	}
	o.global.chunk_builder.Add(1)
	defer o.global.chunk_builder.Done()
	log.Info("Starting Non-InnoDB tables")
	table_job_enqueue(o, o.global.give_me_another_non_innodb_chunk_step_queue, conf, false)
	log.Infof("Non-InnoDB tables completed")
	enqueue_shutdown_jobs(o, conf.non_innodb_queue)

	log.Infof("Starting InnoDB tables")
	table_job_enqueue(o, o.global.give_me_another_innodb_chunk_step_queue, conf, true)
	log.Infof("InnoDB tables completed")
	enqueue_shutdown_jobs(o, conf.innodb_queue)

	return
}
