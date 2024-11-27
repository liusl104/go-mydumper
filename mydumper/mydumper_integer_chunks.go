package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"math"
	"sync/atomic"
	"time"
)

func gint64_abs(a int64) uint64 {
	if a >= 0 {
		return uint64(a)
	}
	return uint64(-a)
}
func initialize_integer_step(o *OptionEntries, cs *chunk_step, is_unsigned bool, types *int_types, is_step_fixed_length bool,
	step uint64, min_css uint64, max_css uint64, check_min bool, check_max bool) {
	cs.integer_step.is_unsigned = is_unsigned
	cs.integer_step.min_chunk_step_size = min_css
	cs.integer_step.max_chunk_step_size = max_css
	if cs.integer_step.is_unsigned {
		cs.integer_step.types.unsign.min = types.unsign.min
		cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.min
		cs.integer_step.types.unsign.max = types.unsign.max
		if step != 0 {
			cs.integer_step.step = step
		} else {
			cs.integer_step.step = (cs.integer_step.types.unsign.max - cs.integer_step.types.unsign.min) / uint64(o.Common.NumThreads)
		}
		if cs.integer_step.step > 0 {
			cs.integer_step.estimated_remaining_steps = (cs.integer_step.types.unsign.max - cs.integer_step.types.unsign.min) / cs.integer_step.step
		} else {
			cs.integer_step.estimated_remaining_steps = 1
		}
	} else {
		cs.integer_step.types.sign.min = types.sign.min
		cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.min
		cs.integer_step.types.sign.max = types.sign.max
		if step != 0 {
			cs.integer_step.step = step
		} else {
			cs.integer_step.step = gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.min)/uint64(o.Common.NumThreads) + 1
		}
		if cs.integer_step.step > 0 {
			cs.integer_step.estimated_remaining_steps = uint64(cs.integer_step.types.sign.max-cs.integer_step.types.sign.min) / cs.integer_step.step
		} else {
			cs.integer_step.estimated_remaining_steps = 1
		}
	}
	cs.integer_step.is_step_fixed_length = is_step_fixed_length
	cs.integer_step.check_max = check_max
	cs.integer_step.check_min = check_min
}

func new_integer_step(o *OptionEntries, is_unsigned bool, types *int_types, is_step_fixed_length bool, step uint64, min_css uint64, max_css uint64, check_min bool, check_max bool) *chunk_step {
	var cs = new(chunk_step)
	cs.integer_step = new(integer_step)
	cs.integer_step.types = new(int_types)
	cs.integer_step.types.unsign = new(unsigned_int)
	cs.integer_step.types.sign = new(signed_int)
	initialize_integer_step(o, cs, is_unsigned, types, is_step_fixed_length, step, min_css, max_css, check_min, check_max)
	return cs
}

func initialize_integer_step_item(o *OptionEntries, csi *chunk_step_item, include_null bool, prefix string, field string, is_unsigned bool,
	types *int_types, deep uint, is_step_fixed_length bool, step uint64, min_css uint64, max_css uint64,
	number uint64, check_min bool, check_max bool, next *chunk_step_item, position uint) {
	csi.chunk_step = new_integer_step(o, is_unsigned, types, is_step_fixed_length, step, min_css, max_css, check_min, check_max)
	csi.chunk_type = INTEGER
	csi.position = position
	csi.next = next
	csi.status = UNASSIGNED
	if csi.chunk_functions == nil {
		csi.chunk_functions = new(chunk_functions)
	}
	csi.chunk_functions.process = process_integer_chunk
	csi.chunk_functions.get_next = get_next_integer_chunk
	csi.where = ""
	csi.include_null = include_null
	csi.prefix = prefix
	csi.field = field
	csi.mutex = g_mutex_new()
	csi.number = number
	csi.deep = deep
	csi.needs_refresh = false
}

func new_integer_step_item(o *OptionEntries, include_null bool, prefix string, field string, is_unsigned bool,
	types *int_types, deep uint, is_step_fixed_length bool, step uint64, min_css uint64, max_css uint64,
	number uint64, check_min bool, check_max bool, next *chunk_step_item, position uint) *chunk_step_item {
	var csi = new(chunk_step_item)
	initialize_integer_step_item(o, csi, include_null, prefix, field, is_unsigned, types, deep, is_step_fixed_length,
		step, min_css, max_css, number, check_min, check_max, next, position)
	return csi
}

func free_integer_step(cs *chunk_step) {
	cs = nil
}

func free_integer_step_item(csi *chunk_step_item) {
	if csi != nil && csi.chunk_step != nil {
		free_integer_step(csi.chunk_step)
		csi.chunk_step = nil
	}
}

func split_chunk_step(o *OptionEntries, csi *chunk_step_item) *chunk_step_item {
	var new_csi *chunk_step_item
	var number uint = uint(csi.number)
	var new_minmax_signed int64 = 0
	var new_minmax_unsigned uint64 = 0
	var types *int_types = new(int_types)
	types.unsign = new(unsigned_int)
	types.sign = new(signed_int)
	var ics *integer_step = csi.chunk_step.integer_step
	if ics.is_unsigned {
		types.unsign.max = ics.types.unsign.max
		if csi.status == DUMPING_CHUNK {
			types.unsign.min = ics.types.unsign.cursor
		} else {
			types.unsign.min = ics.types.unsign.min
		}
	} else {
		types.sign.max = ics.types.sign.max
		if csi.status == DUMPING_CHUNK {
			types.sign.min = ics.types.sign.cursor
		} else {
			types.sign.min = ics.types.sign.min
		}
	}
	if ics.is_step_fixed_length {
		if ics.is_unsigned {
			new_minmax_unsigned = (types.unsign.min/ics.step)*ics.step + ics.step*((((ics.types.unsign.max/ics.step)-(types.unsign.min/ics.step))/2)+1)
			if (types.unsign.min / ics.step) == (new_minmax_unsigned / ics.step) {
				return nil
			}
			if new_minmax_unsigned == types.unsign.min {
				return nil
			}

			types.unsign.min = new_minmax_unsigned
		} else {

			new_minmax_signed = (types.sign.min/int64(ics.step))*int64(ics.step) + int64(ics.step)*((((ics.types.sign.max/int64(ics.step))-(types.sign.min/int64(ics.step)))/2)+1)
			if types.sign.min/int64(ics.step) == new_minmax_signed/int64(ics.step) {
				return nil
			}
			if new_minmax_signed == types.sign.min {
				return nil
			}
			types.sign.min = new_minmax_signed
		}

	} else {
		number += uint(math.Pow(2, float64(csi.deep)))
		if ics.is_unsigned {
			new_minmax_unsigned = types.unsign.min + (ics.types.unsign.max-types.unsign.min)/2
			if new_minmax_unsigned == types.unsign.min {
				new_minmax_unsigned++
			}

			types.unsign.min = new_minmax_unsigned
		} else {
			new_minmax_signed = types.sign.min + (ics.types.sign.max-types.sign.min)/2
			if new_minmax_signed == types.sign.min {
				new_minmax_signed++
			}

			types.sign.min = new_minmax_signed
		}
	}

	new_csi = new_integer_step_item(o, false, "", csi.field, csi.chunk_step.integer_step.is_unsigned,
		types, csi.deep+1, csi.chunk_step.integer_step.is_step_fixed_length, csi.chunk_step.integer_step.step,
		csi.chunk_step.integer_step.min_chunk_step_size, csi.chunk_step.integer_step.max_chunk_step_size,
		uint64(number), true, csi.chunk_step.integer_step.check_max, nil, csi.position)
	new_csi.status = ASSIGNED

	csi.chunk_step.integer_step.check_max = true
	if ics.is_unsigned {
		csi.chunk_step.integer_step.types.unsign.max = new_minmax_unsigned - 1
	} else {
		csi.chunk_step.integer_step.types.sign.max = new_minmax_signed - 1
	}
	csi.deep = csi.deep + 1
	return new_csi
}

func has_only_one_level(csi *chunk_step_item) bool {
	return csi.chunk_step.integer_step.is_step_fixed_length && ((csi.chunk_step.integer_step.is_unsigned &&
		csi.chunk_step.integer_step.types.unsign.max == csi.chunk_step.integer_step.types.unsign.min) ||
		(!csi.chunk_step.integer_step.is_unsigned &&
			csi.chunk_step.integer_step.types.sign.max == csi.chunk_step.integer_step.types.sign.min))
}

func is_splitable(csi *chunk_step_item) bool {
	return !csi.chunk_step.integer_step.is_step_fixed_length && ((csi.chunk_step.integer_step.is_unsigned && (csi.chunk_step.integer_step.types.unsign.cursor < csi.chunk_step.integer_step.types.unsign.max && ((csi.status == DUMPING_CHUNK && (csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.cursor) >= csi.chunk_step.integer_step.step) ||
		(csi.status == ASSIGNED && (csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.min) >= csi.chunk_step.integer_step.step)))) || (!csi.chunk_step.integer_step.is_unsigned && (csi.chunk_step.integer_step.types.sign.cursor < csi.chunk_step.integer_step.types.sign.max &&
		(csi.status == DUMPING_CHUNK && gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.cursor) >= csi.chunk_step.integer_step.step ||
			(csi.status == ASSIGNED && gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.min) >= csi.chunk_step.integer_step.step))))) ||
		(csi.chunk_step.integer_step.is_step_fixed_length && ((csi.chunk_step.integer_step.is_unsigned && csi.chunk_step.integer_step.types.unsign.max/csi.chunk_step.integer_step.step > csi.chunk_step.integer_step.types.unsign.min/csi.chunk_step.integer_step.step+1) || (!csi.chunk_step.integer_step.is_unsigned && csi.chunk_step.integer_step.types.sign.max/int64(csi.chunk_step.integer_step.step) > csi.chunk_step.integer_step.types.sign.min/int64(csi.chunk_step.integer_step.step)+1)))
}

func clone_chunk_step_item(o *OptionEntries, csi *chunk_step_item) *chunk_step_item {
	return new_integer_step_item(o, csi.include_null, csi.prefix, csi.field, csi.chunk_step.integer_step.is_unsigned, csi.chunk_step.integer_step.types, csi.deep, csi.chunk_step.integer_step.is_step_fixed_length, csi.chunk_step.integer_step.step, csi.chunk_step.integer_step.min_chunk_step_size, csi.chunk_step.integer_step.max_chunk_step_size, csi.number, csi.chunk_step.integer_step.check_min, csi.chunk_step.integer_step.check_max, nil, csi.position)
}

func get_next_integer_chunk(o *OptionEntries, dbt *db_table) *chunk_step_item {
	var csi, new_csi *chunk_step_item
	var task any
	if dbt.chunks != nil {
		task = dbt.chunks_queue.try_pop()
		if task != nil {
			csi = task.(*chunk_step_item)
		}
		for csi != nil {
			csi.mutex.Lock()
			if csi.status == UNASSIGNED {
				csi.status = ASSIGNED
				dbt.chunks_queue.push(csi)
				csi.mutex.Unlock()
				return csi
			}
			if csi.status != COMPLETED {
				if is_splitable(csi) {
					new_csi = split_chunk_step(o, csi)
					if new_csi != nil {
						dbt.chunks.PushBack(new_csi)
						dbt.chunks_queue.push(csi)
						dbt.chunks_queue.push(new_csi)
						csi.mutex.Unlock()
						return new_csi
					}
				} else {
					if dbt.multicolumn && csi.next != nil && csi.next.chunk_type == INTEGER {
						csi.next.mutex.Lock()
						if csi.next.status != COMPLETED && has_only_one_level(csi) && is_splitable(csi.next) {
							csi.deep = csi.deep + 1
							new_csi = clone_chunk_step_item(o, csi)
							if csi.chunk_step.integer_step.is_step_fixed_length {
								new_csi.number += uint64(math.Pow(2, float64(csi.deep)))
							}
							update_where_on_integer_step(o, new_csi)
							new_csi.next = split_chunk_step(o, csi.next)
							if new_csi.next != nil {
								new_csi.next.prefix = new_csi.where
								dbt.chunks.PushBack(new_csi)
								dbt.chunks_queue.push(csi)
								dbt.chunks_queue.push(new_csi)
								csi.next.mutex.Unlock()
								csi.mutex.Unlock()
								return new_csi
							} else {
								free_integer_step_item(new_csi)
							}
						}
						csi.next.mutex.Unlock()

						//split_unsigned_chunk_step
					}
				}
			} else {
				free_integer_step_item(csi)
			}
			dbt.chunks_mutex.Unlock()
			csi = dbt.chunks_queue.try_pop().(*chunk_step_item)
		}
	}
	return nil
}

func refresh_integer_min_max(o *OptionEntries, conn *client.Conn, dbt *db_table, csi *chunk_step_item) {
	var ics *integer_step = csi.chunk_step.integer_step
	var query string
	var cache string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var err error
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where_prefix string
	var prefix string
	if csi.prefix != "" {
		where_prefix = " WHERE "
		prefix = csi.prefix
	}
	query = fmt.Sprintf("SELECT %s MIN(%s%s%s),MAX(%s%s%s) FROM %s%s%s.%s%s%s%s%s", cache,
		o.global.identifier_quote_character_str, csi.field, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, csi.field,
		o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, dbt.table,
		o.global.identifier_quote_character_str,
		where_prefix, prefix)
	log.Debugf("query sql: %s", query)
	minmax, err = conn.Execute(query)
	if err != nil {
		return
	}
	for _, row = range minmax.Values {
		if ics.is_unsigned {
			var nmin uint64 = row[0].AsUint64()
			var nmax uint64 = row[1].AsUint64()
			ics.types.unsign.min = nmin
			ics.types.unsign.max = nmax
		} else {
			var nmin int64 = row[0].AsInt64()
			var nmax int64 = row[1].AsInt64()
			ics.types.sign.min = nmin
			ics.types.sign.max = nmax
		}
		csi.include_null = true
	}
	return

}

func update_integer_min(o *OptionEntries, conn *client.Conn, dbt *db_table, csi *chunk_step_item) {
	var ics = csi.chunk_step.integer_step
	var query string
	var minmax *mysql.Result
	var cache string
	var err error
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where string = ""
	update_integer_where_on_gstring(o, &where, false, csi.prefix, csi.field, csi.chunk_step.integer_step.is_unsigned, csi.chunk_step.integer_step.types, false)
	query = fmt.Sprintf("SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s ASC LIMIT 1",
		cache,
		o.global.identifier_quote_character_str, csi.field, o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, dbt.table, o.global.identifier_quote_character_str,
		where,
		o.global.identifier_quote_character_str, csi.field, o.global.identifier_quote_character_str)

	minmax, err = conn.Execute(query)
	if err != nil {
		return
	}
	if len(minmax.Values) == 0 {
		return
	}
	for _, row := range minmax.Values {
		if row[0].Value() == nil {
			return
		}
		if ics.is_unsigned {
			var nmin = row[0].AsUint64()
			ics.types.unsign.min = nmin
		} else {
			var nmin = row[0].AsInt64()
			ics.types.sign.min = nmin
		}
	}
	return

}

func update_integer_max(o *OptionEntries, conn *client.Conn, dbt *db_table, csi *chunk_step_item) {
	var ics *integer_step = csi.chunk_step.integer_step
	var query, cache string
	var minmax *mysql.Result
	var row []mysql.FieldValue
	var err error
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where string = ""
	update_integer_where_on_gstring(o, &where, false, csi.prefix, csi.field, csi.chunk_step.integer_step.is_unsigned, csi.chunk_step.integer_step.types, false)
	query = fmt.Sprintf("SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s DESC LIMIT 1",
		cache,
		o.global.identifier_quote_character_str, csi.field, o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, dbt.table,
		o.global.identifier_quote_character_str,
		where,
		o.global.identifier_quote_character_str, csi.field, o.global.identifier_quote_character_str)
	minmax, err = conn.Execute(query)
	if err != nil {
		return
	}
	if len(minmax.Values) == 0 {
		if ics.is_unsigned {
			ics.types.unsign.max = ics.types.unsign.min
		} else {
			ics.types.sign.max = ics.types.sign.min
		}
		return
	}
	for _, row = range minmax.Values {
		if ics.is_unsigned {
			var nmax uint64 = row[0].AsUint64()
			ics.types.unsign.max = nmax
		} else {
			var nmax int64 = row[0].AsInt64()
			ics.types.sign.max = nmax
		}
	}
	return
}

func process_integer_chunk_step(o *OptionEntries, tj *table_job, csi *chunk_step_item) bool {
	var td *thread_data = tj.td
	var cs *chunk_step = csi.chunk_step
	check_pause_resume(td)
	if o.global.shutdown_triggered {
		return true
	}
	csi.mutex.Lock()
	csi.status = DUMPING_CHUNK
	if cs.integer_step.check_max {
		cs.integer_step.check_max = false
	}
	if cs.integer_step.check_min {
		cs.integer_step.check_min = false
	}
	if cs.integer_step.is_unsigned {
		if cs.integer_step.step-1 > cs.integer_step.types.unsign.max-cs.integer_step.types.unsign.min {
			cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.max
		} else {
			cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.min + cs.integer_step.step - 1
		}
		if cs.integer_step.step > 0 {
			cs.integer_step.estimated_remaining_steps = (cs.integer_step.types.unsign.max - cs.integer_step.types.unsign.cursor) / cs.integer_step.step
		} else {
			cs.integer_step.estimated_remaining_steps = 1
		}
	} else {
		if cs.integer_step.step-1 > gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.min) {
			cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.max
		} else {
			cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.min + int64(cs.integer_step.step) - 1
		}
		if cs.integer_step.step > 0 {
			cs.integer_step.estimated_remaining_steps = (uint64(cs.integer_step.types.sign.max) - uint64(cs.integer_step.types.sign.cursor)) / cs.integer_step.step
		} else {
			cs.integer_step.estimated_remaining_steps = 1
		}
	}
	csi.mutex.Unlock()
	update_estimated_remaining_chunks_on_dbt(tj.dbt)
	update_where_on_integer_step(o, csi)
	if csi.next != nil {
		if csi.next.needs_refresh {
			refresh_integer_min_max(o, td.thrconn, tj.dbt, csi.next)
		}
		csi.next.chunk_functions.process(o, tj, csi.next)
		csi.next.needs_refresh = true
	} else {
		tj.where = ""
		tj.where += csi.where
		if cs.integer_step.is_step_fixed_length {
			write_table_job_into_file(o, tj)
		} else {
			var from = time.Now()
			write_table_job_into_file(o, tj)
			var to = time.Now()
			var diff = to.Sub(from).Seconds()
			if diff > 2 {
				cs.integer_step.step = cs.integer_step.step / 2
				if cs.integer_step.step < csi.chunk_step.integer_step.min_chunk_step_size {
					cs.integer_step.step = csi.chunk_step.integer_step.min_chunk_step_size
				}
			}
		}
	}
	csi.mutex.Lock()
	if csi.status != COMPLETED {
		csi.status = ASSIGNED
	}
	if cs.integer_step.is_unsigned {
		cs.integer_step.types.unsign.min = cs.integer_step.types.unsign.cursor + 1
	} else {
		cs.integer_step.types.sign.min = cs.integer_step.types.sign.cursor + 1
	}
	csi.mutex.Unlock()
	return false
}

func process_integer_chunk(o *OptionEntries, tj *table_job, csi *chunk_step_item) {
	var td = tj.td
	var dbt = tj.dbt
	var cs *chunk_step = csi.chunk_step
	var multicolumn_process bool
	if csi.next == nil && dbt.multicolumn && uint(len(dbt.primary_key))-1 > csi.position {
		csi.where = ""
		update_integer_where_on_gstring(o, &csi.where, csi.include_null, csi.prefix, csi.field, csi.chunk_step.integer_step.is_unsigned, csi.chunk_step.integer_step.types, false)
		var rows uint64 = get_rows_from_explain(o, td.thrconn, tj.dbt, csi.where, csi.field)
		if rows > csi.chunk_step.integer_step.min_chunk_step_size {
			var next_csi *chunk_step_item = initialize_chunk_step_item(o, td.thrconn, dbt, csi.position+1, csi.where, rows)
			if next_csi != nil {
				if next_csi.chunk_type != NONE {
					csi.next = next_csi
					multicolumn_process = true
				}
			}
		}
	}
	tj.where = ""
	if process_integer_chunk_step(o, tj, csi) {
		log.Infof("Thread %d: Job has been cacelled", td.thread_id)
		return
	}
	atomic.AddInt64(&dbt.chunks_completed, 1)
	csi.include_null = false
	csi.mutex.Lock()
	for (cs.integer_step.is_unsigned && cs.integer_step.types.unsign.min <= cs.integer_step.types.unsign.max) || (!cs.integer_step.is_unsigned && cs.integer_step.types.sign.min <= cs.integer_step.types.sign.max) {
		csi.mutex.Lock()
		tj.where = ""
		if process_integer_chunk_step(o, tj, csi) {
			log.Infof("Thread %d: Job has been cacelled", td.thread_id)
			return
		}
		atomic.AddInt64(&dbt.chunks_completed, 1)
		csi.mutex.Lock()
	}
	csi.mutex.Unlock()
	csi.mutex.Lock()
	if csi.position == 0 {
		cs.integer_step.estimated_remaining_steps = 0
	}
	csi.status = COMPLETED
	csi.mutex.Unlock()
	if multicolumn_process {
		free_integer_step_item(csi.next)
		csi.next = nil
	}
}

func update_integer_where_on_gstring(o *OptionEntries, where *string, include_null bool, prefix string, field string, is_unsigned bool, types *int_types, use_cursor bool) {
	var t *int_types = new(int_types)
	t.sign = new(signed_int)
	t.unsign = new(unsigned_int)
	if prefix != "" && len(prefix) > 0 {
		*where += fmt.Sprintf("(%s AND ", prefix)
	}
	if include_null {
		*where += fmt.Sprintf("(%s%s%s IS NULL OR", o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str)
	}
	*where += "("
	if is_unsigned {
		t.unsign.min = types.unsign.min
		if !use_cursor {
			t.unsign.cursor = types.unsign.max
		} else {
			t.unsign.cursor = types.unsign.cursor
		}
		if t.unsign.min == t.unsign.cursor {
			*where += fmt.Sprintf("%s%s%s = %d", o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str, t.unsign.cursor)
		} else {
			*where += fmt.Sprintf("%d <= %s%s%s AND %s%s%s <= %d", t.unsign.min,
				o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str,
				t.unsign.cursor)
		}
	} else {
		t.sign.min = types.sign.min
		if !use_cursor {
			t.sign.cursor = types.sign.max
		} else {
			t.sign.cursor = types.sign.cursor
		}
		if t.sign.min == t.sign.cursor {
			*where += fmt.Sprintf("%s%s%s = %d", o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str, t.sign.cursor)
		} else {
			*where += fmt.Sprintf("%d <= %s%s%s AND %s%s%s <= %d",
				t.sign.min,
				o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str,
				t.sign.cursor)
		}
	}
	if include_null {
		*where += ")"
	}
	*where += ")"
	if prefix != "" {
		*where += ")"
	}
}

func update_where_on_integer_step(o *OptionEntries, csi *chunk_step_item) {
	var chunk_step *integer_step = csi.chunk_step.integer_step
	csi.where = ""
	update_integer_where_on_gstring(o, &csi.where, csi.include_null, csi.prefix, csi.field, chunk_step.is_unsigned, chunk_step.types, true)
}
