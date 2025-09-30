package mydumper

import (
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"math"
	"sync/atomic"
	"time"
)

const MAX_TIME_PER_QUERY = 2

var (
	MaxTimePerSelect            int = MAX_TIME_PER_QUERY
	min_integer_chunk_step_size uint64
	max_integer_chunk_step_size uint64
)

func gint64_abs(a int64) uint64 {
	if a >= 0 {
		return uint64(a)
	}
	return uint64(-a)
}
func initialize_integer_step(cs *chunk_step, is_unsigned bool, types *int_types, is_step_fixed_length bool,
	step uint64, min_css uint64, max_css uint64, check_min bool, check_max bool, rows_in_explain uint64) {
	cs.integer_step.is_unsigned = is_unsigned
	cs.integer_step.min_chunk_step_size = min_css
	cs.integer_step.max_chunk_step_size = max_css
	if cs.integer_step.is_unsigned {
		cs.integer_step.types.unsign.min = types.unsign.min
		cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.min
		cs.integer_step.types.unsign.max = types.unsign.max
		cs.integer_step.step = step
		cs.integer_step.estimated_remaining_steps = (cs.integer_step.types.unsign.max - cs.integer_step.types.unsign.min) / cs.integer_step.step
	} else {
		cs.integer_step.types.sign.min = types.sign.min
		cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.min
		cs.integer_step.types.sign.max = types.sign.max
		cs.integer_step.step = step
		cs.integer_step.estimated_remaining_steps = uint64((cs.integer_step.types.sign.max - cs.integer_step.types.sign.min) / int64(cs.integer_step.step))
	}
	cs.integer_step.is_step_fixed_length = is_step_fixed_length
	cs.integer_step.check_max = check_max
	cs.integer_step.check_min = check_min
	cs.integer_step.rows_in_explain = rows_in_explain
}

func new_integer_step(is_unsigned bool, types *int_types, is_step_fixed_length bool, step uint64, min_css uint64, max_css uint64, check_min bool, check_max bool, rows_in_explain uint64) *chunk_step {
	var cs = new(chunk_step)
	cs.integer_step = new(integer_step)
	cs.integer_step.types = new(int_types)
	cs.integer_step.types.unsign = new(unsigned_int)
	cs.integer_step.types.sign = new(signed_int)
	initialize_integer_step(cs, is_unsigned, types, is_step_fixed_length, step, min_css, max_css, check_min, check_max, rows_in_explain)
	return cs
}

func initialize_integer_step_item(csi *chunk_step_item, include_null bool, prefix *GString, field string, is_unsigned bool,
	types *int_types, deep uint, is_step_fixed_length bool, step uint64, min_css uint64, max_css uint64,
	part uint64, check_min bool, check_max bool, next *chunk_step_item, position uint, multicolumn bool, rows_in_explain uint64) {
	csi.chunk_step = new_integer_step(is_unsigned, types, is_step_fixed_length, step, min_css, max_css, check_min, check_max, rows_in_explain)
	csi.chunk_type = INTEGER
	csi.position = position
	csi.next = next
	csi.status = UNASSIGNED
	csi.chunk_functions.process = process_integer_chunk
	csi.chunk_functions.free = free_integer_step_item
	csi.chunk_functions.get_next = get_next_integer_chunk
	csi.where = nil
	csi.include_null = include_null
	csi.prefix = prefix
	csi.field = field
	csi.mutex = G_mutex_new()
	csi.part = part
	csi.deep = deep
	csi.needs_refresh = false
	csi.multicolumn = multicolumn
}

func new_integer_step_item(include_null bool, prefix *GString, field string, is_unsigned bool,
	types *int_types, deep uint, is_step_fixed_length bool, step uint64, min_css uint64, max_css uint64,
	part uint64, check_min bool, check_max bool, next *chunk_step_item, position uint, multicolumn bool, rows_in_explain uint64) *chunk_step_item {
	var csi = new(chunk_step_item)
	csi.chunk_functions = new(chunk_functions)
	csi.chunk_step = new(chunk_step)
	initialize_integer_step_item(csi, include_null, prefix, field, is_unsigned, types, deep, is_step_fixed_length, step, min_css, max_css, part, check_min, check_max, next, position, multicolumn, rows_in_explain)
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

func print_type(types *int_types, is_unsigned bool) {
	if is_unsigned {
		log.Infof("new_integer_step_item: min: %d | max: %d", types.unsign.min, types.unsign.max)
	} else {
		log.Infof("new_integer_step_item: min: %d | max: %d", types.sign.min, types.sign.max)
	}
}

func split_chunk_step(csi *chunk_step_item) *chunk_step_item {
	var new_csi *chunk_step_item
	var part uint64 = csi.part
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
			new_minmax_unsigned = (types.unsign.min/ics.step)*ics.step + ics.step*
				((((ics.types.unsign.max/ics.step)-
					(types.unsign.min/ics.step))/2)+1)
			if (types.unsign.min / ics.step) == (new_minmax_unsigned / ics.step) {
				return nil
			}
			if new_minmax_unsigned == types.unsign.min {
				return nil
			}
			types.unsign.min = new_minmax_unsigned
			part = types.unsign.min/csi.chunk_step.integer_step.step + 1
		} else {

			new_minmax_signed = (types.sign.min/int64(ics.step))*int64(ics.step) + int64(ics.step)*((((ics.types.sign.max/int64(ics.step))-(types.sign.min/int64(ics.step)))/2)+1)
			if types.sign.min/int64(ics.step) == new_minmax_signed/int64(ics.step) {
				return nil
			}
			if new_minmax_signed == types.sign.min {
				return nil
			}
			types.sign.min = new_minmax_signed
			part = uint64(types.sign.min)/csi.chunk_step.integer_step.step + 1
		}

	} else {
		part += uint64(math.Pow(2, float64(csi.deep)))
		if ics.is_unsigned {
			new_minmax_unsigned = types.unsign.min + ics.types.unsign.max/2 - types.unsign.min/2
			if new_minmax_unsigned == types.unsign.min {
				new_minmax_unsigned++
			}
			types.unsign.min = new_minmax_unsigned
		} else {
			new_minmax_signed = types.sign.min + ics.types.sign.max/2 - types.sign.min/2
			if new_minmax_signed == types.sign.min {
				new_minmax_signed++
			}
			types.sign.min = new_minmax_signed
			log.Tracef("Signed Chunk split like this: min: %d | Mid: %d | max: %d", types.sign.min, new_minmax_signed, types.sign.max)
		}
	}

	new_csi = new_integer_step_item(false, nil, csi.field, csi.chunk_step.integer_step.is_unsigned, types, csi.deep+1, csi.chunk_step.integer_step.is_step_fixed_length, csi.chunk_step.integer_step.step, csi.chunk_step.integer_step.min_chunk_step_size, csi.chunk_step.integer_step.max_chunk_step_size, part, true, true /*csi.chunk_step.integer_step.check_max*/, nil, csi.position, csi.multicolumn, 0)
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
	return (!csi.chunk_step.integer_step.is_step_fixed_length && ((csi.chunk_step.integer_step.is_unsigned && (csi.chunk_step.integer_step.types.unsign.cursor < csi.chunk_step.integer_step.types.unsign.max &&
		((csi.status == DUMPING_CHUNK && (csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.cursor) >= csi.chunk_step.integer_step.step) ||
			(csi.status == ASSIGNED && (csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.min) >= csi.chunk_step.integer_step.step)))) ||
		(!csi.chunk_step.integer_step.is_unsigned && (csi.chunk_step.integer_step.types.sign.cursor < csi.chunk_step.integer_step.types.sign.max && ((csi.status == DUMPING_CHUNK &&
			gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.cursor) >= csi.chunk_step.integer_step.step) ||
			(csi.status == ASSIGNED && gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.min) >= csi.chunk_step.integer_step.step)))))) ||
		(csi.chunk_step.integer_step.is_step_fixed_length && csi.chunk_step.integer_step.step > 0 && ((csi.chunk_step.integer_step.is_unsigned && csi.chunk_step.integer_step.types.unsign.max/csi.chunk_step.integer_step.step > csi.chunk_step.integer_step.types.unsign.min/csi.chunk_step.integer_step.step+1) ||
			(!csi.chunk_step.integer_step.is_unsigned && csi.chunk_step.integer_step.types.sign.max/int64(csi.chunk_step.integer_step.step) > csi.chunk_step.integer_step.types.sign.min/int64(csi.chunk_step.integer_step.step)+1)))
}
func is_last_step(csi *chunk_step_item) bool {
	return !csi.chunk_step.integer_step.is_step_fixed_length && ((csi.chunk_step.integer_step.is_unsigned &&
		(csi.chunk_step.integer_step.types.unsign.cursor < csi.chunk_step.integer_step.types.unsign.max &&
			(csi.status == DUMPING_CHUNK && (csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.cursor) <= csi.chunk_step.integer_step.step))) ||
		(!csi.chunk_step.integer_step.is_unsigned && (csi.chunk_step.integer_step.types.sign.cursor < csi.chunk_step.integer_step.types.sign.max &&
			(csi.status == DUMPING_CHUNK && gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.cursor) <= csi.chunk_step.integer_step.step))))
}
func clone_chunk_step_item(csi *chunk_step_item) *chunk_step_item {
	return new_integer_step_item(csi.include_null, csi.prefix, csi.field, csi.chunk_step.integer_step.is_unsigned, csi.chunk_step.integer_step.types, csi.deep, csi.chunk_step.integer_step.is_step_fixed_length, csi.chunk_step.integer_step.step, csi.chunk_step.integer_step.min_chunk_step_size, csi.chunk_step.integer_step.max_chunk_step_size, csi.part, csi.chunk_step.integer_step.check_min, csi.chunk_step.integer_step.check_max, nil, csi.position, csi.multicolumn, 0)
}

func get_next_integer_chunk(dbt *db_table) *chunk_step_item {
	var csi, new_csi, new_csi_next *chunk_step_item
	if dbt.chunks != nil {
		csi = G_async_queue_try_pop(dbt.chunks_queue).(*chunk_step_item)
	}
	for csi != nil {
		csi.mutex.Lock()
		if csi.status == UNASSIGNED {
			csi.status = ASSIGNED
			G_async_queue_push(dbt.chunks_queue, csi)
			csi.mutex.Unlock()
			return csi
		}
		if csi.status == UNSPLITTABLE {
			if csi.next != nil {
				G_async_queue_push(dbt.chunks_queue, csi)
			}
			goto end
		}
		if csi.status == COMPLETED {
			goto end
		}
		if is_last_step(csi) {
			log.Tracef("Last chunk on step in `%s`.`%s` assigned", dbt.database.name, dbt.table)
			csi.status = UNASSIGNED
			csi.deep = csi.deep + 1
			new_csi = clone_chunk_step_item(csi)
			new_csi.status = ASSIGNED
			if csi.chunk_step.integer_step.is_unsigned {
				csi.chunk_step.integer_step.types.unsign.max = csi.chunk_step.integer_step.types.unsign.cursor
				new_csi.chunk_step.integer_step.types.unsign.min = csi.chunk_step.integer_step.types.unsign.cursor + 1
			} else {
				csi.chunk_step.integer_step.types.sign.max = csi.chunk_step.integer_step.types.sign.cursor
				new_csi.chunk_step.integer_step.types.sign.min = csi.chunk_step.integer_step.types.sign.cursor + 1
			}
			new_csi.part += uint64(math.Pow(2, float64(csi.deep)))
			update_where_on_integer_step(new_csi)
			dbt.chunks.PushBack(new_csi)
			csi.mutex.Unlock()
			return new_csi
		}
		if !is_splitable(csi) {
			if csi.multicolumn && csi.next != nil && csi.next.chunk_type == INTEGER {
				log.Tracef("Multicolumn table checking next")
				csi.next.mutex.Lock()
				if csi.next.status == UNSPLITTABLE || csi.next.status == COMPLETED {
					log.Tracef("Multicolumn table is not splittable: %d Ref: COMPLETED=%d", csi.next.status, COMPLETED)
					csi.status = UNSPLITTABLE
					csi.next.mutex.Unlock()
					goto end
				}
				if !is_splitable(csi.next) {
					log.Tracef("Multicolumn table is not splittable")
					csi.next.status = UNSPLITTABLE
					csi.next.mutex.Unlock()
					goto end
				}
				new_csi_next = split_chunk_step(csi.next)

				if new_csi_next != nil {
					log.Tracef("Multicolumn table is splittable")
					new_csi_next.multicolumn = false
					csi.deep = csi.deep + 1
					new_csi = clone_chunk_step_item(csi)
					new_csi.status = ASSIGNED
					//              if ( csi.chunk_step.integer_step.is_step_fixed_length ){
					new_csi.part += uint64(math.Pow(2, float64(csi.deep)))
					//              }
					update_where_on_integer_step(new_csi)

					new_csi.next = new_csi_next

					new_csi.next.prefix = new_csi.where
					dbt.chunks.PushBack(new_csi)
					G_async_queue_push(dbt.chunks_queue, csi)
					G_async_queue_push(dbt.chunks_queue, new_csi)
					csi.next.mutex.Unlock()
					csi.mutex.Unlock()
					return new_csi
				} else {
					log.Tracef("Multicolumn table: not able to split?")
				}
				csi.next.mutex.Unlock()
			}
			csi.status = UNSPLITTABLE
			goto end
		}
		new_csi = split_chunk_step(csi)
		if new_csi != nil {
			if new_csi.chunk_step.integer_step.is_unsigned {
				log.Tracef("Multicolumn table splited min: %d max: %d ", new_csi.chunk_step.integer_step.
					types.unsign.min, new_csi.chunk_step.integer_step.
					types.unsign.max)
			} else {
				log.Tracef("Multicolumn table splited min: %d max: %d ", new_csi.chunk_step.integer_step.
					types.sign.min, new_csi.chunk_step.integer_step.
					types.sign.max)
			}
			dbt.chunks.PushBack(new_csi)
			G_async_queue_push(dbt.chunks_queue, csi)
			G_async_queue_push(dbt.chunks_queue, new_csi)
			csi.mutex.Unlock()
			return new_csi
		}
	end:
		csi.mutex.Unlock()
		csi = G_async_queue_try_pop(dbt.chunks_queue).(*chunk_step_item)
	}

	return nil
}

func refresh_integer_min_max(conn *DBConnection, dbt *db_table, csi *chunk_step_item) bool {
	var ics *integer_step = csi.chunk_step.integer_step
	var query string
	var cache string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where_prefix string
	var prefix string
	if csi.prefix != nil {
		where_prefix = " WHERE "
		prefix = csi.prefix.Str.String()
	}
	query = fmt.Sprintf("SELECT %s MIN(%s%s%s),MAX(%s%s%s) FROM %s%s%s.%s%s%s%s%s", cache,
		Identifier_quote_character_str, csi.field, Identifier_quote_character_str, Identifier_quote_character_str, csi.field,
		Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str, Identifier_quote_character_str, dbt.table,
		Identifier_quote_character_str,
		where_prefix, prefix)
	var minmax *MYSQL_RES = M_store_result(conn, query, nil, "Query to get a new min and max failed")
	log.Tracef("refresh_integer_min_max: %s", query)
	if minmax == nil {
		return false
	}
	var row = Mysql_fetch_row(minmax)
	if row == nil || row[0].Value() == nil || row[1].Value() == nil {
		Mysql_free_result(minmax)
		return false
	}
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
	Mysql_free_result(minmax)
	return true

}

func update_integer_min(conn *DBConnection, dbt *db_table, csi *chunk_step_item) bool {
	var ics = csi.chunk_step.integer_step
	var query string
	var cache string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where *GString = get_where_from_csi(csi)
	query = fmt.Sprintf("SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s ASC LIMIT 1",
		cache,
		Identifier_quote_character_str, csi.field, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str,
		where.Str.String(),
		Identifier_quote_character_str, csi.field, Identifier_quote_character_str)

	var Min *MYSQL_RES = M_store_result(conn, query, nil, "Query to get a new min failed")
	if Min == nil {
		return false
	}
	var row = Mysql_fetch_row(Min)
	if row == nil || row[0].Value() == nil {
		Mysql_free_result(Min)
		return false
	}

	if ics.is_unsigned {
		var nmin = row[0].AsUint64()
		ics.types.unsign.min = nmin
	} else {
		var nmin = row[0].AsInt64()
		ics.types.sign.min = nmin
	}
	Mysql_free_result(Min)
	return true

}

func update_integer_max(conn *DBConnection, dbt *db_table, csi *chunk_step_item) bool {
	var ics *integer_step = csi.chunk_step.integer_step
	var query, cache string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where *GString = get_where_from_csi(csi)
	query = fmt.Sprintf("SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s DESC LIMIT 1",
		cache,
		Identifier_quote_character_str, csi.field, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str, Identifier_quote_character_str, dbt.table,
		Identifier_quote_character_str,
		where.Str.String(),
		Identifier_quote_character_str, csi.field, Identifier_quote_character_str)
	var Max *MYSQL_RES = M_store_result(conn, query, nil, "Query to get a new max failed")
	if Max == nil {
		if ics.is_unsigned {
			ics.types.unsign.max = ics.types.unsign.min
		} else {
			ics.types.sign.max = ics.types.sign.min
		}
		if Max != nil {
			Mysql_free_result(Max)
		}
		return false
	}
	var row = Mysql_fetch_row(Max)
	if row == nil || row[0].Value() == nil {
		if ics.is_unsigned {
			ics.types.unsign.max = ics.types.unsign.min
		} else {
			ics.types.sign.max = ics.types.sign.min
		}
		if Max != nil {
			Mysql_free_result(Max)
		}
		return false
	}
	if ics.is_unsigned {
		var nmax uint64 = row[0].AsUint64()
		ics.types.unsign.max = nmax
	} else {
		var nmax int64 = row[0].AsInt64()
		ics.types.sign.max = nmax
	}
	Mysql_free_result(Max)
	return true
}

func is_last(csi *chunk_step_item) bool {
	csi.mutex.Lock()
	var r bool
	if csi.chunk_step.integer_step.is_unsigned {
		r = csi.chunk_step.integer_step.types.unsign.cursor == csi.chunk_step.integer_step.types.unsign.max
	} else {
		r = csi.chunk_step.integer_step.types.sign.cursor >= csi.chunk_step.integer_step.types.sign.max
	}
	csi.mutex.Unlock()
	return r
}
func process_integer_chunk_step(tj *table_job, csi *chunk_step_item) uint {
	var td *thread_data = tj.td
	var cs *chunk_step = csi.chunk_step

	check_pause_resume(td)
	if shutdown_triggered {
		return 1
	}

	// Stage 1: Update min and max if needed

	csi.mutex.Lock()
	//  if (tj.status == COMPLETED)
	//    m_critical("Thread %d: Trying to process COMPLETED chunk",td.thread_id);
	csi.status = DUMPING_CHUNK

	var c_min bool = true
	var c_max bool = true

	if !cs.integer_step.is_step_fixed_length {
		if cs.integer_step.check_max && !cs.integer_step.is_step_fixed_length {
			log.Tracef("Thread %d: I-Chunk 1: Updating MAX", td.thread_id)
			if cs.integer_step.is_unsigned {
				log.Tracef("Thread %d: I-Chunk 1: Updating MAX: %d", td.thread_id, cs.integer_step.types.unsign.max)
			} else {
				log.Tracef("Thread %d: I-Chunk 1: Updating MAX: %d", td.thread_id, cs.integer_step.types.sign.max)
			}
			c_max = update_integer_max(td.thrconn, tj.dbt, csi)
			if cs.integer_step.is_unsigned {
				log.Tracef("Thread %d: I-Chunk 1: New MAX: %d", td.thread_id, cs.integer_step.types.unsign.max)
			} else {
				log.Tracef("Thread %d: I-Chunk 1: New MAX: %d", td.thread_id, cs.integer_step.types.sign.max)
			}
			cs.integer_step.check_max = false
		}
		if cs.integer_step.check_min && !cs.integer_step.is_step_fixed_length {
			if cs.integer_step.is_unsigned {
				log.Tracef("Thread %d: I-Chunk 1: Updating MIN: %d", td.thread_id, cs.integer_step.types.unsign.min)
			} else {
				log.Tracef("Thread %d: I-Chunk 1: Updating MIN: %d", td.thread_id, cs.integer_step.types.sign.min)
			}
			c_min = update_integer_min(td.thrconn, tj.dbt, csi)
			if cs.integer_step.is_unsigned {
				log.Tracef("Thread %d: I-Chunk 1: New MIN: %d", td.thread_id, cs.integer_step.types.unsign.min)
			} else {
				log.Tracef("Thread %d: I-Chunk 1: New MIN: %d", td.thread_id, cs.integer_step.types.sign.min)
			}
			cs.integer_step.check_min = false
		}
		if !c_min && !c_max {
			log.Tracef("Thread %d: I-Chunk 1: both min and max doesn't exists", td.thread_id)
			close_files(tj)
			csi.mutex.Unlock()
			goto update_min
			//    goto end_process;
		}

		if cs.integer_step.rows_in_explain == 0 {
			var _where = get_where_from_csi(csi)
			cs.integer_step.rows_in_explain = get_rows_from_explain(td.thrconn, tj.dbt, _where, csi.field)
			log.Tracef("Thread %d: I-Chunk 1: We calculated rows %d for %s", td.thread_id, cs.integer_step.rows_in_explain, _where.Str.String())
		}
	}

	// Stage 2: Setting cursor
	if tj.dbt.multicolumn && csi.multicolumn && csi.next == nil && !cs.integer_step.is_step_fixed_length {
		var integer_step_step uint64 = cs.integer_step.step
	retry:
		// We are setting cursor to build the WHERE clause for the EXPLAIN
		if cs.integer_step.is_unsigned {
			if integer_step_step > cs.integer_step.types.unsign.max-cs.integer_step.types.unsign.min+1 {
				cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.max
			} else {
				cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.min + integer_step_step - 1
			}
			log.Tracef("Thread %d: I-Chunk 2: cs.integer_step.types.unsign.cursor: %d", td.thread_id, cs.integer_step.types.unsign.cursor)
		} else {
			if integer_step_step > gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.min)+1 {
				cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.max
			} else {
				cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.min + int64(integer_step_step) - 1
			}
			log.Tracef("Thread %d: I-Chunk 2: cs.integer_step.types.sign.cursor: %lld", td.thread_id, cs.integer_step.types.sign.cursor)
		}
		update_where_on_integer_step(csi)
		var rows uint64
		if CheckRowCount {
			rows = get_rows_from_count(td.thrconn, tj.dbt, csi.where)
		} else {
			rows = get_rows_from_explain(td.thrconn, tj.dbt, csi.where, csi.field)
		}
		log.Tracef("Thread %d: I-Chunk 2: multicolumn and next == NULL with rows: %d", td.thread_id, rows)

		var tmpstep uint64
		if csi.chunk_step.integer_step.is_unsigned {
			tmpstep = csi.chunk_step.integer_step.types.unsign.cursor - csi.chunk_step.integer_step.types.unsign.min
		} else {
			tmpstep = gint64_abs(csi.chunk_step.integer_step.types.sign.cursor - csi.chunk_step.integer_step.types.sign.min)
		}
		tmpstep++
		if integer_step_step > tmpstep {
			integer_step_step = tmpstep
		}
		if integer_step_step > 1 {
			// rows / num_threads > integer_step_step
			if rows > tj.dbt.min_chunk_step_size && (rows > cs.integer_step.step || (tj.num_rows_of_last_run > 0 && rows/100 > tj.num_rows_of_last_run)) {
				log.Tracef("Thread %d: I-Chunk 2: integer_step.step>1 then retrying", td.thread_id)
				integer_step_step = integer_step_step / 2
				goto retry
			}
			log.Tracef("Thread %d: I-Chunk 2: integer_step.step>1 not retrying as rows %d <=  step %d and integer_step_step: %d", td.thread_id, rows, cs.integer_step.step, integer_step_step)
			cs.integer_step.step = integer_step_step
		} else {
			// at this poing cs.integer_step.step == 1 always
			cs.integer_step.step = 1
			if cs.integer_step.is_unsigned {
				log.Tracef("Thread %d: I-Chunk 2: integer_step.step==1 min: %d | max: %d", td.thread_id, csi.chunk_step.integer_step.types.unsign.min, csi.chunk_step.integer_step.types.unsign.max)
			} else {
				log.Tracef("Thread %d: I-Chunk 2: integer_step.step==1 min: %d | max: %d", td.thread_id, csi.chunk_step.integer_step.types.sign.min, csi.chunk_step.integer_step.types.sign.max)
			}
			if rows > tj.dbt.min_chunk_step_size {
				csi.next = initialize_chunk_step_item(td.thrconn, tj.dbt, csi.position+1, rows, csi.where)
				if csi.next != nil {
					csi.next.multicolumn = false
					log.Tracef("Thread %d: I-Chunk 2: New next with where %s | rows: %d", td.thread_id, csi.where.Str.String(), rows)
				}
			} else {
				log.Tracef("Thread %d: I-Chunk 2: multicolumn=FALSE", td.thread_id)
				csi.multicolumn = false
			}
		}
	}

	if cs.integer_step.is_unsigned {

		if cs.integer_step.step > cs.integer_step.types.unsign.max-cs.integer_step.types.unsign.min+1 {
			cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.max
		} else {
			cs.integer_step.types.unsign.cursor = cs.integer_step.types.unsign.min + cs.integer_step.step - 1
		}
		if cs.integer_step.types.unsign.cursor < cs.integer_step.types.unsign.min {
			log.Errorf("Thread %d: integer_step.types.unsign.cursor: %d  | integer_step.types.unsign.min %d  | cs.integer_step.types.unsign.max : %d | cs.integer_step.step %d", td.thread_id, cs.integer_step.types.unsign.cursor, cs.integer_step.types.unsign.min, cs.integer_step.types.unsign.max, cs.integer_step.step)
		}
		G_assert(cs.integer_step.types.unsign.cursor >= cs.integer_step.types.unsign.min)

		if cs.integer_step.step > 0 {
			cs.integer_step.estimated_remaining_steps = cs.integer_step.types.unsign.max - cs.integer_step.types.unsign.cursor/cs.integer_step.step
		} else {
			cs.integer_step.estimated_remaining_steps = 1
		}
	} else {

		if cs.integer_step.step > gint64_abs(cs.integer_step.types.sign.max-cs.integer_step.types.sign.min)+1 {
			cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.max
		} else {
			cs.integer_step.types.sign.cursor = cs.integer_step.types.sign.min + int64(cs.integer_step.step) - 1
		}
		if cs.integer_step.types.sign.cursor < cs.integer_step.types.sign.min {
			log.Errorf("Thread %d: integer_step.types.sign.cursor: %d  | integer_step.types.sign.min %d  | cs.integer_step.types.sign.max : %d | cs.integer_step.step %d", td.thread_id, cs.integer_step.types.sign.cursor, cs.integer_step.types.sign.min, cs.integer_step.types.sign.max, cs.integer_step.step)
		}
		G_assert(cs.integer_step.types.sign.cursor >= cs.integer_step.types.sign.min)
		if cs.integer_step.step > 0 {
			cs.integer_step.estimated_remaining_steps = (uint64(cs.integer_step.types.sign.max) - uint64(cs.integer_step.types.sign.cursor)) / cs.integer_step.step
		} else {
			cs.integer_step.estimated_remaining_steps = 1
		}
	}

	if csi.next != nil && csi.status == UNSPLITTABLE {
		// Could be possible that in previous iteration on a multicolumn table, the status changed ot UNSPLITTABLE, but on next iteration could be possible
		// to splittable, that is why we need to change back to ASSIGNED
		csi.status = ASSIGNED
	}

	csi.mutex.Unlock()

	update_estimated_remaining_chunks_on_dbt(tj.dbt)

	// Step 3: Executing query and writing data
	update_where_on_integer_step(csi)
	if csi.prefix != nil {
		log.Tracef("Thread %d: I-Chunk 3: PREFIX: %s WHERE: %s", td.thread_id, csi.prefix.Str.String(), csi.where.Str.String())
	} else {
		log.Tracef("Thread %d: I-Chunk 3: WHERE: %s", td.thread_id, csi.where.Str.String())
	}
	if csi.next != nil {
		log.Tracef("Thread %d: I-Chunk 3: going down", td.thread_id)
		// Multi column
		if csi.next.needs_refresh {
			if !refresh_integer_min_max(td.thrconn, tj.dbt, csi.next) {
				log.Tracef("Thread %d: I-Chunk 3: No min and max found", td.thread_id)
				goto update_min
			}
		}
		csi.next.chunk_functions.process(tj, csi.next)
		csi.next.needs_refresh = true
	} else {
		G_string_set_size(tj.where, 0)
		G_string_append(tj.where, csi.where.Str.String())
		log.Tracef("Thread %d: I-Chunk 3: WHERE in TJ: %s", td.thread_id, tj.where.Str.String())
		if cs.integer_step.is_step_fixed_length {
			if cs.integer_step.is_unsigned {
				tj.part = cs.integer_step.types.unsign.min/cs.integer_step.step + 1
			} else {
				tj.part = uint64(cs.integer_step.types.sign.min)/cs.integer_step.step + 1
			}
			close_files(tj)
			write_table_job_into_file(tj)
		} else if is_last(csi) {
			log.Tracef("Thread %d: I-Chunk 3: Last chunk on `%s`.`%s` no need to calculate anything else after finish", td.thread_id, tj.dbt.database.name, tj.dbt.table)
			write_table_job_into_file(tj)
		} else {
			var from = time.Now()
			write_table_job_into_file(tj)
			var to = time.Now()

			// Step 3.1: Updating Step length

			var diff = to.Sub(from)
			csi.mutex.Lock()

			// Let's calculate last run
			//
			// and we also going to calculate the average
			//
			// if last_run is above, then we use it
			// if it is not, we use the average
			//      cs.integer_step.rows_in_explain
			//      tj.num_rows_of_last_run

			if cs.integer_step.rows_in_explain > tj.num_rows_of_last_run {
				cs.integer_step.rows_in_explain -= tj.num_rows_of_last_run
			} else {
				cs.integer_step.rows_in_explain = 0
			}
			if diff > 0 && tj.num_rows_of_last_run > 0 {
				cs.integer_step.step = tj.num_rows_of_last_run * uint64(max_time_per_select) * uint64(diff.Seconds())
				log.Tracef("Thread %d: I-Chunk 3: Step size on `%s`.`%s` is %d  ( %d %d)", td.thread_id, tj.dbt.database.name, tj.dbt.table, cs.integer_step.step, tj.num_rows_of_last_run, diff)
			} else {
				cs.integer_step.step *= 2
				cs.integer_step.check_min = true
				log.Tracef("Thread %d: I-Chunk 3: During last query we get zero rows, duplicating the step size to %d", td.thread_id, cs.integer_step.step)
			}

			if csi.chunk_step.integer_step.max_chunk_step_size != 0 && cs.integer_step.step > csi.chunk_step.integer_step.max_chunk_step_size {
				cs.integer_step.step = csi.chunk_step.integer_step.max_chunk_step_size
			}
			if csi.chunk_step.integer_step.min_chunk_step_size != 0 && cs.integer_step.step < csi.chunk_step.integer_step.min_chunk_step_size {
				cs.integer_step.step = csi.chunk_step.integer_step.min_chunk_step_size
			}

			//      trace("After checking: %ld == %ld | max_integer_chunk_step_size=%ld | min_integer_chunk_step_size=%ld", ant, cs.integer_step.step, max_integer_chunk_step_size, min_integer_chunk_step_size);
			csi.mutex.Unlock()
		}
	}

	// Step 5: Updating min
update_min:
	csi.mutex.Lock()
	if csi.status != COMPLETED {
		csi.status = ASSIGNED
	}
	if cs.integer_step.is_unsigned {
		if cs.integer_step.types.unsign.cursor+1 < cs.integer_step.types.unsign.min {
			// Overflow
			log.Tracef("Thread %d: I-Chunk 5: Overflow due integer_step.types.unsign.cursor: %d  | integer_step.types.unsign.min %d", td.thread_id, cs.integer_step.types.unsign.cursor, cs.integer_step.types.unsign.min)
			cs.integer_step.types.unsign.min = cs.integer_step.types.unsign.max
			cs.integer_step.types.unsign.max--
		} else {
			cs.integer_step.types.unsign.min = cs.integer_step.types.unsign.cursor + 1
		}

	} else {
		if cs.integer_step.types.sign.cursor+1 < cs.integer_step.types.sign.min {
			log.Tracef("Thread %d: I-Chunk 5: Overflow due integer_step.types.unsign.cursor: %d  | integer_step.types.unsign.min %d", td.thread_id, cs.integer_step.types.sign.cursor, cs.integer_step.types.sign.min)
			cs.integer_step.types.sign.min = cs.integer_step.types.sign.max
			cs.integer_step.types.sign.max--
		} else {
			cs.integer_step.types.sign.min = cs.integer_step.types.sign.cursor + 1
		}

	}
	var tmpstep uint64
	if csi.chunk_step.integer_step.is_unsigned {
		tmpstep = csi.chunk_step.integer_step.types.unsign.max - csi.chunk_step.integer_step.types.unsign.min
	} else {
		tmpstep = gint64_abs(csi.chunk_step.integer_step.types.sign.max - csi.chunk_step.integer_step.types.sign.min)
	}
	tmpstep++

	log.Tracef("Thread %d: I-Chunk 5: integer_step.types.sign.cursor: %d  | integer_step.types.sign.min %d | cs.integer_step.types.sign.max : %d | cs.integer_step.step %d | tmpstep: %d", td.thread_id, cs.integer_step.types.sign.cursor, cs.integer_step.types.sign.min, cs.integer_step.types.sign.max, cs.integer_step.step, tmpstep)

	if cs.integer_step.step > tmpstep {
		cs.integer_step.step = tmpstep
	}

	//  g_message("Thread %d: I-Chunk 5: integer_step.types.sign.cursor: %"G_GINT64_FORMAT"  | integer_step.types.sign.min %"G_GINT64_FORMAT"  | cs.integer_step.types.sign.max : %"G_GINT64_FORMAT" | cs.integer_step.step %ld", td.thread_id, cs.integer_step.types.sign.cursor, cs.integer_step.types.sign.min, cs.integer_step.types.sign.max, cs.integer_step.step);

	//end_process:

	if csi.position == 0 {
		csi.multicolumn = tj.dbt.multicolumn
	}
	if csi.next != nil {
		free_integer_step_item(csi.next)
		csi.next = nil
	}
	csi.mutex.Unlock()
	return 0
}

func process_integer_chunk(tj *table_job, csi *chunk_step_item) {
	var td = tj.td
	var dbt = tj.dbt
	var cs *chunk_step = csi.chunk_step
	G_string_set_size(tj.where, 0)
	if process_integer_chunk_step(tj, csi) != 0 {
		log.Infof("Thread %d: Job has been cacelled", td.thread_id)
		return
	}
	atomic.AddInt64(&dbt.chunks_completed, 1)
	csi.include_null = false
	csi.mutex.Lock()
	for (cs.integer_step.is_unsigned && cs.integer_step.types.unsign.min <= cs.integer_step.types.unsign.max) ||
		(!cs.integer_step.is_unsigned && cs.integer_step.types.sign.min <= cs.integer_step.types.sign.max) {
		csi.mutex.Unlock()
		G_string_set_size(tj.where, 0)
		if process_integer_chunk_step(tj, csi) != 0 {
			log.Infof("Thread %d: Job has been cacelled", td.thread_id)
			return
		}
		atomic.AddInt64(&dbt.chunks_completed, 1)
		csi.mutex.Lock()
	}
	if csi.position == 0 {
		cs.integer_step.estimated_remaining_steps = 0
	}
	csi.status = COMPLETED
	csi.mutex.Unlock()

}

func update_integer_where_on_gstring(where *GString, include_null bool, prefix *GString, field string, is_unsigned bool, types *int_types, use_cursor bool) {
	var t *int_types = new(int_types)
	t.sign = new(signed_int)
	t.unsign = new(unsigned_int)
	if prefix != nil && prefix.Len > 0 {
		G_string_append_printf(where, "(%s AND ", prefix.Str.String())
	}
	if include_null {
		G_string_append_printf(where, "(%s%s%s IS NULL OR", Identifier_quote_character_str, field, Identifier_quote_character_str)
	}
	G_string_append(where, "(")
	if is_unsigned {
		t.unsign.min = types.unsign.min
		if !use_cursor {
			t.unsign.cursor = types.unsign.max
		} else {
			t.unsign.cursor = types.unsign.cursor
		}
		if t.unsign.min == t.unsign.cursor {
			G_string_append_printf(where, "%s%s%s = %d", Identifier_quote_character_str, field, Identifier_quote_character_str, t.unsign.cursor)
		} else {
			G_string_append_printf(where, "%d <= %s%s%s AND %s%s%s <= %d", t.unsign.min,
				Identifier_quote_character_str, field, Identifier_quote_character_str, Identifier_quote_character_str, field, Identifier_quote_character_str,
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
			G_string_append_printf(where, "%s%s%s = %d", Identifier_quote_character_str, field, Identifier_quote_character_str, t.sign.cursor)
		} else {
			G_string_append_printf(where, "%d <= %s%s%s AND %s%s%s <= %d",
				t.sign.min,
				Identifier_quote_character_str, field, Identifier_quote_character_str, Identifier_quote_character_str, field, Identifier_quote_character_str,
				t.sign.cursor)
		}
	}
	if include_null {
		G_string_append(where, ")")
	}
	G_string_append(where, ")")
	if prefix != nil && prefix.Len > 0 {
		G_string_append(where, ")")
	}
}

func update_where_on_integer_step(csi *chunk_step_item) {
	var chunk_step *integer_step = csi.chunk_step.integer_step
	G_string_set_size(csi.where, 0)
	update_integer_where_on_gstring(csi.where, csi.include_null, csi.prefix, csi.field, chunk_step.is_unsigned, chunk_step.types, true)
}

func get_where_from_csi(csi *chunk_step_item) *GString {
	var where *GString = G_string_new("")
	update_integer_where_on_gstring(where, false, csi.prefix, csi.field, csi.chunk_step.integer_step.is_unsigned, csi.chunk_step.integer_step.types, false)
	return where
}

func determine_if_we_can_go_deeper(csi *chunk_step_item, rows uint64) {
	if csi.multicolumn && csi.position == 0 {
		if csi.chunk_step.integer_step.is_unsigned {
			log.Tracef("is_unsigned: %v | rows: %d | max - min: %d", csi.chunk_step.integer_step.is_unsigned, rows, csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.min)
		} else {
			log.Tracef("is_unsigned: %v | rows: %d | max - min: %d", csi.chunk_step.integer_step.is_unsigned, rows, gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.min))
		}
		// In a multi column table, we will use the first column to split the table.
		// This calculation will let us know how many rows are we getting on average per first column value
		// we need to have have at least 1 chunk size per first column to perform multi column spliting
		if (csi.chunk_step.integer_step.is_unsigned && (rows/(csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.min) > 1)) || (!csi.chunk_step.integer_step.is_unsigned && (rows/gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.min) > 1)) {
			csi.chunk_step.integer_step.min_chunk_step_size = 1
			csi.chunk_step.integer_step.is_step_fixed_length = true
			csi.chunk_step.integer_step.max_chunk_step_size = 1
			csi.chunk_step.integer_step.step = 1
		} else {
			csi.multicolumn = false
		}
	}
}
