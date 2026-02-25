package mydumper

import (
	"container/list"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"time"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

const MIN_CHUNK_STEP_SIZE = 1000

type chunk_step_item struct {
	chunk_step      *chunk_step
	chunk_type      chunk_type
	next            *chunk_step_item
	chunk_functions *chunk_functions
	multicolumn     bool
	where           *GString
	include_null    bool
	prefix          *GString
	field           string
	part            uint64
	deep            uint
	position        uint
	mutex           *sync.Mutex
	needs_refresh   bool
	status          chunk_states
}

var (
	give_me_another_transactional_chunk_step_queue     *GAsyncQueue
	give_me_another_non_transactional_chunk_step_queue *GAsyncQueue
	chunk_builder                                      *GThread
)

// initialize_chunk creates the transactional and non-transactional chunk request queues.
func initialize_chunk() {
	give_me_another_transactional_chunk_step_queue = G_async_queue_new()
	give_me_another_non_transactional_chunk_step_queue = G_async_queue_new()
}

// start_chunk_builder starts the chunk_builder_thread (unless NoData).
func start_chunk_builder(conf *Configuration) {
	if !NoData {
		chunk_builder = M_thread_new("chunk_builder", chunk_builder_thread, conf, "Chunk builder thread could not be created")
	}
}

// finalize_chunk unreferences chunk queues and joins the chunk_builder thread.
func finalize_chunk() {
	G_async_queue_unref(give_me_another_transactional_chunk_step_queue)
	G_async_queue_unref(give_me_another_non_transactional_chunk_step_queue)
	if !NoData {
		G_thread_join(chunk_builder)
	}
}

// process_none_chunk dumps the entire table in one shot via write_table_job_into_file (no chunking).
func process_none_chunk(tj *table_job, csi *chunk_step_item) {
	_ = csi
	write_table_job_into_file(tj)
}

// initialize_chunk_step_as_none sets csi to NONE chunk type with process_none_chunk.
func initialize_chunk_step_as_none(csi *chunk_step_item) {
	csi.part = 0
	csi.chunk_type = NONE
	csi.chunk_functions.process = process_none_chunk
	csi.chunk_functions.free = nil
	csi.chunk_step = nil
}

// new_none_chunk_step allocates a chunk_step_item configured as NONE (full table dump).
func new_none_chunk_step() *chunk_step_item {
	var csi *chunk_step_item = new(chunk_step_item)
	csi.chunk_functions = new(chunk_functions)
	initialize_chunk_step_as_none(csi)
	return csi
}

// initialize_chunk_step_item determines chunk type from MIN/MAX of the key column: returns integer step, char step, or NONE chunk.
func initialize_chunk_step_item(conn *DBConnection, dbt *db_table, position uint, rows uint64, prefix *GString) *chunk_step_item {
	var csi *chunk_step_item
	var query, cache string
	var field string
	if dbt.primary_key != nil || len(dbt.primary_key) > 0 {
		field = dbt.primary_key[position]
	} else {
		field = "(NULL)"
	}
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where_option, where_option_prefix string
	var prefix_option, prefix_option_prefix string
	if WhereOption != "" || prefix != nil {
		where_option = "WHERE"
	}
	if WhereOption != "" {
		where_option_prefix = WhereOption
	}
	if prefix != nil && prefix.Len > 0 {
		prefix_option = "AND"
		prefix_option_prefix = prefix.Str.String()
	}
	query = fmt.Sprintf("SELECT %s MIN(%s%s%s),MAX(%s%s%s),LEFT(MIN(%s%s%s),1),LEFT(MAX(%s%s%s),1) FROM %s%s%s.%s%s%s %s %s %s %s",
		cache,
		Identifier_quote_character_str, field, Identifier_quote_character_str, Identifier_quote_character_str, field, Identifier_quote_character_str,
		Identifier_quote_character_str, field, Identifier_quote_character_str, Identifier_quote_character_str, field, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str, Identifier_quote_character_str, dbt.table, Identifier_quote_character_str,
		where_option, where_option_prefix, prefix_option, prefix_option_prefix)
	var mr *M_ROW = M_store_result_row(conn, query, M_message, nil, "It is NONE with minmax == NULL")
	if mr.Res == nil || mr.Row == nil {
		M_store_result_row_free(mr)
		return new_none_chunk_step()
	}
	if mr.Row[0].Value() == nil {
		M_store_result_row_free(mr)
		log.Infof("It is NONE with minmax == NULL")
		return new_none_chunk_step()
	}
	var fields []*sql.ColumnType = Mysql_fetch_fields(mr.Res)
	var diff_btwn_max_min, unmin, unmax uint64
	var nmin, nmax int64
	// var lengths = minmax.Fields

	switch GetStandardType(fields[0].DatabaseTypeName()) {
	case "MYSQL_TYPE_TINY", "MYSQL_TYPE_SHORT", "MYSQL_TYPE_LONG", "MYSQL_TYPE_LONGLONG", "MYSQL_TYPE_INT24":
		log.Debugf("Integer PK found on `%s`.`%s`", dbt.database.name, dbt.table)
		unmin = mr.Row[0].AsUint64()
		unmax = mr.Row[1].AsUint64()
		nmin = mr.Row[0].AsInt64()
		nmax = mr.Row[1].AsInt64()
		var unsign bool = IsColumnUnsigned(fields[0])
		if unsign {
			diff_btwn_max_min = gint64_abs(int64(unmax - unmin))
		} else {
			diff_btwn_max_min = gint64_abs(nmax - nmin)
		}
		M_store_result_row_free(mr)

		if diff_btwn_max_min > dbt.min_chunk_step_size {
			var types *int_types = new(int_types)
			types.sign = new(signed_int)
			types.unsign = new(unsigned_int)
			if unsign {
				log.Tracef("Min: %d | Max %d", unmin, unmax)
				types.unsign.min = unmin
				types.unsign.max = unmax
			} else {
				log.Tracef("Min: %d | Max %d", nmin, nmax)
				types.sign.min = nmin
				types.sign.max = nmax
			}
			var _starting_chunk_step_size uint64
			var percentage_of_fragmentation uint64 = diff_btwn_max_min / rows
			log.Tracef("percentage_of_fragmentation of `%s`.`%s` %f", dbt.database.name, dbt.table, math.Log(float64(percentage_of_fragmentation)))
			if dbt.starting_chunk_step_size == 0 {
				if dbt.max_chunk_step_size != 0 {
					if rows/uint64(NumThreads) > dbt.max_chunk_step_size {
						_starting_chunk_step_size = dbt.max_chunk_step_size
					} else {
						_starting_chunk_step_size = uint64(float64(rows) / ((math.Log(float64(percentage_of_fragmentation)) + 1) * float64(NumThreads)))
					}
				} else {
					_starting_chunk_step_size = uint64(float64(rows) / ((math.Log(float64(percentage_of_fragmentation)) + 1) * float64(NumThreads)))
				}
				if dbt.max_chunk_step_size == 0 {
					max_chunk_step_size = uint64(float64(diff_btwn_max_min) / (math.Log(float64(percentage_of_fragmentation)+1) * float64(NumThreads)))
				}
			}
			if _starting_chunk_step_size < dbt.min_chunk_step_size {
				_starting_chunk_step_size = dbt.min_chunk_step_size
			}
			G_assert(_starting_chunk_step_size > 0)
			csi = new_integer_step_item(true, prefix, field, unsign, types, 0, dbt.is_fixed_length, _starting_chunk_step_size, dbt.min_chunk_step_size, dbt.max_chunk_step_size, 0, false, false, nil, position, dbt.multicolumn, rows)
			if csi.chunk_step.integer_step.is_step_fixed_length {
				if csi.chunk_step.integer_step.is_unsigned {
					csi.chunk_step.integer_step.types.unsign.min = (csi.chunk_step.integer_step.types.unsign.min / csi.chunk_step.integer_step.step) * csi.chunk_step.integer_step.step
				} else {
					csi.chunk_step.integer_step.types.sign.min = csi.chunk_step.integer_step.types.sign.min / int64(csi.chunk_step.integer_step.step) * int64(csi.chunk_step.integer_step.step)
				}
			}
			return csi
		} else {
			if position == 0 {
				log.Tracef("Integer PK on `%s`.`%s` performing full table scan", dbt.database.name, dbt.table)
				return new_none_chunk_step()
			}
		}
		break
	case "MYSQL_TYPE_STRING", "MYSQL_TYPE_VAR_STRING":
		log.Tracef("String type %d", position)
		M_store_result_row_free(mr)
		if position > 0 {
			dbt.multicolumn = false
		} else {
			return new_none_chunk_step()
		}
		break
	default:
		M_store_result_row_free(mr)
		log.Infof("It is NONE: default")
		if position > 0 {
			dbt.multicolumn = false
		} else {
			return new_none_chunk_step()
		}
		break
	}
	return nil
}

// get_rows_from_explain runs EXPLAIN on the table (with optional WHERE/field) and returns the rows estimate from the result.
func get_rows_from_explain(conn *DBConnection, dbt *db_table, where *GString, field string) uint64 {
	var query string
	var cache string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var q, field_column, where_column, where_column_opt string
	if field != "" {
		q = Identifier_quote_character_str
		field_column = field
	} else {
		field_column = "*"
	}
	if where != nil {
		where_column = " WHERE "
		where_column_opt = where.Str.String()
	}
	query = fmt.Sprintf("EXPLAIN SELECT %s %s%s%s FROM %s%s%s.%s%s%s%s%s", cache, q, field_column, q,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str, where_column, where_column_opt)
	log.Tracef("EXPLAIN: %s", query)
	var mr *M_ROW = M_store_result_row(conn, query, M_critical, M_warning, "Failed to execute EXPLAIN: %s", query)
	if mr.Res == nil || mr.Row == nil {
		M_store_result_row_free(mr)
		return 0
	}
	var row_col uint
	determine_explain_columns(mr.Res, &row_col)
	if mr.Row[row_col].Value() == nil {
		M_store_result_row_free(mr)
		return 0
	}
	var rows_in_explain uint64 = mr.Row[row_col].AsUint64()
	M_store_result_row_free(mr)
	return rows_in_explain
}

// get_rows_from_count runs SELECT COUNT(*) on the table (with optional WHERE) and returns the count.
func get_rows_from_count(conn *DBConnection, dbt *db_table, where *GString) uint64 {
	var cache, query string
	var whereOpt, whereKey string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	if where != nil {
		whereKey = " WHERE "
		whereOpt = where.Str.String()
	}
	query = fmt.Sprintf("SELECT %s COUNT(*) FROM %s%s%s.%s%s%s %s%s", cache,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str,
		whereKey, whereOpt)
	var mr *M_ROW = M_store_result_row(conn, query, M_critical, M_warning, "Failed to get count")
	if mr.Res == nil || mr.Row == nil || mr.Row[0].Value() == nil {
		M_store_result_row_free(mr)
		return 0
	}
	var rows uint64 = mr.Row[0].AsUint64()
	M_store_result_row_free(mr)
	return rows
}

// set_chunk_strategy_for_dbt gets row count (or estimate), then builds the initial chunk (partition, integer, or NONE) and enqueues it; sets dbt.status to READY.
func set_chunk_strategy_for_dbt(conn *DBConnection, dbt *db_table) {
	dbt.chunks_mutex.Lock()
	var csi *chunk_step_item
	var rows uint64
	if CheckRowCount {
		rows = get_rows_from_count(conn, dbt, nil)
		log.Infof("%s.%s has %s%d rows", dbt.database.name, dbt.table, "", rows)
	} else {
		rows = get_rows_from_explain(conn, dbt, nil, "")
		log.Infof("%s.%s has %s%d rows", dbt.database.name, dbt.table, "~", rows)
	}
	dbt.rows_total = rows
	if rows > dbt.min_chunk_step_size {
		var partitions []string
		if SplitPartitions || dbt.partition_regex != nil {
			partitions = get_partitions_for_table(conn, dbt)
		}
		if len(partitions) > 0 {
			csi = new_real_partition_step_item(partitions, 0, 0)
		} else {
			if dbt.split_integer_tables {
				csi = initialize_chunk_step_item(conn, dbt, 0, rows, nil)
			} else {
				csi = new_none_chunk_step()
			}
		}
	} else {
		csi = new_none_chunk_step()
	}
	dbt.chunks.PushBack(csi)
	G_async_queue_push(dbt.chunks_queue, csi)
	dbt.status = READY
	dbt.chunks_mutex.Unlock()
}

// get_next_dbt_and_chunk_step_item finds the next table and chunk step from dbt_list (DEFINING or READY with chunks), assigns to pointers, and returns whether any job is still defining.
func get_next_dbt_and_chunk_step_item(dbt_pointer **db_table, csi **chunk_step_item, dbt_list *MList) bool {
	var iter *list.Element
	var dbt *db_table
	var are_there_jobs_defining bool
	var lcs *chunk_step_item
	var finish bool
	var current_max_threads_running uint
	for current_max_threads_running = 0; current_max_threads_running < MaxThreadsPerTable && !finish; current_max_threads_running++ {
		dbt_list.mutex.Lock()
		iter = dbt_list.list.Front()
		for iter != nil && !finish {
			dbt = iter.Value.(*db_table)
			dbt.chunks_mutex.Lock()
			if dbt.status != DEFINING {
				if dbt.status == UNDEFINED {
					*dbt_pointer = dbt
					dbt.status = DEFINING
					are_there_jobs_defining = true
					dbt.chunks_mutex.Unlock()
					finish = true
					iter.Next()
					continue
				}
				G_assert(dbt.status == READY)
				if dbt.chunks == nil {
					dbt.chunks_mutex.Unlock()
					iter = iter.Next()
					continue
				}
				// Reading first chunk
				lcs = dbt.chunks.Front().Value.(*chunk_step_item)
				// If it is a full table scan, we assign it and exit
				if lcs.chunk_type == NONE {
					*dbt_pointer = iter.Value.(*db_table)
					*csi = lcs
					dbt_list.list.Remove(iter)
					dbt.chunks_mutex.Unlock()
					finish = true
					iter = iter.Next()
					continue
				}
				// if we reach the max limit of threads per table, we continue with next table
				if dbt.max_threads_per_table <= dbt.current_threads_running {
					dbt.chunks_mutex.Unlock()
					iter = iter.Next()
					continue
				}
				if dbt.current_threads_running > current_max_threads_running {
					dbt.chunks_mutex.Unlock()
					iter = iter.Next()
					continue
				}
				lcs = lcs.chunk_functions.get_next(dbt)
				if lcs != nil {
					dbt.current_threads_running++
					*dbt_pointer = dbt
					*csi = lcs
					dbt.chunks_mutex.Unlock()
					finish = true
					iter = iter.Next()
					continue
				} else {
					// If there is no more chunks on this table, we remove it from the list, and continue with the next table
					dbt_list.list.Remove(iter)
					iter = iter.Next()
					// Assign iter previous removing dbt from list is important as we might break the list
					dbt.chunks_mutex.Unlock()
					continue
				}
			} else {
				dbt.chunks_mutex.Unlock()
				are_there_jobs_defining = true
			}
		}
		dbt_list.mutex.Unlock()
	}

	return are_there_jobs_defining
}

// enqueue_shutdown_jobs pushes NumThreads JOB_SHUTDOWN jobs into the queue so workers can exit.
func enqueue_shutdown_jobs(queue *GAsyncQueue) {
	var n uint
	var j *job
	for n = 0; n < NumThreads; n++ {
		j = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(queue, j)
	}
}

// enqueue_shutdown sends shutdown jobs to both queue and deferQueue of the table_queuing.
func enqueue_shutdown(q *table_queuing) {
	enqueue_shutdown_jobs(q.queue)
	enqueue_shutdown_jobs(q.deferQueue)

}

// table_job_enqueue loops: waits for request_chunk, gets next dbt/csi via get_next_dbt_and_chunk_step_item, creates dump or determine_chunk_type jobs, then calls enqueue_shutdown when no more work.
func table_job_enqueue(q *table_queuing) {
	var dbt *db_table
	var csi *chunk_step_item
	var are_there_jobs_defining bool
	log.Infof("Starting to enqueue %s tables", q.descr)
	for {
		G_async_queue_pop(q.request_chunk)
		if shutdown_triggered {
			break
		}
		dbt = nil
		csi = nil
		are_there_jobs_defining = false
		are_there_jobs_defining = get_next_dbt_and_chunk_step_item(&dbt, &csi, q.table_list)
		if dbt != nil {
			if dbt.status == DEFINING {
				create_job_to_determine_chunk_type(dbt, G_async_queue_push, q.queue)
				continue
			}
			if csi != nil {
				if dbt.status == DEFINING {
					create_job_to_determine_chunk_type(dbt, G_async_queue_push, q.queue)
					continue
				}
				if csi != nil {
					switch csi.chunk_type {
					case INTEGER:
						if UseDefer {
							create_job_to_dump_chunk(dbt, "", csi.part, csi, G_async_queue_push, q.deferQueue)
							create_job_defer(dbt, q.queue)
						} else {
							create_job_to_dump_chunk(dbt, "", csi.part, csi, G_async_queue_push, q.queue)
						}
						break
					case CHAR:
						create_job_to_dump_chunk(dbt, "", csi.part, csi, G_async_queue_push, q.queue)
						break
					case PARTITION:
						create_job_to_dump_chunk(dbt, "", csi.part, csi, G_async_queue_push, q.queue)
						break
					case NONE:
						create_job_to_dump_chunk(dbt, "", csi.part, csi, G_async_queue_push, q.queue)
						break
					default:
						log.Errorf("This should not happen %v", csi.chunk_type)
						break
					}
				}
			}
		} else {
			if are_there_jobs_defining {
				G_async_queue_push(q.request_chunk, 1)
				time.Sleep(1 * time.Millisecond)
				continue
			}
			break
		}
	}
	log.Infof("Enqueuing of %s tables completed", q.descr)
	enqueue_shutdown(q)
}

// chunk_builder_thread runs table_job_enqueue for non_transactional then transactional queues (consumes request_chunk and enqueues dump jobs).
func chunk_builder_thread(c any) {
	conf := c.(*Configuration)
	table_job_enqueue(conf.non_transactional)
	table_job_enqueue(conf.transactional)
	return
}
