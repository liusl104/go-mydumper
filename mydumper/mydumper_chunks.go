package mydumper

import (
	"container/list"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"strings"
	"time"
)

var (
	give_me_another_innodb_chunk_step_queue     *GAsyncQueue
	give_me_another_non_innodb_chunk_step_queue *GAsyncQueue
)

func initialize_chunk() {
	give_me_another_innodb_chunk_step_queue = G_async_queue_new(BufferSize)
	give_me_another_non_innodb_chunk_step_queue = G_async_queue_new(BufferSize)
	initialize_char_chunk()
}

func finalize_chunk() {
	G_async_queue_unref(give_me_another_innodb_chunk_step_queue)
	G_async_queue_unref(give_me_another_non_innodb_chunk_step_queue)
}

func process_none_chunk(tj *table_job, csi *chunk_step_item) {
	_ = csi
	write_table_job_into_file(tj)
}

func initialize_chunk_step_as_none(csi *chunk_step_item) {
	csi.chunk_type = NONE
	csi.chunk_functions.process = process_none_chunk
	csi.chunk_step = nil
}

func new_none_chunk_step() *chunk_step_item {
	var csi *chunk_step_item = new(chunk_step_item)
	csi.chunk_functions = new(chunk_functions)
	initialize_chunk_step_as_none(csi)
	return csi
}

func initialize_chunk_step_item(conn *DBConnection, dbt *DB_Table, position uint, prefix string, rows uint64) *chunk_step_item {
	var csi *chunk_step_item
	var query, cache string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var field = dbt.primary_key[position]
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where_option, where_option_prefix string
	if WhereOption != "" || prefix != "" {
		where_option = "WHERE"
		where_option_prefix = "AND"
	}
	query = fmt.Sprintf("SELECT %s MIN(%s%s%s),MAX(%s%s%s),LEFT(MIN(%s%s%s),1),LEFT(MAX(%s%s%s),1) FROM %s%s%s.%s%s%s %s %s %s %s",
		cache,
		Identifier_quote_character_str, field, Identifier_quote_character_str, Identifier_quote_character_str, field,
		Identifier_quote_character_str,
		Identifier_quote_character_str, field, Identifier_quote_character_str, Identifier_quote_character_str, field,
		Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str, Identifier_quote_character_str, dbt.table,
		Identifier_quote_character_str, where_option, WhereOption, where_option_prefix, prefix)
	minmax = conn.Execute(query)
	if conn.Err != nil {
		log.Infof("It is NONE with minmax == NULL")
		return new_none_chunk_step()
	}
	if len(minmax.Values) == 0 {
		log.Infof("It is NONE with minmax == NULL")
		return new_none_chunk_step()
	}
	var fields []*mysql.Field = minmax.Fields
	var diff_btwn_max_min, unmin, unmax uint64
	var nmin, nmax int64
	// var lengths = minmax.Fields
	for _, row = range minmax.Values {
		if row[0].Value() == nil {
			log.Infof("It is NONE with minmax == NULL")
			return new_none_chunk_step()
		}
		switch fields[0].Type {
		case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_INT24:
			log.Debugf("Integer PK found on `%s`.`%s`", dbt.database.name, dbt.table)
			unmin = row[0].AsUint64()
			unmax = row[1].AsUint64()
			nmin = row[0].AsInt64()
			nmax = row[1].AsInt64()
			if (fields[0].Flag & mysql.UNSIGNED_FLAG) != 0 {
				diff_btwn_max_min = gint64_abs(int64(unmax - unmin))
			} else {
				diff_btwn_max_min = gint64_abs(nmax - nmin)
			}
			var unsign bool = (fields[0].Flag & mysql.UNSIGNED_FLAG) != 0
			if diff_btwn_max_min > dbt.min_chunk_step_size {
				var types *int_types = new(int_types)
				types.sign = new(signed_int)
				types.unsign = new(unsigned_int)
				var min_css uint64 = dbt.min_chunk_step_size
				var max_css uint64 = dbt.max_chunk_step_size
				var starting_css uint64 = dbt.starting_chunk_step_size
				var is_step_fixed_length bool = min_css != 0 && min_css == starting_css && max_css == starting_css

				if unsign {
					types.unsign.min = unmin
					types.unsign.max = unmax
				} else {
					types.sign.min = nmin
					types.sign.max = nmax
				}

				csi = new_integer_step_item(true, prefix, field, unsign, types, 0, is_step_fixed_length, starting_css, min_css, max_css, 0, false, false, nil, position)

				if dbt.multicolumn && csi.position == 0 {
					if (csi.chunk_step.integer_step.is_unsigned && (rows/(csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.min) > dbt.min_chunk_step_size)) ||
						(!csi.chunk_step.integer_step.is_unsigned && (rows/gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.min) > dbt.min_chunk_step_size)) {
						csi.chunk_step.integer_step.min_chunk_step_size = 1
						csi.chunk_step.integer_step.is_step_fixed_length = true
						csi.chunk_step.integer_step.max_chunk_step_size = 1
						csi.chunk_step.integer_step.step = 1
					} else {
						dbt.multicolumn = false
					}
				}

				if csi.chunk_step.integer_step.is_step_fixed_length {
					if csi.chunk_step.integer_step.is_unsigned {
						csi.chunk_step.integer_step.types.unsign.min = (csi.chunk_step.integer_step.types.unsign.min / csi.chunk_step.integer_step.step) * csi.chunk_step.integer_step.step
					} else {
						csi.chunk_step.integer_step.types.sign.min = (csi.chunk_step.integer_step.types.sign.min / int64(csi.chunk_step.integer_step.step)) * int64(csi.chunk_step.integer_step.step)
					}
				}

				if dbt.min_chunk_step_size == dbt.starting_chunk_step_size && dbt.max_chunk_step_size == dbt.starting_chunk_step_size && dbt.min_chunk_step_size != 0 {
					dbt.chunk_filesize = 0
				}
				return csi

			} else {
				log.Debugf("Integer PK on `%s`.`%s` performing full table scan", dbt.database.name, dbt.table)
				return new_none_chunk_step()
			}
		case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VAR_STRING:
			/*
				if (minmax)
				          mysql_free_result(minmax);
				        return new_none_chunk_step();
			*/
			return new_none_chunk_step()
			/*csi = new_char_step_item(o, conn, true, prefix, dbt.primary_key[0], 0, 0, row, lengths, nil)
			  return csi */
		default:
			log.Infof("It is NONE: default")
			return new_none_chunk_step()
		}
	}
	return nil
}

func get_rows_from_explain(conn *DBConnection, dbt *DB_Table, where string, field string) uint64 {
	var query string
	var row []mysql.FieldValue
	var res *mysql.Result
	var cache string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var q, field_column, where_column string
	if field != "" {
		q = Identifier_quote_character_str
		field_column = field
	} else {
		field_column = "*"
	}
	if where != "" {
		where_column = " WHERE "
	}
	query = fmt.Sprintf("EXPLAIN SELECT %s %s%s%s FROM %s%s%s.%s%s%s%s%s", cache, q, field_column, q,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str, where_column, where)
	res = conn.Execute(query)
	if conn.Err != nil {
		log.Fatalf("Error executing EXPLAIN query `%s`: %s", query, conn.Err)
	}
	if len(res.Values) == 0 {
		return 0
	}
	var row_col int = 0
	determine_explain_columns(res, &row_col)
	for _, row = range res.Values {
		if row[row_col].Value() == nil {
			return 0
		}
	}
	var rows_in_explain uint64 = row[row_col].AsUint64()
	return rows_in_explain
}

func get_rows_from_count(conn *DBConnection, dbt *DB_Table) uint64 {
	var cache, query string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	query = fmt.Sprintf("SELECT %s COUNT(*) FROM %s%s%s.%s%s%s", cache,
		Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
	res := conn.Execute(query)
	if conn.Err != nil {
		log.Errorf("Error executing EXPLAIN query `%s`: %v", query, conn.Err)
		return 0
	}
	var rows uint64
	for _, row := range res.Values {
		rows = row[0].AsUint64()
	}
	return rows
}

func set_chunk_strategy_for_dbt(conn *DBConnection, dbt *DB_Table) {
	dbt.chunks_mutex.Lock()
	var csi *chunk_step_item
	var rows uint64
	if CheckRowCount {
		rows = get_rows_from_count(conn, dbt)
		log.Infof("%s.%s has %s%d rows", dbt.database.name, dbt.table, "", rows)
	} else {
		rows = get_rows_from_explain(conn, dbt, "", "")
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
				csi = initialize_chunk_step_item(conn, dbt, 0, "", rows)
			} else {
				csi = new_none_chunk_step()
			}
		}
	} else {
		csi = new_none_chunk_step()
	}
	dbt.chunks.PushFront(csi)
	G_async_queue_push(dbt.chunks_queue, csi)
	dbt.status = READY
	dbt.chunks_mutex.Unlock()
}

func get_primary_key(conn *DBConnection, dbt *DB_Table, conf *configuration) {
	var indexes *mysql.Result
	var row []mysql.FieldValue
	var query string = fmt.Sprintf("SHOW INDEX FROM %s%s%s.%s%s%s", Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
	indexes = conn.Execute(query)
	if conn.Err != nil {
		log.Errorf("Error executing SHOW INDEX query `%s`: %s", query, conn.Err)
	}
	for _, row = range indexes.Values {
		if strings.Compare(string(row[2].AsString()), "PRIMARY") == 0 {
			dbt.primary_key = append(dbt.primary_key, string(row[4].AsString()))
		}
	}
	if len(dbt.primary_key) != 0 {
		return
	}
	for _, row = range indexes.Values {
		if row[1].AsUint64() == 0 {
			dbt.primary_key = append(dbt.primary_key, string(row[4].AsString()))
		}
	}
	if len(dbt.primary_key) != 0 {
		return
	}
	if len(dbt.primary_key) == 0 && conf.use_any_index != "" {
		var max_cardinality uint64
		var cardinality uint64
		var field string
		for _, row = range indexes.Values {
			if row[3].AsUint64() == 1 {
				if row[6].Value() != nil {
					cardinality = row[6].AsUint64()
				}
				if cardinality > max_cardinality {
					field = string(row[4].AsString())
					max_cardinality = cardinality
				}
			}
		}
		if field != "" {
			dbt.primary_key = append(dbt.primary_key, field)
		}
	}
}

func get_next_dbt_and_chunk_step_item(dbt_pointer **DB_Table, csi **chunk_step_item, dbt_list *MList) bool {
	dbt_list.mutex.Lock()
	var dbt *DB_Table
	var iter *list.List = dbt_list.list
	var are_there_jobs_defining bool
	var lcs *chunk_step_item
	for v := iter.Front(); v != nil; v = v.Next() {
		dbt = v.Value.(*DB_Table)
		dbt.chunks_mutex.Lock()
		if dbt.status != DEFINING {
			if dbt.status == UNDEFINED {
				*dbt_pointer = v.Value.(*DB_Table)
				dbt.status = DEFINING
				are_there_jobs_defining = true
				dbt.chunks_mutex.Unlock()
				break
			}
			if dbt.status != READY {
				log.Errorf("dbt status not is ready")
			}
			if dbt.chunks == nil {
				dbt.chunks_mutex.Unlock()
				continue
			}
			lcs = dbt.chunks.Front().Value.(*chunk_step_item)
			if lcs.chunk_type == NONE {
				*dbt_pointer = v.Value.(*DB_Table)
				*csi = lcs
				dbt_list.list.Remove(v)
				dbt.chunks_mutex.Unlock()
				break
			}
			if dbt.max_threads_per_table <= dbt.current_threads_running {
				dbt.chunks_mutex.Unlock()
				continue
			}
			dbt.current_threads_running++
			lcs = lcs.chunk_functions.get_next(dbt)
			if lcs != nil {
				*dbt_pointer = v.Value.(*DB_Table)
				*csi = lcs
				dbt.chunks_mutex.Unlock()
				break
			} else {
				dbt_list.list.Remove(v)
				iter.Front().Next()
				dbt.chunks_mutex.Unlock()
				continue
			}
		} else {
			dbt.chunks_mutex.Unlock()
			are_there_jobs_defining = true
		}
	}
	dbt_list.mutex.Unlock()
	return are_there_jobs_defining
}

func enqueue_shutdown_jobs(queue *GAsyncQueue) {
	var n uint
	for n = 0; n < NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(queue, j)
	}
}

func enqueue_shutdown(q *table_queuing) {
	enqueue_shutdown_jobs(q.queue)
	enqueue_shutdown_jobs(q.deferQueue)

}
func table_job_enqueue(q *table_queuing) {
	var dbt *DB_Table
	var csi *chunk_step_item
	var are_there_jobs_defining bool
	log.Infof("Starting %s tables", q.descr)
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
				switch csi.chunk_type {
				case INTEGER:
					if UseDefer {
						create_job_to_dump_chunk(dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, G_async_queue_push, q.queue)
						create_job_defer(dbt, q.queue)
					} else {
						create_job_to_dump_chunk(dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, G_async_queue_push, q.queue)
					}
				case CHAR:
					create_job_to_dump_chunk(dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, G_async_queue_push, q.queue)
				case PARTITION:
					create_job_to_dump_chunk(dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, G_async_queue_push, q.queue)
				case NONE:
					create_job_to_dump_chunk(dbt, "", 0, dbt.primary_key_separated_by_comma, csi, G_async_queue_push, q.queue)
				default:
					log.Errorf("This should not happen %v", csi.chunk_type)
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
	log.Infof("%s tables completed", q.descr)
	enqueue_shutdown(q)
}

func chunk_builder_thread(conf *configuration) {
	defer chunk_builder.Thread.Done()
	table_job_enqueue(conf.non_innodb)
	table_job_enqueue(conf.innodb)
	return
}

func build_where_clause_on_table_job(tj *table_job) {
	var csi *chunk_step_item = tj.chunk_step_item
	tj.where = ""
	tj.where += csi.where
	csi = csi.next
	for csi != nil && csi.chunk_type != NONE {
		tj.where += " AND "
		tj.where += csi.where
		csi = csi.next
	}
}
