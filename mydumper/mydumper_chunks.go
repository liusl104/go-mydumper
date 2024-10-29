package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"slices"
	"strings"
	"time"
)

func initialize_chunk(o *OptionEntries) {
	o.global.give_me_another_innodb_chunk_step_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.give_me_another_non_innodb_chunk_step_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	initialize_char_chunk(o)
}

func finalize_chunk(o *OptionEntries) {
	o.global.give_me_another_innodb_chunk_step_queue.unref()
	o.global.give_me_another_non_innodb_chunk_step_queue.unref()
}

func process_none_chunk(o *OptionEntries, tj *table_job, csi *chunk_step_item) {
	_ = csi
	write_table_job_into_file(o, tj)
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

func initialize_chunk_step_item(o *OptionEntries, conn *client.Conn, dbt *db_table, position uint, prefix string, rows uint64) *chunk_step_item {
	var csi *chunk_step_item
	var err error
	var query, cache string
	var row []mysql.FieldValue
	var minmax *mysql.Result
	var field = dbt.primary_key[position]
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var where_option, where_option_prefix string
	if o.Filter.WhereOption != "" || prefix != "" {
		where_option = "WHERE"
		where_option_prefix = "AND"
	}
	query = fmt.Sprintf("SELECT %s MIN(%s%s%s),MAX(%s%s%s),LEFT(MIN(%s%s%s),1),LEFT(MAX(%s%s%s),1) FROM %s%s%s.%s%s%s %s %s %s %s",
		cache,
		o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, field,
		o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, field, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, field,
		o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str, dbt.table,
		o.global.identifier_quote_character_str, where_option, o.Filter.WhereOption, where_option_prefix, prefix)
	minmax, err = conn.Execute(query)
	if err != nil {
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
	var lengths = minmax.Fields
	for _, row = range minmax.Values {
		switch fields[0].Type {
		case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_INT24:
			log.Debugf("Integer PK found on `%s`.`%s`", dbt.database.name, dbt.table)
			unmin = row[0].AsUint64()
			unmax = row[1].AsUint64()
			nmin = row[0].AsInt64()
			nmax = row[1].AsInt64()
			if (fields[0].Flag & mysql.UNSIGNED_FLAG) != 0 {
				diff_btwn_max_min = uint64(gint64_abs(int64(unmax - unmin)))
			} else {
				diff_btwn_max_min = uint64(gint64_abs(nmax - nmin))
			}
			var unsign bool = (fields[0].Flag & mysql.UNSIGNED_FLAG) != 0
			if diff_btwn_max_min > dbt.min_chunk_step_size {
				var types *int_types
				var min_css uint64 = dbt.min_chunk_step_size
				var max_css uint64 = dbt.max_chunk_step_size
				var starting_css uint64 = dbt.starting_chunk_step_size
				var is_step_fixed_length bool = (min_css != 0 && min_css == starting_css && max_css == starting_css)

				if unsign {
					types.unsign.min = unmin
					types.unsign.max = unmax
				} else {
					types.sign.min = nmin
					types.sign.max = nmax
				}

				csi = new_integer_step_item(o, true, prefix, field, unsign, types, 0, is_step_fixed_length, starting_css, min_css, max_css, 0, false, false, nil, position)

				if dbt.multicolumn && csi.position == 0 {
					if (csi.chunk_step.integer_step.is_unsigned && (rows/(csi.chunk_step.integer_step.types.unsign.max-csi.chunk_step.integer_step.types.unsign.min) > dbt.min_chunk_step_size)) || (!csi.chunk_step.integer_step.is_unsigned && (int64(rows)/gint64_abs(csi.chunk_step.integer_step.types.sign.max-csi.chunk_step.integer_step.types.sign.min) > int64(dbt.min_chunk_step_size))) {
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
			csi = new_char_step_item(o, conn, true, prefix, dbt.primary_key[0], 0, 0, row, lengths, nil)
			return csi
		default:
			log.Infof("It is NONE: default")
			return new_none_chunk_step()
		}
	}
	return nil
}

func get_rows_from_explain(o *OptionEntries, conn *client.Conn, dbt *db_table, where string, field string) uint64 {
	var query string
	var row []mysql.FieldValue
	var res *mysql.Result
	var err error
	var cache string
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	var q, field_column, where_column string
	if field != "" {
		q = o.Common.IdentifierQuoteCharacter
		field_column = field
	} else {
		field_column = "*"
	}
	if where != "" {
		where_column = " WHERE "
	}
	query = fmt.Sprintf("EXPLAIN SELECT %s %s%s%s FROM %s%s%s.%s%s%s%s%s", cache, q, field_column, q,
		o.Common.IdentifierQuoteCharacter, dbt.database.name, o.Common.IdentifierQuoteCharacter,
		o.Common.IdentifierQuoteCharacter, dbt.table, o.Common.IdentifierQuoteCharacter, where_column, where)
	res, err = conn.Execute(query)
	if err != nil {
		log.Fatalf("Error executing EXPLAIN query `%s`: %s", query, err)
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

func get_rows_from_count(o *OptionEntries, conn *client.Conn, dbt *db_table) uint64 {
	var cache, query string
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	query = fmt.Sprintf("SELECT %s COUNT(*) FROM %s%s%s.%s%s%s", cache,
		o.Common.IdentifierQuoteCharacter, dbt.database.name, o.Common.IdentifierQuoteCharacter,
		o.Common.IdentifierQuoteCharacter, dbt.table, o.Common.IdentifierQuoteCharacter)
	res, err := conn.Execute(query)
	if err != nil {
		log.Errorf("Error executing EXPLAIN query `%s`: %s", query, err)
		return 0
	}
	var rows uint64
	for _, row := range res.Values {
		rows = row[0].AsUint64()
	}
	return rows
}

func set_chunk_strategy_for_dbt(o *OptionEntries, conn *client.Conn, dbt *db_table) {
	dbt.chunks_mutex.Lock()
	var csi *chunk_step_item
	var rows uint64
	if o.Extra.CheckRowCount {
		rows = get_rows_from_count(o, conn, dbt)
		log.Infof("%s.%s has %s%d rows", dbt.database.name, dbt.table, "", rows)
	} else {
		rows = get_rows_from_explain(o, conn, dbt, "", "")
		log.Infof("%s.%s has %s%d rows", dbt.database.name, dbt.table, "~", rows)
	}
	dbt.rows_total = rows
	if rows > dbt.min_chunk_step_size {
		var partitions []string
		if o.Chunks.SplitPartitions || dbt.partition_regex != nil {
			partitions = get_partitions_for_table(o, conn, dbt)
		}
		if len(partitions) > 0 {
			csi = new_real_partition_step_item(partitions, 0, 0)
		} else {
			if dbt.split_integer_tables {
				csi = initialize_chunk_step_item(o, conn, dbt, 0, "", rows)
			} else {
				csi = new_none_chunk_step()
			}
		}
	} else {
		csi = new_none_chunk_step()
	}
	dbt.chunks = append(dbt.chunks, csi)
	dbt.chunks_queue.push(csi)
	dbt.status = READY
	dbt.chunks_mutex.Unlock()
}

func get_primary_key(o *OptionEntries, conn *client.Conn, dbt *db_table, conf *configuration) {
	var indexes *mysql.Result
	var row []mysql.FieldValue
	var err error
	dbt.primary_key = make([]string, 0)
	var query string = fmt.Sprintf("SHOW INDEX FROM %s%s%s.%s%s%s", o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, dbt.table, o.global.identifier_quote_character_str)
	indexes, err = conn.Execute(query)
	if err != nil {
		log.Errorf("Error executing SHOW INDEX query `%s`: %s", query, err)
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
		if strings.Compare(string(row[1].AsString()), "0") == 0 {
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
			if strings.Compare(string(row[3].AsString()), "1") == 0 {
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

func get_next_dbt_and_chunk_step_item(dbt_pointer *db_table, csi *chunk_step_item, dbt_list *MList) bool {
	dbt_list.mutex.Lock()
	var dbt *db_table
	var are_there_jobs_defining bool
	var lcs *chunk_step_item
	for _, dbt = range dbt_list.list {
		dbt.chunks_mutex.Lock()
		if dbt.status != DEFINING {
			if dbt.status == UNDEFINED {
				dbt_pointer = dbt
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
			lcs = dbt.chunks[0].(*chunk_step_item)
			if lcs.chunk_type == NONE {
				dbt_pointer = dbt
				csi = lcs
				index := slices.Index(dbt_list.list, dbt)
				dbt_list.list = append(dbt_list.list[:index], dbt_list.list[index+1:]...)
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

func enqueue_shutdown_jobs(o *OptionEntries, queue *asyncQueue) {
	var n uint
	for n = 0; n < o.Common.NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		queue.push(j)
	}
}

func enqueue_shutdown(o *OptionEntries, q *table_queuing) {
	enqueue_shutdown_jobs(o, q.queue)
	enqueue_shutdown_jobs(o, q.deferQueue)

}
func table_job_enqueue(o *OptionEntries, q *table_queuing) {
	var dbt *db_table
	var csi *chunk_step_item
	var are_there_jobs_defining bool
	log.Infof("Starting %s tables", q.descr)
	for {
		q.request_chunk.pop()
		if o.global.shutdown_triggered {
			break
		}
		dbt = nil
		csi = nil
		are_there_jobs_defining = false
		are_there_jobs_defining = get_next_dbt_and_chunk_step_item(dbt, csi, q.table_list)
		if dbt != nil {
			if dbt.status == DEFINING {
				create_job_to_determine_chunk_type(dbt, g_async_queue_push, q.queue)
				continue
			}
			if csi != nil {
				switch csi.chunk_type {
				case INTEGER:
					if o.Extra.UseDefer {
						create_job_to_dump_chunk(o, dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, g_async_queue_push, q.queue)
						create_job_defer(dbt, q.queue)
					} else {
						create_job_to_dump_chunk(o, dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, g_async_queue_push, q.queue)
					}
				case CHAR:
					create_job_to_dump_chunk(o, dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, g_async_queue_push, q.queue)
				case PARTITION:
					create_job_to_dump_chunk(o, dbt, "", csi.number, dbt.primary_key_separated_by_comma, csi, g_async_queue_push, q.queue)
				case NONE:
					create_job_to_dump_chunk(o, dbt, "", 0, dbt.primary_key_separated_by_comma, csi, g_async_queue_push, q.queue)
				default:
					log.Errorf("This should not happen %v", csi.chunk_type)
				}
			} else {
				if are_there_jobs_defining {
					g_async_queue_push(q.request_chunk, 1)
					time.Sleep(1 * time.Millisecond)
					continue
				}
				break
			}
		}
	}
	log.Infof("%s tables completed", q.descr)
	enqueue_shutdown(o, q)
}

func chunk_builder_thread(o *OptionEntries, conf *configuration) {
	table_job_enqueue(o, conf.non_innodb)
	table_job_enqueue(o, conf.innodb)
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
