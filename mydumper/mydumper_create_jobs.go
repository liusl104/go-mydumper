package mydumper

import (
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	"os"
	"sync/atomic"
)

func create_job_to_dump_table(conf *Configuration, is_view bool, is_sequence bool, database *database, table string, collation string, engine string) {
	var j *job = new(job)
	var dtj *dump_table_job = new(dump_table_job)
	dtj.is_view = is_view
	dtj.is_sequence = is_sequence
	dtj.database = database
	dtj.table = table
	dtj.collation = collation
	dtj.engine = engine
	j.job_data = dtj
	j.types = JOB_TABLE
	G_async_queue_push(conf.initial_queue, j)
}

func create_job_to_write_source_and_replica_status(conf *Configuration, mdfile *os.File) {
	var j *job = new(job)
	j.job_data = mdfile
	j.types = JOB_WRITE_SOURCE_AND_REPLICA_STATUS
	G_async_queue_push(conf.initial_queue, j)
}
func create_job_to_dump_all_databases(conf *Configuration) {
	atomic.AddInt64(&database_counter, 1)
	var j = new(job)
	j.types = JOB_DUMP_ALL_DATABASES
	j.job_data = nil
	G_async_queue_push(conf.initial_queue, j)
	return
}

func create_job_to_dump_table_list(table_list []string, conf *Configuration) {
	atomic.AddInt64(&database_counter, 1)
	var j *job = new(job)
	var dtlj *dump_table_list_job = new(dump_table_list_job)
	j.job_data = dtlj
	dtlj.table_list = table_list
	j.types = JOB_DUMP_TABLE_LIST
	G_async_queue_push(conf.initial_queue, j)
	return
}

func create_job_to_dump_database(database *database, conf *Configuration) {
	atomic.AddInt64(&database_counter, 1)
	var j = new(job)
	var ddj = new(dump_database_job)
	j.job_data = ddj
	ddj.database = database
	j.types = JOB_DUMP_DATABASE
	G_async_queue_push(conf.initial_queue, j)
	return
}

// Enqueueing in schema_queue

func create_job_to_dump_tablespaces(conf *Configuration) {
	var j *job = new(job)
	var ctj *create_tablespace_job = new(create_tablespace_job)
	j.job_data = ctj
	j.types = JOB_CREATE_TABLESPACE
	ctj.filename = build_tablespace_filename()
	G_async_queue_push(conf.schema_queue, j)
}

func create_database_related_job(database *database, conf *Configuration, types job_type, suffix string, checksum_filename bool) {
	var j *job = new(job)
	var dj *database_job = new(database_job)
	j.job_data = dj
	dj.database = database
	j.types = types
	dj.filename = build_schema_filename(database.filename, suffix)
	dj.checksum_filename = checksum_filename
	G_async_queue_push(conf.schema_queue, j)
	return
}

func create_job_to_dump_table_schema(dbt *db_table, conf *Configuration) {
	var j *job = new(job)
	var sj *schema_job = new(schema_job)
	j.job_data = sj
	sj.dbt = dbt
	j.types = JOB_SCHEMA
	sj.filename = build_schema_table_filename(dbt.database.filename, dbt.table_filename, "schema")
	sj.checksum_filename = SchemaChecksums
	sj.checksum_index_filename = SchemaChecksums
	G_async_queue_push(conf.schema_queue, j)
}

func create_job_to_dump_schema(database *database, conf *Configuration) {
	create_database_related_job(database, conf, JOB_CREATE_DATABASE, "schema-create", SchemaChecksums)
}

func create_job_to_dump_post(database *database, conf *Configuration) {
	create_database_related_job(database, conf, JOB_SCHEMA_POST, "schema-post", SchemaChecksums)
}

// Enqueueing in post_data_queue

func create_job_to_dump_triggers(conn *DBConnection, dbt *db_table, conf *Configuration) {
	var query string
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s LIKE '%s'", Identifier_quote_character, dbt.database.name, Identifier_quote_character, dbt.escaped_table)
	var result *MYSQL_RES = M_store_result(conn, query, M_critical, "Error Checking triggers for %s.%s. St: %s", dbt.database.name, dbt.table, query)
	if result != nil {
		if result.RecNumber != 0 {
			var t *job = new(job)
			var st *schema_job = new(schema_job)
			t.job_data = st
			t.types = JOB_TRIGGERS
			st.dbt = dbt
			st.filename = build_schema_table_filename(dbt.database.filename, dbt.table_filename, "schema-triggers")
			st.checksum_filename = RoutineChecksums
			G_async_queue_push(conf.post_data_queue, t)
		}
		Mysql_free_result(result)
	}
}

func create_job_to_dump_schema_triggers(database *database, conf *Configuration) {
	var t = new(job)
	var st = new(database_job)
	t.job_data = st
	t.types = JOB_SCHEMA_TRIGGERS
	st.database = database
	st.filename = build_schema_filename(database.filename, "schema-triggers")
	st.checksum_filename = RoutineChecksums
	G_async_queue_push(conf.post_data_queue, t)
}

func create_job_to_dump_view(dbt *db_table, conf *Configuration) {
	var j = new(job)
	var vj = new(view_job)
	j.job_data = vj
	vj.dbt = dbt
	j.types = JOB_VIEW
	vj.tmp_table_filename = build_schema_table_filename(dbt.database.filename, dbt.table_filename, "schema")
	vj.view_filename = build_schema_table_filename(dbt.database.filename, dbt.table_filename, "schema-view")
	vj.checksum_filename = SchemaChecksums
	G_async_queue_push(conf.post_data_queue, j)
	return
}

func create_job_to_dump_sequence(dbt *db_table, conf *Configuration) {
	var j = new(job)
	var sj = new(sequence_job)
	j.job_data = sj
	sj.dbt = dbt
	j.types = JOB_SEQUENCE
	sj.filename = build_schema_table_filename(dbt.database.filename, dbt.table_filename, "schema-sequence")
	sj.checksum_filename = SchemaChecksums
	G_async_queue_push(conf.post_data_queue, j)
	return
}

func create_job_to_dump_checksum(dbt *db_table, conf *Configuration) {
	var j = new(job)
	var tcj = new(table_checksum_job)
	tcj.dbt = dbt
	j.job_data = tcj
	j.types = JOB_CHECKSUM
	tcj.filename = build_meta_filename(dump_directory, dbt.database.filename, dbt.table_filename, "checksum")
	G_async_queue_push(conf.post_data_queue, j)
	return
}

// Enqueueing in data tables queue
func new_table_job(dbt *db_table, partition string, part uint64, chunk_step_item *chunk_step_item) *table_job {
	var tj = new(table_job)
	tj.partition = partition
	tj.chunk_step_item = chunk_step_item
	tj.where = nil
	tj.part = part
	tj.sub_part = 0
	tj.rows = new(table_job_file)
	tj.rows.file = nil
	tj.rows.filename = ""
	if output_format == SQL_INSERT {
		tj.sql = nil
	} else {
		tj.sql = new(table_job_file)
		tj.sql.file = nil
		tj.sql.filename = ""
	}
	tj.exec_out_filename = ""
	tj.dbt = dbt
	tj.st_in_file = 0
	tj.filesize = 0
	tj.child_process = 0
	tj.where = nil
	tj.num_rows_of_last_run = 0
	update_estimated_remaining_chunks_on_dbt(tj.dbt)
	return tj
}

func free_table_job(tj *table_job) {
	if tj.sql != nil && tj.sql.file != nil {
		if tj.sql.file != nil {
			m_close(tj.td.thread_id, tj.sql.file, tj.sql.filename, tj.filesize, tj.dbt)
		}
		tj.sql.file = nil
		tj.sql = nil
	}
	if tj.rows != nil {
		m_close(tj.td.thread_id, tj.rows.file, tj.rows.filename, tj.filesize, tj.dbt)
		tj.rows.file = nil
		tj.rows = nil
	}
	if tj.where != nil {
		tj.where = nil
	}
	tj = nil
}

func create_job_to_dump_chunk(dbt *db_table, partition string, part uint64, csi *chunk_step_item, f func(q *GAsyncQueue, task any), queue *GAsyncQueue) {
	var j = new(job)
	var tj = new_table_job(dbt, partition, part, csi)
	j.job_data = tj
	if dbt.is_transactional {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	f(queue, j)
}

func create_job_defer(dbt *db_table, queue *GAsyncQueue) {
	var j *job = new(job)
	j.types = JOB_DEFER
	j.job_data = dbt
	G_async_queue_push(queue, j)
}

func create_job_to_determine_chunk_type(dbt *db_table, f func(q *GAsyncQueue, task any), queue *GAsyncQueue) {
	var j = new(job)
	j.job_data = dbt
	j.types = JOB_DETERMINE_CHUNK_TYPE
	f(queue, j)
	return
}
