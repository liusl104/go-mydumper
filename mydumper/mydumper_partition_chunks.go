package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/liusl104/go-mydumper/src"
	"math"
)

var (
	SplitPartitions bool
	PartitionRegex  string
)

func process_partition_chunk(tj *table_job, csi *chunk_step_item) {
	var cs = csi.chunk_step
	var partition string
	for _, data := range cs.partition_step.list {
		if shutdown_triggered {
			return
		}
		csi.mutex.Lock()
		partition = fmt.Sprintf(" PARTITION (%s) ", data)
		csi.mutex.Unlock()
		tj.partition = partition
		write_table_job_into_file(tj)
	}
}

func new_real_partition_step(partition []string) *chunk_step {
	var cs = new(chunk_step)
	cs.partition_step = new(partition_step)
	cs.partition_step.list = partition
	return cs
}

func new_real_partition_step_item(partition []string, deep uint, part uint64) *chunk_step_item {
	var csi = new(chunk_step_item)
	csi.chunk_type = PARTITION
	csi.chunk_step = new_real_partition_step(partition)
	csi.chunk_functions.process = process_partition_chunk
	csi.chunk_functions.get_next = get_next_partition_chunk
	csi.chunk_functions.free = nil
	csi.status = UNASSIGNED
	csi.mutex = G_mutex_new()
	csi.deep = deep
	csi.part = part
	return csi
}

func get_next_partition_chunk(dbt *db_table) *chunk_step_item {
	var l = dbt.chunks.Front()
	var csi *chunk_step_item
	for l != nil {
		csi = l.Value.(*chunk_step_item)
		csi.mutex.Lock()
		if csi.status == UNASSIGNED {
			csi.status = ASSIGNED
			csi.mutex.Unlock()
			return csi
		}
		if len(csi.chunk_step.partition_step.list) > 3 {
			var pos uint = uint(len(csi.chunk_step.partition_step.list)) / 2
			var new_list = csi.chunk_step.partition_step.list[pos:]
			var new_csi = new_real_partition_step_item(new_list, csi.deep+1, csi.part+uint64(math.Pow(2, float64(csi.deep))))
			csi.deep++
			new_csi.status = ASSIGNED
			dbt.chunks.PushBack(new_csi)
			csi.mutex.Unlock()
			return new_csi
		}
		csi.mutex.Unlock()
		l = l.Next()
	}
	return nil
}

func get_partitions_for_table(conn *DBConnection, dbt *db_table) []string {
	var partition_list []string
	var query = fmt.Sprintf("select PARTITION_NAME from information_schema.PARTITIONS where PARTITION_NAME is not null and TABLE_SCHEMA='%s' and TABLE_NAME='%s'", dbt.database.name, dbt.table)
	var res *MYSQL_RES = M_store_result(conn, query, nil, "Partitioning is not supported")
	if res == nil {
		return nil
	}
	var row []mysql.FieldValue
	for {
		row = Mysql_fetch_row(res)
		if row == nil {
			break
		}
		if (dbt.partition_regex == nil && Eval_partition_regex(string(row[0].AsString()))) || (dbt.partition_regex != nil && Eval_partition_regex(string(row[0].AsString()))) {
			partition_list = append(partition_list, string(row[0].AsString()))
		}
	}
	Mysql_free_result(res)
	return partition_list
}
