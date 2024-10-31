package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"math"
)

func process_partition_chunk(o *OptionEntries, tj *table_job, csi *chunk_step_item) {
	var cs = csi.chunk_step
	var partition string
	for _, data := range cs.partition_step.list {
		if o.global.shutdown_triggered {
			return
		}
		csi.mutex.Lock()
		partition = fmt.Sprintf(" PARTITION (%s) ", data)
		csi.mutex.Unlock()
		tj.partition = partition
		write_table_job_into_file(o, tj)
	}
}

func new_real_partition_step(partition []string) *chunk_step {
	var cs = new(chunk_step)
	cs.partition_step = new(partition_step)
	cs.partition_step.list = partition
	return cs
}

func new_real_partition_step_item(partition []string, deep uint, number uint) *chunk_step_item {
	var csi = new(chunk_step_item)
	csi.chunk_type = PARTITION
	csi.chunk_step = new_real_partition_step(partition)
	csi.chunk_functions.process = process_partition_chunk
	csi.chunk_functions.get_next = get_next_partition_chunk
	csi.status = UNASSIGNED
	csi.mutex = g_mutex_new()
	csi.deep = deep
	csi.number = uint64(number)
	return csi
}

func get_next_partition_chunk(o *OptionEntries, dbt *db_table) *chunk_step_item {
	var l = dbt.chunks
	var csi *chunk_step_item
	_ = o
	for _, data := range l {
		csi = data.(*chunk_step_item)
		csi.mutex.Lock()
		if csi.status == UNASSIGNED {
			csi.status = ASSIGNED
			csi.mutex.Unlock()
			return csi
		}
		if len(csi.chunk_step.partition_step.list) > 3 {
			var pos uint = uint(len(csi.chunk_step.partition_step.list)) / 2
			var new_list = csi.chunk_step.partition_step.list[pos:]
			var new_csi = new_real_partition_step_item(new_list, csi.deep+1, uint(csi.number)+uint(math.Pow(2, float64(csi.deep))))
			csi.deep++
			new_csi.status = ASSIGNED
			dbt.chunks = append(dbt.chunks, new_csi)
			csi.mutex.Unlock()
			return new_csi
		}
		csi.mutex.Unlock()
	}
	return nil
}

func get_partitions_for_table(o *OptionEntries, conn *client.Conn, dbt *db_table) []string {
	var partition_list []string
	var row []mysql.FieldValue
	var query = fmt.Sprintf("select PARTITION_NAME from information_schema.PARTITIONS where PARTITION_NAME is not null and TABLE_SCHEMA='%s' and TABLE_NAME='%s'", dbt.database.name, dbt.table)
	res, err := conn.Execute(query)
	if err != nil {
		log.Errorf("get partition name fail:%v", err)
		return partition_list
	}
	for _, row = range res.Values {
		if (dbt.partition_regex == nil && eval_partition_regex(o, string(row[0].AsString()))) || (dbt.partition_regex != nil && eval_partition_regex(o, string(row[0].AsString()))) {
			partition_list = append(partition_list, string(row[0].AsString()))
		}
	}
	return partition_list
}