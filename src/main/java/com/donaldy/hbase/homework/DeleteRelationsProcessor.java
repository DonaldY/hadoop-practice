package com.donaldy.hbase.homework;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import java.io.IOException;
import java.util.List;

/**
 * uid1 解除 uid2关系， uid2 同时 解除 uid1 关系
 *
 * uid1 ： currUser
 * uid2 ： otherUser
 *
 * @author donald
 * @date 2020/09/01
 */
public class DeleteRelationsProcessor extends BaseRegionObserver {

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                           Durability durability) throws IOException {

        final HTableInterface relations = e.getEnvironment().getTable(TableName.valueOf("relations"));

        List<Cell> cells = delete.getFamilyCellMap().get(Bytes.toBytes("friends"));

        if (CollectionUtils.isEmpty(cells)) {

            relations.close();

            return;
        }

        // 获取 uid1 第一个 column
        Cell cell = cells.get(0);

        // 创建 uid2， 并设置需要删除的 column
        Delete otherUserDelete = new Delete(CellUtil.cloneQualifier(cell));
        otherUserDelete.addColumns(Bytes.toBytes("friends"), CellUtil.cloneRow(cell));

        relations.delete(otherUserDelete);

        // 关闭 table 对象
        relations.close();
    }
}
